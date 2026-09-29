//! SSH client to worker VMs (Phase 10), built on `russh` -- never a
//! subprocess, matching this project's `FROM scratch` deployment (no
//! `ssh` binary exists in the image to shell out to even if that were
//! otherwise desirable).
//!
//! **Host key verification is deliberately skipped.** A worker VM is
//! created by this same process moments earlier (`reconcile::job`'s
//! `VMPending`), with no prior channel to have received its real host
//! key through -- there is no TOFU-meaningful "first contact" here the
//! way there is for a long-lived server, and no third party to have
//! tampered with in between (the only path from the shim to a worker's
//! IP is UpCloud's own network fabric, already trusted for every other
//! call this project makes to the same account). This mirrors the same
//! "plaintext secrets at rest" reasoning docs/IMPLEMENTATION_PLAN.md
//! already gives for not encrypting the database: closing this specific
//! gap would add real complexity against a threat model (compromising
//! UpCloud's own internal network) this project doesn't otherwise defend
//! against.
//!
//! Every command this module ever runs is either a fixed, hardcoded
//! string (`cat /tmp/exit-code`, `cat /tmp/container-id.txt`) or built
//! from a value this project generated itself and validates the shape of
//! before use (a `podman` container ID -- see `reconcile::job`'s own
//! validation) -- never job-supplied text, so there's no equivalent of
//! `src/cloud_init.rs`'s base64-escaping concern here.

use axum::body::Bytes;
use russh::client::{self, Handle};
use russh::keys::{decode_secret_key, PrivateKeyWithHashAlg};
use russh::ChannelMsg;
use std::sync::Arc;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Everything `api::logs` (Phase 10) needs to SSH into a worker on
/// demand, handed to the router as an `Extension` -- separate from
/// `reconcile::job::JobContext`, which is reconcile-loop-only and not
/// otherwise reachable from the HTTP layer.
pub struct WorkerSshConfig {
    pub private_key: String,
    pub port: u16,
}

#[derive(Debug, Error)]
pub enum SshError {
    #[error("invalid private key: {0}")]
    InvalidKey(#[from] russh::keys::Error),
    #[error("SSH connection/protocol error: {0}")]
    Protocol(#[from] russh::Error),
    #[error("SSH authentication failed")]
    AuthenticationFailed,
}

/// The result of a one-shot (non-streaming) command: everything the
/// remote side wrote to stdout, and its exit status if the channel
/// reported one before closing (`None` is rare -- an abrupt disconnect --
/// but real, so callers must not assume it's always present).
#[derive(Debug)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub exit_status: Option<u32>,
}

struct Client;

impl client::Handler for Client {
    type Error = russh::Error;

    async fn check_server_key(
        &mut self,
        _server_public_key: &russh::keys::PublicKeyOrCertificate,
    ) -> Result<bool, Self::Error> {
        // See this module's own top-level docs for why accepting any key
        // is the deliberate choice here, not an oversight.
        Ok(true)
    }
}

/// The real, production SSH port every worker VM actually listens on --
/// every real call site (`reconcile::job`, `api::logs`) uses this;
/// `connect_and_open_channel` itself takes an explicit port only so tests
/// can point it at a local mock server bound to an OS-assigned one.
pub const SSH_PORT: u16 = 22;

async fn connect_and_open_channel(
    host: &str,
    port: u16,
    private_key_pem: &str,
    command: &str,
) -> Result<russh::Channel<client::Msg>, SshError> {
    let key_pair = decode_secret_key(private_key_pem, None)?;
    let config = Arc::new(client::Config::default());
    let mut session: Handle<Client> = client::connect(config, (host, port), Client).await?;

    let auth_result = session
        .authenticate_publickey(
            "root",
            PrivateKeyWithHashAlg::new(
                Arc::new(key_pair),
                session.best_supported_rsa_hash().await?.flatten(),
            ),
        )
        .await?;
    if !auth_result.success() {
        return Err(SshError::AuthenticationFailed);
    }

    let channel = session.channel_open_session().await?;
    channel.exec(true, command).await?;
    Ok(channel)
}

/// Runs `command` once, collecting every byte written to stdout (stderr
/// and other channel messages are discarded) until the channel closes,
/// then returns it along with the exit status. Used for short, quick
/// checks (`cat /tmp/exit-code`) and one-shot full-log fetches (`podman
/// logs {id}`) -- never for a `-f`/follow command, which would never
/// return; see `exec_stream` for that.
pub async fn exec_once(
    host: &str,
    port: u16,
    private_key_pem: &str,
    command: &str,
) -> Result<ExecOutput, SshError> {
    let mut channel = connect_and_open_channel(host, port, private_key_pem, command).await?;

    let mut stdout = Vec::new();
    let mut exit_status = None;
    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { ref data } => stdout.extend_from_slice(data),
            ChannelMsg::ExitStatus {
                exit_status: status,
            } => exit_status = Some(status),
            _ => {}
        }
    }
    Ok(ExecOutput {
        stdout,
        exit_status,
    })
}

/// Runs `command` (typically `podman logs -f {id}`), forwarding each
/// chunk of stdout to the returned stream as it arrives, rather than
/// buffering it all before returning -- for live log following. The
/// connection and authentication happen before this returns, so a
/// connection/auth failure comes back as a normal `Err` the caller can
/// turn into a proper HTTP error response; only data arriving *after*
/// that point is streamed. The stream simply ends (no error surfaced to
/// the consumer) when the remote command exits or the connection drops --
/// "handle SSH disconnects gracefully" means exactly this: the HTTP
/// response body just ends, rather than the whole request handler
/// panicking or hanging.
pub async fn exec_stream(
    host: &str,
    port: u16,
    private_key_pem: &str,
    command: &str,
) -> Result<ReceiverStream<Bytes>, SshError> {
    let mut channel = connect_and_open_channel(host, port, private_key_pem, command).await?;

    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(msg) = channel.wait().await {
            if let ChannelMsg::Data { data } = msg {
                if tx.send(Bytes::copy_from_slice(&data)).await.is_err() {
                    // Receiver (the HTTP response body) is gone -- the
                    // client disconnected. Stop reading, let `channel`
                    // drop and close the SSH side too.
                    break;
                }
            }
        }
    });

    Ok(ReceiverStream::new(rx))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use russh::keys::{Algorithm, PrivateKey};
    use russh::server::{self, Msg, Server as _, Session};
    use russh::{Channel, ChannelId};
    use std::collections::HashMap;
    use std::net::SocketAddr;
    use std::sync::Mutex as StdMutex;
    use tokio::net::TcpListener;

    /// A real local SSH server (russh's own server API, not a hand-mocked
    /// transport) -- same "real local server" pattern
    /// `providers::upcloud::tests::mock_server` uses for HTTP. Accepts any
    /// key, and replies to an exec request by looking up a canned
    /// `(stdout, exit_status)` response for the exact command string in
    /// `responses`; an unlisted command gets empty output and exit 1.
    pub(crate) async fn mock_ssh_server(
        responses: HashMap<&'static str, (&'static str, u32)>,
    ) -> (String, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let config = Arc::new(server::Config {
            keys: vec![PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()],
            ..Default::default()
        });
        let mut server = TestServer {
            responses: Arc::new(responses),
        };

        tokio::spawn(async move {
            server.run_on_socket(config, &listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr.ip().to_string(), addr.port())
    }

    /// Generates a throwaway ed25519 keypair PEM, for tests that need
    /// *some* syntactically valid private key to hand to `exec_once`/
    /// `exec_stream` -- the mock server's `auth_publickey` accepts any
    /// key, so its actual content never matters beyond being parseable.
    pub(crate) fn throwaway_private_key_pem() -> String {
        let key = PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap();
        key.to_openssh(russh::keys::ssh_key::LineEnding::LF)
            .unwrap()
            .to_string()
    }

    #[derive(Clone)]
    struct TestServer {
        responses: Arc<HashMap<&'static str, (&'static str, u32)>>,
    }

    impl server::Server for TestServer {
        type Handler = Self;
        fn new_client(&mut self, _: Option<SocketAddr>) -> Self {
            self.clone()
        }
    }

    impl server::Handler for TestServer {
        type Error = russh::Error;

        async fn auth_publickey(
            &mut self,
            _user: &str,
            _key: &russh::keys::ssh_key::PublicKey,
        ) -> Result<server::Auth, Self::Error> {
            Ok(server::Auth::Accept)
        }

        async fn channel_open_session(
            &mut self,
            _channel: Channel<Msg>,
            reply: server::ChannelOpenHandle,
            _session: &mut Session,
        ) -> Result<(), Self::Error> {
            reply.accept().await;
            Ok(())
        }

        async fn exec_request(
            &mut self,
            channel: ChannelId,
            data: &[u8],
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            let command = String::from_utf8_lossy(data).to_string();
            session.channel_success(channel)?;
            let (stdout, exit_status) = self
                .responses
                .get(command.as_str())
                .copied()
                .unwrap_or(("", 1));
            if !stdout.is_empty() {
                session.data(channel, Bytes::from(stdout.to_string()))?;
            }
            session.exit_status_request(channel, exit_status)?;
            session.eof(channel)?;
            session.close(channel)?;
            Ok(())
        }
    }

    // A slow/streaming variant used by the exec_stream test, kept
    // separate from TestServer above since it needs to send multiple
    // Data messages over time rather than one fixed canned reply.
    #[derive(Clone)]
    struct StreamingTestServer {
        chunks: Arc<StdMutex<Vec<&'static str>>>,
    }

    impl server::Server for StreamingTestServer {
        type Handler = Self;
        fn new_client(&mut self, _: Option<SocketAddr>) -> Self {
            self.clone()
        }
    }

    impl server::Handler for StreamingTestServer {
        type Error = russh::Error;

        async fn auth_publickey(
            &mut self,
            _user: &str,
            _key: &russh::keys::ssh_key::PublicKey,
        ) -> Result<server::Auth, Self::Error> {
            Ok(server::Auth::Accept)
        }

        async fn channel_open_session(
            &mut self,
            _channel: Channel<Msg>,
            reply: server::ChannelOpenHandle,
            _session: &mut Session,
        ) -> Result<(), Self::Error> {
            reply.accept().await;
            Ok(())
        }

        async fn exec_request(
            &mut self,
            channel: ChannelId,
            _data: &[u8],
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            session.channel_success(channel)?;
            let chunks = self.chunks.lock().unwrap().clone();
            let handle = session.handle();
            tokio::spawn(async move {
                for chunk in chunks {
                    let _ = handle.data(channel, Bytes::from(chunk)).await;
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                let _ = handle.eof(channel).await;
                let _ = handle.close(channel).await;
            });
            Ok(())
        }
    }

    async fn mock_streaming_ssh_server(chunks: Vec<&'static str>) -> (String, u16) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let config = Arc::new(server::Config {
            keys: vec![PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()],
            ..Default::default()
        });
        let mut server = StreamingTestServer {
            chunks: Arc::new(StdMutex::new(chunks)),
        };

        tokio::spawn(async move {
            server.run_on_socket(config, &listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr.ip().to_string(), addr.port())
    }

    #[tokio::test]
    async fn test_exec_once_returns_stdout_and_exit_status() {
        let mut responses = HashMap::new();
        responses.insert("echo hi", ("hi\n", 0));
        let (host, port) = mock_ssh_server(responses).await;
        let key = throwaway_private_key_pem();

        let output = exec_once(&host, port, &key, "echo hi").await.unwrap();

        assert_eq!(output.stdout, b"hi\n");
        assert_eq!(output.exit_status, Some(0));
    }

    #[tokio::test]
    async fn test_exec_once_nonzero_exit_status() {
        let mut responses = HashMap::new();
        responses.insert("false", ("", 1));
        let (host, port) = mock_ssh_server(responses).await;
        let key = throwaway_private_key_pem();

        let output = exec_once(&host, port, &key, "false").await.unwrap();

        assert_eq!(output.stdout, b"");
        assert_eq!(output.exit_status, Some(1));
    }

    #[tokio::test]
    async fn test_exec_once_invalid_key_is_an_error() {
        let (host, port) = mock_ssh_server(HashMap::new()).await;

        let err = exec_once(&host, port, "not a real key", "echo hi")
            .await
            .unwrap_err();

        assert!(matches!(err, SshError::InvalidKey(_)));
    }

    #[tokio::test]
    async fn test_exec_once_unreachable_host_is_an_error() {
        let key = throwaway_private_key_pem();
        // Port 1 rather than the real SSH_PORT: nothing listening there,
        // and unlike 22 it can't accidentally hit a real sshd on the
        // machine running this test.
        let err = exec_once("127.0.0.1", 1, &key, "echo hi")
            .await
            .unwrap_err();
        assert!(matches!(err, SshError::Protocol(_)));
    }

    #[tokio::test]
    async fn test_exec_stream_forwards_chunks_as_they_arrive() {
        let (host, port) = mock_streaming_ssh_server(vec!["chunk-1", "chunk-2", "chunk-3"]).await;
        let key = throwaway_private_key_pem();

        let mut stream = exec_stream(&host, port, &key, "podman logs -f abc")
            .await
            .unwrap();

        use tokio_stream::StreamExt;
        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.push(String::from_utf8(chunk.to_vec()).unwrap());
        }

        assert_eq!(collected, vec!["chunk-1", "chunk-2", "chunk-3"]);
    }
}
