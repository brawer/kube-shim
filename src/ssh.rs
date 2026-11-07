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
//! `src/cloud_init.rs`'s base64-escaping concern here. Job-supplied data
//! that does travel over SSH (a container's resolved env, command and
//! args -- see `exec_with_stdin`) is only ever the command's *stdin*,
//! written to a file by a fixed `cat > path` command, never part of the
//! command line.

use axum::body::Bytes;
use russh::client::{self, Handle};
use russh::keys::{decode_secret_key, PrivateKeyWithHashAlg};
use russh::ChannelMsg;
use std::sync::Arc;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// Phase 11: bounds the initial TCP connect + SSH handshake. Without
/// this, a worker whose firewall silently drops packets (rather than
/// actively refusing the connection) would hang on the OS's own TCP
/// connect timeout, commonly 1-2+ minutes -- far longer than useful for
/// a reconciliation tick that's meant to retry every `FALLBACK_INTERVAL`
/// (10s).
const CONNECT_TIMEOUT: Duration = Duration::from_secs(15);

/// Separate from `CONNECT_TIMEOUT`: once connected, this is how long the
/// session may go with no traffic at all before `russh` itself tears it
/// down -- catches a connection that completed its handshake but then
/// genuinely stalled (e.g. the remote process wedged).
const INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

/// Everything `api::logs` (Phase 10) needs to SSH into a worker on
/// demand, handed to the router as an `Extension` -- separate from
/// `reconcile::job::JobContext`, which is reconcile-loop-only and not
/// otherwise reachable from the HTTP layer. `Clone` so
/// `reconcile::metrics_collector` can hand each concurrently spawned
/// per-job task its own cheap copy.
#[derive(Clone)]
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
    #[error("connection timed out after {0:?}")]
    ConnectTimeout(Duration),
}

/// The SSH extended-data type code for stderr (RFC 4254, section 5.2).
const SSH_EXTENDED_DATA_STDERR: u32 = 1;

/// The result of a one-shot (non-streaming) command, and its exit status
/// if the channel reported one before closing (`None` is rare -- an
/// abrupt disconnect -- but real, so callers must not assume it's always
/// present).
///
/// `stdout` and `stderr` hold each stream on its own. Callers that parse
/// a value (`cat /tmp/exit-code`) read `stdout`. Callers that show a
/// container's log (`podman logs`, which writes the container's stderr
/// to its own stderr, issue #121) read both and merge them by podman's
/// timestamps (`api::log_filter::merge_by_timestamp`): the order in
/// which their chunks arrive over SSH says nothing about the order the
/// container wrote them in (issue #139).
#[derive(Debug)]
pub struct ExecOutput {
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
    pub exit_status: Option<u32>,
}

/// Reads `channel` until it closes, collecting stdout and stderr. Other extended-data types and channel
/// messages are ignored.
async fn collect_output(channel: &mut russh::Channel<client::Msg>) -> ExecOutput {
    let mut output = ExecOutput {
        stdout: Vec::new(),
        stderr: Vec::new(),
        exit_status: None,
    };
    while let Some(msg) = channel.wait().await {
        match msg {
            ChannelMsg::Data { ref data } => {
                output.stdout.extend_from_slice(data);
            }
            ChannelMsg::ExtendedData { ref data, ext } if ext == SSH_EXTENDED_DATA_STDERR => {
                output.stderr.extend_from_slice(data);
            }
            ChannelMsg::ExitStatus { exit_status } => output.exit_status = Some(exit_status),
            _ => {}
        }
    }
    output
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
    connect_and_open_channel_with_timeout(host, port, private_key_pem, command, CONNECT_TIMEOUT)
        .await
}

/// Split out from `connect_and_open_channel` purely so a test can pass a
/// short timeout and verify `ConnectTimeout` actually fires, rather than
/// paying the real `CONNECT_TIMEOUT` (15s) on every test run just to
/// prove the wrapper works.
async fn connect_and_open_channel_with_timeout(
    host: &str,
    port: u16,
    private_key_pem: &str,
    command: &str,
    connect_timeout: Duration,
) -> Result<russh::Channel<client::Msg>, SshError> {
    let key_pair = decode_secret_key(private_key_pem, None)?;
    let config = Arc::new(client::Config {
        inactivity_timeout: Some(INACTIVITY_TIMEOUT),
        ..Default::default()
    });
    let mut session: Handle<Client> = tokio::time::timeout(
        connect_timeout,
        client::connect(config, (host, port), Client),
    )
    .await
    .map_err(|_| SshError::ConnectTimeout(connect_timeout))??;

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

/// Runs `command` once, collecting everything it writes to stdout and
/// stderr until the channel closes, then returns that along with the
/// exit status (see `ExecOutput`). Used for short, quick
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
    Ok(collect_output(&mut channel).await)
}

/// Runs `command` with `stdin` as its standard input (then EOF), and
/// otherwise behaves like `exec_once`. Used to deliver a job's container
/// config (env file, command, args) to its worker without it ever
/// appearing in the VM's `user_data` (issue #84): `command` is always a
/// fixed `cat > /run/kube-shim/...` string, the job-supplied bytes are
/// only ever data on the channel.
pub async fn exec_with_stdin(
    host: &str,
    port: u16,
    private_key_pem: &str,
    command: &str,
    stdin: &[u8],
) -> Result<ExecOutput, SshError> {
    let mut channel = connect_and_open_channel(host, port, private_key_pem, command).await?;
    channel.data(stdin).await?;
    channel.eof().await?;
    Ok(collect_output(&mut channel).await)
}

/// Which of a command's output streams a chunk came from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

/// Runs `command` (typically `podman logs -f {id}`), forwarding each
/// chunk of stdout and stderr to the returned stream as it arrives,
/// tagged with the stream it came from, rather than buffering it all
/// before returning -- for live log following. The two streams are kept
/// apart because SSH carries them separately, so a chunk boundary can
/// fall mid-line on either; `api::log_filter` reassembles each stream's
/// lines and merges them by timestamp (issue #139). The connection and
/// authentication happen before this returns, so a
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
) -> Result<ReceiverStream<(OutputStream, Bytes)>, SshError> {
    let mut channel = connect_and_open_channel(host, port, private_key_pem, command).await?;

    let (tx, rx) = mpsc::channel(16);
    tokio::spawn(async move {
        while let Some(msg) = channel.wait().await {
            let chunk = match msg {
                ChannelMsg::Data { data } => (OutputStream::Stdout, data),
                ChannelMsg::ExtendedData { data, ext } if ext == SSH_EXTENDED_DATA_STDERR => {
                    (OutputStream::Stderr, data)
                }
                _ => continue,
            };
            if tx.send(chunk).await.is_err() {
                // Receiver (the HTTP response body) is gone -- the
                // client disconnected. Stop reading, let `channel`
                // drop and close the SSH side too.
                break;
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
        let responses = responses
            .into_iter()
            .map(|(command, (stdout, exit_status))| {
                (command, (vec![MockOut::Stdout(stdout)], exit_status))
            })
            .collect();
        mock_ssh_server_with_output(responses).await
    }

    /// One chunk a mock server writes, on stdout or on stderr.
    #[derive(Clone, Copy, Debug)]
    pub(crate) enum MockOut {
        Stdout(&'static str),
        Stderr(&'static str),
    }

    /// Like `mock_ssh_server`, but each command's reply is a sequence of
    /// stdout/stderr chunks, sent in order -- for testing that callers
    /// see a container's stderr too (issue #121).
    pub(crate) async fn mock_ssh_server_with_output(
        responses: HashMap<&'static str, (Vec<MockOut>, u32)>,
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
        responses: Arc<HashMap<&'static str, (Vec<MockOut>, u32)>>,
    }

    fn send_mock_out(
        session: &mut Session,
        channel: ChannelId,
        out: MockOut,
    ) -> Result<(), russh::Error> {
        match out {
            MockOut::Stdout(text) => session.data(channel, Bytes::from(text)),
            MockOut::Stderr(text) => {
                session.extended_data(channel, SSH_EXTENDED_DATA_STDERR, Bytes::from(text))
            }
        }
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
            let (output, exit_status) = self
                .responses
                .get(command.as_str())
                .cloned()
                .unwrap_or((Vec::new(), 1));
            for out in output {
                if !matches!(out, MockOut::Stdout("") | MockOut::Stderr("")) {
                    send_mock_out(session, channel, out)?;
                }
            }
            session.exit_status_request(channel, exit_status)?;
            session.eof(channel)?;
            session.close(channel)?;
            Ok(())
        }
    }

    /// Every `(command, stdin)` pair an upload mock server received, in
    /// the order the commands were run.
    pub(crate) type ReceivedUploads = Arc<StdMutex<Vec<(String, Vec<u8>)>>>;

    /// A mock server for `exec_with_stdin`: records each exec'd command
    /// together with everything sent on its stdin, and only answers
    /// (exit status `exit_status`) once the client sends EOF -- the same
    /// order a real `cat > file` follows.
    pub(crate) async fn mock_upload_ssh_server(exit_status: u32) -> (String, u16, ReceivedUploads) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr: SocketAddr = listener.local_addr().unwrap();

        let config = Arc::new(server::Config {
            keys: vec![PrivateKey::random(&mut rand::rng(), Algorithm::Ed25519).unwrap()],
            ..Default::default()
        });
        let received: ReceivedUploads = Arc::new(StdMutex::new(Vec::new()));
        let mut server = UploadTestServer {
            received: received.clone(),
            exit_status,
            current: HashMap::new(),
        };

        tokio::spawn(async move {
            server.run_on_socket(config, &listener).await.unwrap();
        });

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        (addr.ip().to_string(), addr.port(), received)
    }

    #[derive(Clone)]
    struct UploadTestServer {
        received: ReceivedUploads,
        exit_status: u32,
        current: HashMap<ChannelId, (String, Vec<u8>)>,
    }

    impl server::Server for UploadTestServer {
        type Handler = Self;
        fn new_client(&mut self, _: Option<SocketAddr>) -> Self {
            self.clone()
        }
    }

    impl server::Handler for UploadTestServer {
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
            self.current.insert(channel, (command, Vec::new()));
            session.channel_success(channel)?;
            Ok(())
        }

        async fn data(
            &mut self,
            channel: ChannelId,
            data: &[u8],
            _session: &mut Session,
        ) -> Result<(), Self::Error> {
            if let Some((_, stdin)) = self.current.get_mut(&channel) {
                stdin.extend_from_slice(data);
            }
            Ok(())
        }

        async fn channel_eof(
            &mut self,
            channel: ChannelId,
            session: &mut Session,
        ) -> Result<(), Self::Error> {
            if let Some(upload) = self.current.remove(&channel) {
                self.received.lock().unwrap().push(upload);
            }
            session.exit_status_request(channel, self.exit_status)?;
            session.eof(channel)?;
            session.close(channel)?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_exec_with_stdin_sends_stdin_and_returns_exit_status() {
        let (host, port, received) = mock_upload_ssh_server(0).await;
        let key = throwaway_private_key_pem();

        let output = exec_with_stdin(&host, port, &key, "cat > /x", b"A=1\nB=2\n")
            .await
            .unwrap();

        assert_eq!(output.exit_status, Some(0));
        let received = received.lock().unwrap();
        assert_eq!(
            received.as_slice(),
            &[("cat > /x".to_string(), b"A=1\nB=2\n".to_vec())]
        );
    }

    // A slow/streaming variant used by the exec_stream test, kept
    // separate from TestServer above since it needs to send multiple
    // Data messages over time rather than one fixed canned reply.
    #[derive(Clone)]
    struct StreamingTestServer {
        chunks: Arc<StdMutex<Vec<MockOut>>>,
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
                    let _ = match chunk {
                        MockOut::Stdout(text) => handle.data(channel, Bytes::from(text)).await,
                        MockOut::Stderr(text) => {
                            handle
                                .extended_data(channel, SSH_EXTENDED_DATA_STDERR, Bytes::from(text))
                                .await
                        }
                    };
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                let _ = handle.eof(channel).await;
                let _ = handle.close(channel).await;
            });
            Ok(())
        }
    }

    async fn mock_streaming_ssh_server(chunks: Vec<MockOut>) -> (String, u16) {
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
    async fn test_connect_timeout_fires_against_a_silent_peer() {
        // A real TCP listener that never calls accept(): the TCP
        // handshake itself completes (the kernel ACKs from the listen
        // backlog regardless of the application), but russh's own SSH
        // banner/version exchange then hangs forever waiting for a peer
        // that never speaks -- exactly the "connected but silent" case
        // CONNECT_TIMEOUT exists to bound, verified here with a short
        // timeout rather than the real 15s one.
        // Bound but never accept()-ed from -- kept alive for the whole
        // test so the OS socket stays open without ever being read from.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let addr = listener.local_addr().unwrap();

        let key = throwaway_private_key_pem();
        let err = super::connect_and_open_channel_with_timeout(
            "127.0.0.1",
            addr.port(),
            &key,
            "echo hi",
            Duration::from_millis(200),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, SshError::ConnectTimeout(_)));
    }

    #[tokio::test]
    async fn test_exec_stream_forwards_chunks_as_they_arrive() {
        let (host, port) = mock_streaming_ssh_server(vec![
            MockOut::Stdout("chunk-1"),
            MockOut::Stderr("chunk-2"),
            MockOut::Stdout("chunk-3"),
        ])
        .await;
        let key = throwaway_private_key_pem();

        let mut stream = exec_stream(&host, port, &key, "podman logs -f abc")
            .await
            .unwrap();

        use tokio_stream::StreamExt;
        let mut collected = Vec::new();
        while let Some((kind, chunk)) = stream.next().await {
            collected.push((kind, String::from_utf8(chunk.to_vec()).unwrap()));
        }

        assert_eq!(
            collected,
            vec![
                (OutputStream::Stdout, "chunk-1".to_string()),
                (OutputStream::Stderr, "chunk-2".to_string()),
                (OutputStream::Stdout, "chunk-3".to_string()),
            ],
            "stderr chunks are forwarded too, tagged, in arrival order"
        );
    }

    #[tokio::test]
    async fn test_exec_once_collects_stdout_and_stderr_separately() {
        let mut responses = HashMap::new();
        responses.insert(
            "podman logs abc",
            (
                vec![
                    MockOut::Stdout("out 1\n"),
                    MockOut::Stderr("err 1\n"),
                    MockOut::Stdout("out 2\n"),
                ],
                0,
            ),
        );
        let (host, port) = mock_ssh_server_with_output(responses).await;
        let key = throwaway_private_key_pem();

        let output = exec_once(&host, port, &key, "podman logs abc")
            .await
            .unwrap();

        assert_eq!(output.stdout, b"out 1\nout 2\n");
        assert_eq!(output.stderr, b"err 1\n");
        assert_eq!(output.exit_status, Some(0));
    }
}
