//! Line-level processing of container logs for `GET .../pods/{name}/log`
//! (issue #131): the `timestamps`, `tailLines`, `sinceSeconds`/`sinceTime`
//! and `limitBytes` options of Kubernetes' `PodLogOptions`.
//!
//! Timestamps come from podman (`podman logs --timestamps`), which puts
//! the time the line was received in front of every line, then a space.
//! podman formats it as Go's RFC3339Nano in the worker's own time zone,
//! e.g. `2026-11-07T10:26:27.5012345+00:00`. Kubernetes' kubelet uses a
//! fixed nine-digit RFC3339Nano in UTC instead
//! (`2026-11-07T10:26:27.501234500Z`), so every timestamp is rewritten to
//! that form here.
//!
//! A line whose first field isn't an RFC3339 timestamp is passed through
//! unchanged. That covers logs cached before this change and the
//! worker's own start-error text, which never had timestamps.

use axum::body::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// What to do with each line, from the request's query parameters.
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// The input lines start with a podman timestamp.
    pub input_has_timestamps: bool,
    /// Keep the (normalized) timestamp in the output.
    pub timestamps: bool,
    /// Drop lines written before this time. Only applies when the input
    /// has timestamps.
    pub since: Option<DateTime<Utc>>,
    /// Stop after this many bytes of output.
    pub limit_bytes: Option<usize>,
}

impl LogFilter {
    /// True if the input can be passed through unchanged.
    fn is_identity(&self) -> bool {
        !self.input_has_timestamps && self.limit_bytes.is_none()
    }

    /// Applies the filter to one line, terminator included.
    fn line(&self, line: &[u8], out: &mut Vec<u8>) {
        if !self.input_has_timestamps {
            out.extend_from_slice(line);
            return;
        }
        match split_timestamp(line) {
            Some((time, rest)) => {
                if self.since.is_some_and(|since| time < since) {
                    return;
                }
                if self.timestamps {
                    out.extend_from_slice(format_timestamp(time).as_bytes());
                    out.push(b' ');
                }
                out.extend_from_slice(rest);
            }
            None => out.extend_from_slice(line),
        }
    }

    /// Applies the filter to a complete log, keeping only its last
    /// `tail_lines` lines first, as the kubelet does.
    pub fn apply(&self, input: &[u8], tail_lines: Option<usize>) -> Vec<u8> {
        let lines: Vec<&[u8]> = input.split_inclusive(|&b| b == b'\n').collect();
        let start = match tail_lines {
            Some(n) => lines.len().saturating_sub(n),
            None => 0,
        };
        let mut out = Vec::with_capacity(input.len());
        for line in &lines[start..] {
            self.line(line, &mut out);
        }
        if let Some(limit) = self.limit_bytes {
            out.truncate(limit);
        }
        out
    }

    /// Applies the filter to a live log stream (`follow=true`). Lines are
    /// buffered until their newline when they carry a timestamp, since
    /// the timestamp has to be read before the line can be written.
    /// The stream ends once `limit_bytes` is reached, which also closes
    /// the SSH channel behind `input`.
    pub fn apply_stream(self, mut input: ReceiverStream<Bytes>) -> ReceiverStream<Bytes> {
        if self.is_identity() {
            return input;
        }
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut pending: Vec<u8> = Vec::new();
            let mut remaining = self.limit_bytes;
            loop {
                let chunk = input.next().await;
                let mut out = Vec::new();
                match &chunk {
                    Some(data) if self.input_has_timestamps => {
                        pending.extend_from_slice(data);
                        let complete = match pending.iter().rposition(|&b| b == b'\n') {
                            Some(i) => i + 1,
                            None => 0,
                        };
                        for line in pending[..complete].split_inclusive(|&b| b == b'\n') {
                            self.line(line, &mut out);
                        }
                        pending.drain(..complete);
                    }
                    Some(data) => out.extend_from_slice(data),
                    None => {
                        if !pending.is_empty() {
                            self.line(&pending, &mut out);
                        }
                    }
                }
                let mut done = chunk.is_none();
                if let Some(left) = remaining.as_mut() {
                    if out.len() >= *left {
                        out.truncate(*left);
                        done = true;
                    }
                    *left -= out.len();
                }
                if !out.is_empty() && tx.send(Bytes::from(out)).await.is_err() {
                    break;
                }
                if done {
                    break;
                }
            }
        });
        ReceiverStream::new(rx)
    }
}

/// Splits a line into its leading RFC3339 timestamp and the rest after
/// the separating space, or `None` if the line doesn't start with one.
fn split_timestamp(line: &[u8]) -> Option<(DateTime<Utc>, &[u8])> {
    let space = line.iter().position(|&b| b == b' ')?;
    let field = std::str::from_utf8(&line[..space]).ok()?;
    let time = DateTime::parse_from_rfc3339(field).ok()?;
    Some((time.with_timezone(&Utc), &line[space + 1..]))
}

/// The kubelet's timestamp format: RFC3339 with nine fractional digits,
/// in UTC with `Z`.
pub fn format_timestamp(time: DateTime<Utc>) -> String {
    time.to_rfc3339_opts(SecondsFormat::Nanos, true)
}

/// Rewrites every leading podman timestamp in `logs` to the kubelet's
/// form, leaving everything else as it is. Used once, when a finished
/// job's logs are cached.
pub fn normalize_timestamps(logs: &[u8]) -> Vec<u8> {
    LogFilter {
        input_has_timestamps: true,
        timestamps: true,
        ..Default::default()
    }
    .apply(logs, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn time(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn test_normalizes_podman_offsets_to_utc_nanos() {
        let input = b"2026-11-07T10:26:27.5012345+00:00 first\n\
                      2026-11-07T12:26:28+02:00 second\n";
        assert_eq!(
            normalize_timestamps(input),
            b"2026-11-07T10:26:27.501234500Z first\n\
              2026-11-07T10:26:28.000000000Z second\n"
        );
    }

    #[test]
    fn test_line_without_timestamp_is_unchanged() {
        let input = b"no timestamp here\n2026-11-07T10:26:27Z ok\n";
        assert_eq!(
            normalize_timestamps(input),
            b"no timestamp here\n2026-11-07T10:26:27.000000000Z ok\n"
        );
    }

    #[test]
    fn test_strips_timestamps_when_not_requested() {
        let filter = LogFilter {
            input_has_timestamps: true,
            ..Default::default()
        };
        let input = b"2026-11-07T10:26:27.501234567Z a b\n2026-11-07T10:26:28Z\n";
        // The second line is a timestamp with nothing after it: not a
        // timestamped line (no separating space), so it's kept as is.
        assert_eq!(filter.apply(input, None), b"a b\n2026-11-07T10:26:28Z\n");
    }

    #[test]
    fn test_empty_line_keeps_its_timestamp() {
        let filter = LogFilter {
            input_has_timestamps: true,
            timestamps: true,
            ..Default::default()
        };
        assert_eq!(
            filter.apply(b"2026-11-07T10:26:27Z \n", None),
            b"2026-11-07T10:26:27.000000000Z \n"
        );
    }

    #[test]
    fn test_since_drops_earlier_lines() {
        let filter = LogFilter {
            input_has_timestamps: true,
            since: Some(time("2026-11-07T10:00:01Z")),
            ..Default::default()
        };
        let input = b"2026-11-07T10:00:00Z old\n\
                      2026-11-07T10:00:01Z edge\n\
                      2026-11-07T10:00:02Z new\n";
        assert_eq!(filter.apply(input, None), b"edge\nnew\n");
    }

    #[test]
    fn test_since_is_ignored_without_timestamps() {
        let filter = LogFilter {
            since: Some(time("2026-11-07T10:00:01Z")),
            ..Default::default()
        };
        assert_eq!(filter.apply(b"a\nb\n", None), b"a\nb\n");
    }

    #[test]
    fn test_tail_lines() {
        let filter = LogFilter::default();
        assert_eq!(filter.apply(b"a\nb\nc\n", Some(2)), b"b\nc\n");
        assert_eq!(filter.apply(b"a\nb\nc", Some(1)), b"c");
        assert_eq!(filter.apply(b"a\nb\n", Some(5)), b"a\nb\n");
        assert_eq!(filter.apply(b"a\nb\n", Some(0)), b"");
    }

    #[test]
    fn test_limit_bytes() {
        let filter = LogFilter {
            limit_bytes: Some(3),
            ..Default::default()
        };
        assert_eq!(filter.apply(b"abcdef\n", None), b"abc");
    }

    async fn collect(stream: ReceiverStream<Bytes>) -> Vec<u8> {
        let chunks: Vec<Bytes> = stream.collect().await;
        chunks.concat()
    }

    fn stream_of(chunks: &[&'static [u8]]) -> ReceiverStream<Bytes> {
        let (tx, rx) = mpsc::channel(chunks.len().max(1));
        for chunk in chunks {
            tx.try_send(Bytes::from_static(chunk)).unwrap();
        }
        ReceiverStream::new(rx)
    }

    #[tokio::test]
    async fn test_stream_handles_lines_split_across_chunks() {
        let filter = LogFilter {
            input_has_timestamps: true,
            timestamps: true,
            ..Default::default()
        };
        let input = stream_of(&[
            b"2026-11-07T10:26:27+00:00 fi",
            b"rst\n2026-11-07T10:26:28+00:00 second\n2026-11-07T10:26:29+00:00 no newline",
        ]);
        assert_eq!(
            collect(filter.apply_stream(input)).await,
            b"2026-11-07T10:26:27.000000000Z first\n\
              2026-11-07T10:26:28.000000000Z second\n\
              2026-11-07T10:26:29.000000000Z no newline"
        );
    }

    #[tokio::test]
    async fn test_stream_stops_at_limit_bytes() {
        let filter = LogFilter {
            limit_bytes: Some(5),
            ..Default::default()
        };
        let input = stream_of(&[b"abc", b"def", b"ghi"]);
        assert_eq!(collect(filter.apply_stream(input)).await, b"abcde");
    }

    #[tokio::test]
    async fn test_identity_stream_is_passed_through() {
        let input = stream_of(&[b"a", b"b\n"]);
        assert_eq!(
            collect(LogFilter::default().apply_stream(input)).await,
            b"ab\n"
        );
    }
}
