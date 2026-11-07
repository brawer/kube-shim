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
//!
//! podman writes a container's stdout and stderr to separate file
//! descriptors, and SSH carries them as separate streams, so their
//! relative order is lost on the way (issue #139). Timestamped logs are
//! therefore put back in time order here: `merge_by_timestamp` for a
//! complete log, and a short merge buffer in `apply_stream` for a
//! followed one. The sort is stable, so lines with equal timestamps keep
//! their order, and a line without a timestamp stays with the line
//! before it.

use crate::ssh::OutputStream;
use axum::body::Bytes;
use chrono::{DateTime, SecondsFormat, Utc};
use std::collections::VecDeque;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;

/// How long `apply_stream` holds a followed line back, waiting for an
/// earlier line from the other stream, before writing it anyway. podman
/// writes both streams within microseconds of each other, so this only
/// needs to cover how far apart they can arrive over SSH.
const MERGE_WINDOW: Duration = Duration::from_millis(200);

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
    /// `tail_lines` lines first, as the kubelet does. Timestamped input
    /// is sorted by timestamp before anything else.
    pub fn apply(&self, input: &[u8], tail_lines: Option<usize>) -> Vec<u8> {
        // Logs cached by v0.4.2 hold all of stdout before all of stderr,
        // so they're sorted here too, not only when they're captured.
        let sorted;
        let input = if self.input_has_timestamps {
            sorted = merge_by_timestamp(&[input]);
            &sorted[..]
        } else {
            input
        };
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

    /// Applies the filter to a live log stream (`follow=true`).
    ///
    /// Without timestamps, chunks of both streams are passed on as they
    /// arrive. With timestamps, each stream's lines are reassembled until
    /// their newline (a chunk may end mid-line), then merged by timestamp:
    /// a line is written once the other stream has a line at least as
    /// late, or after `MERGE_WINDOW` (issue #139). The stream ends once
    /// `limit_bytes` is reached, which also closes the SSH channel behind
    /// `input`.
    pub fn apply_stream(
        self,
        mut input: ReceiverStream<(OutputStream, Bytes)>,
    ) -> ReceiverStream<Bytes> {
        let (tx, rx) = mpsc::channel(16);
        tokio::spawn(async move {
            let mut merger = StreamMerger::default();
            let mut remaining = self.limit_bytes;
            let mut ended = false;
            loop {
                let mut out = Vec::new();
                if self.input_has_timestamps {
                    let next = match merger.deadline() {
                        Some(deadline) => {
                            tokio::select! {
                                chunk = input.next() => Some(chunk),
                                _ = tokio::time::sleep_until(deadline) => None,
                            }
                        }
                        None => Some(input.next().await),
                    };
                    match next {
                        Some(Some((kind, data))) => merger.push(kind, &data, Instant::now()),
                        Some(None) => {
                            merger.finish(Instant::now());
                            ended = true;
                        }
                        None => {}
                    }
                    for line in merger.ready(Instant::now(), ended) {
                        self.line(&line, &mut out);
                    }
                } else {
                    match input.next().await {
                        Some((_, data)) => out.extend_from_slice(&data),
                        None => ended = true,
                    }
                }
                let mut done = ended;
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

/// One complete line of a followed log, waiting in `StreamMerger`.
struct PendingLine {
    time: Option<DateTime<Utc>>,
    received: Instant,
    line: Vec<u8>,
}

/// One stream's state in `StreamMerger`.
#[derive(Default)]
struct MergerInput {
    /// The start of a line whose newline hasn't arrived yet.
    partial: Vec<u8>,
    /// The timestamp of this stream's last line, for lines without one.
    last_time: Option<DateTime<Utc>>,
    lines: VecDeque<PendingLine>,
}

impl MergerInput {
    fn push_line(&mut self, line: Vec<u8>, received: Instant) {
        if let Some((time, _)) = split_timestamp(&line) {
            self.last_time = Some(time);
        }
        self.lines.push_back(PendingLine {
            time: self.last_time,
            received,
            line,
        });
    }
}

/// Merges the stdout and stderr of a followed log by timestamp.
#[derive(Default)]
struct StreamMerger {
    inputs: [MergerInput; 2],
}

impl StreamMerger {
    fn input(&mut self, kind: OutputStream) -> &mut MergerInput {
        match kind {
            OutputStream::Stdout => &mut self.inputs[0],
            OutputStream::Stderr => &mut self.inputs[1],
        }
    }

    /// Adds a chunk of `kind`'s output.
    fn push(&mut self, kind: OutputStream, data: &[u8], received: Instant) {
        let input = self.input(kind);
        input.partial.extend_from_slice(data);
        let complete = match input.partial.iter().rposition(|&b| b == b'\n') {
            Some(i) => i + 1,
            None => return,
        };
        let complete: Vec<u8> = input.partial.drain(..complete).collect();
        for line in complete.split_inclusive(|&b| b == b'\n') {
            input.push_line(line.to_vec(), received);
        }
    }

    /// Ends both streams: an unterminated last line counts as a line.
    fn finish(&mut self, received: Instant) {
        for input in &mut self.inputs {
            if !input.partial.is_empty() {
                let line = std::mem::take(&mut input.partial);
                input.push_line(line, received);
            }
        }
    }

    /// When the oldest waiting line must be written at the latest.
    fn deadline(&self) -> Option<Instant> {
        self.inputs
            .iter()
            .filter_map(|input| input.lines.front())
            .map(|line| line.received + MERGE_WINDOW)
            .min()
    }

    /// Takes the lines that can be written now, in timestamp order: all
    /// of them once `ended`.
    fn ready(&mut self, now: Instant, ended: bool) -> Vec<Vec<u8>> {
        let mut ready = Vec::new();
        loop {
            let [stdout, stderr] = &mut self.inputs;
            let next = match (stdout.lines.front(), stderr.lines.front()) {
                (Some(a), Some(b)) => {
                    if b.time < a.time {
                        stderr
                    } else {
                        stdout
                    }
                }
                (Some(a), None) if ended || now >= a.received + MERGE_WINDOW => stdout,
                (None, Some(b)) if ended || now >= b.received + MERGE_WINDOW => stderr,
                _ => break,
            };
            let mut line = next.lines.pop_front().unwrap().line;
            let more = self.inputs.iter().any(|input| !input.lines.is_empty());
            if more && line.last() != Some(&b'\n') {
                line.push(b'\n');
            }
            ready.push(line);
        }
        ready
    }
}

/// Merges complete logs, each a stream of podman-timestamped lines
/// (typically a container's stdout and stderr), into one log in
/// timestamp order (issue #139). The sort is stable and earlier
/// `streams` win ties. A line without a timestamp keeps the timestamp of
/// the line before it in its stream, so it stays with that line; one
/// before any timestamped line sorts first. An unterminated last line
/// gets a newline if another line follows it.
pub fn merge_by_timestamp(streams: &[&[u8]]) -> Vec<u8> {
    let mut lines: Vec<(Option<DateTime<Utc>>, &[u8])> = Vec::new();
    for stream in streams {
        let mut last_time = None;
        for line in stream.split_inclusive(|&b| b == b'\n') {
            if let Some((time, _)) = split_timestamp(line) {
                last_time = Some(time);
            }
            lines.push((last_time, line));
        }
    }
    lines.sort_by_key(|(time, _)| *time);
    let mut out = Vec::with_capacity(lines.iter().map(|(_, line)| line.len() + 1).sum());
    let count = lines.len();
    for (i, (_, line)) in lines.into_iter().enumerate() {
        out.extend_from_slice(line);
        if i + 1 < count && line.last() != Some(&b'\n') {
            out.push(b'\n');
        }
    }
    out
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

    fn stream_of(chunks: &[&'static [u8]]) -> ReceiverStream<(OutputStream, Bytes)> {
        let (tx, rx) = mpsc::channel(chunks.len().max(1));
        for chunk in chunks {
            tx.try_send((OutputStream::Stdout, Bytes::from_static(chunk)))
                .unwrap();
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

    #[test]
    fn test_merge_interleaves_stdout_and_stderr_by_timestamp() {
        // Issue #139's job: stderr's line belongs between stdout's.
        let stdout = b"2026-11-07T12:30:32.473409000Z first line\n\
                       2026-11-07T12:30:33.473398000Z second line\n\
                       2026-11-07T12:30:33.475578000Z nameserver 94.237.127.9\n";
        let stderr = b"2026-11-07T12:30:33.473721000Z to stderr\n";
        assert_eq!(
            merge_by_timestamp(&[stdout, stderr]),
            b"2026-11-07T12:30:32.473409000Z first line\n\
              2026-11-07T12:30:33.473398000Z second line\n\
              2026-11-07T12:30:33.473721000Z to stderr\n\
              2026-11-07T12:30:33.475578000Z nameserver 94.237.127.9\n"
        );
    }

    #[test]
    fn test_merge_compares_times_not_text() {
        // Same instant in podman's offset form and kubelet's form, and
        // an earlier one written with a later-looking local offset.
        let stdout = b"2026-11-07T12:00:00+02:00 b\n";
        let stderr = b"2026-11-07T10:00:00.5Z c\n2026-11-07T11:59:59+02:00 a\n";
        assert_eq!(
            merge_by_timestamp(&[stdout, stderr]),
            b"2026-11-07T11:59:59+02:00 a\n\
              2026-11-07T12:00:00+02:00 b\n\
              2026-11-07T10:00:00.5Z c\n"
        );
    }

    #[test]
    fn test_merge_is_stable_and_keeps_untimestamped_lines_in_place() {
        let stdout = b"2026-11-07T10:00:01Z out 1\ncontinued\n2026-11-07T10:00:01Z out 2\n";
        let stderr = b"no time yet\n2026-11-07T10:00:00Z err\n2026-11-07T10:00:01Z err 2";
        assert_eq!(
            merge_by_timestamp(&[stdout, stderr]),
            b"no time yet\n\
              2026-11-07T10:00:00Z err\n\
              2026-11-07T10:00:01Z out 1\n\
              continued\n\
              2026-11-07T10:00:01Z out 2\n\
              2026-11-07T10:00:01Z err 2"
        );
    }

    #[test]
    fn test_merge_terminates_a_line_that_is_followed() {
        let stdout = b"2026-11-07T10:00:02Z late, no newline";
        let stderr = b"2026-11-07T10:00:01Z early\n";
        assert_eq!(
            merge_by_timestamp(&[stderr, stdout]),
            b"2026-11-07T10:00:01Z early\n2026-11-07T10:00:02Z late, no newline"
        );
        assert_eq!(
            merge_by_timestamp(&[stdout, stderr]),
            b"2026-11-07T10:00:01Z early\n2026-11-07T10:00:02Z late, no newline"
        );
        let stderr = b"2026-11-07T10:00:03Z later\n";
        assert_eq!(
            merge_by_timestamp(&[stdout, stderr]),
            b"2026-11-07T10:00:02Z late, no newline\n2026-11-07T10:00:03Z later\n"
        );
    }

    #[test]
    fn test_apply_sorts_timestamped_logs_before_tail() {
        // A log cached by v0.4.2: all of stdout, then all of stderr.
        let filter = LogFilter {
            input_has_timestamps: true,
            ..Default::default()
        };
        let input = b"2026-11-07T10:00:00Z a\n\
                      2026-11-07T10:00:02Z c\n\
                      2026-11-07T10:00:01Z b\n";
        assert_eq!(filter.apply(input, None), b"a\nb\nc\n");
        assert_eq!(filter.apply(input, Some(1)), b"c\n");
    }

    #[test]
    fn test_apply_does_not_sort_untimestamped_logs() {
        let filter = LogFilter::default();
        let input = b"2026-11-07T10:00:02Z b\n2026-11-07T10:00:01Z a\n";
        assert_eq!(filter.apply(input, None), input);
    }

    type Chunk = (OutputStream, Bytes);

    fn tagged_stream() -> (mpsc::Sender<Chunk>, ReceiverStream<Chunk>) {
        let (tx, rx) = mpsc::channel(16);
        (tx, ReceiverStream::new(rx))
    }

    async fn send(
        tx: &mpsc::Sender<(OutputStream, Bytes)>,
        kind: OutputStream,
        data: &'static [u8],
    ) {
        tx.send((kind, Bytes::from_static(data))).await.unwrap();
    }

    #[tokio::test]
    async fn test_stream_merges_stdout_and_stderr_by_timestamp() {
        let filter = LogFilter {
            input_has_timestamps: true,
            ..Default::default()
        };
        let (tx, input) = tagged_stream();
        let output = tokio::spawn(collect(filter.apply_stream(input)));
        // stdout's lines arrive first, and split mid-line; stderr's
        // earlier line arrives a little later, within the window.
        send(
            &tx,
            OutputStream::Stdout,
            b"2026-11-07T10:00:00Z first\n2026-11-07T10:00:02Z th",
        )
        .await;
        tokio::time::sleep(Duration::from_millis(50)).await;
        send(&tx, OutputStream::Stderr, b"2026-11-07T10:00:01Z second\n").await;
        send(&tx, OutputStream::Stdout, b"ird\n").await;
        drop(tx);
        assert_eq!(output.await.unwrap(), b"first\nsecond\nthird\n");
    }

    #[tokio::test]
    async fn test_stream_writes_a_line_after_the_merge_window() {
        let filter = LogFilter {
            input_has_timestamps: true,
            ..Default::default()
        };
        let (tx, input) = tagged_stream();
        let mut output = filter.apply_stream(input);
        send(
            &tx,
            OutputStream::Stderr,
            b"2026-11-07T10:00:00Z only stderr\n",
        )
        .await;
        // Written without waiting for stdout or the end of the stream.
        let chunk = tokio::time::timeout(MERGE_WINDOW * 2, output.next())
            .await
            .expect("line written after the merge window")
            .unwrap();
        assert_eq!(&chunk[..], b"only stderr\n");
        drop(tx);
        assert!(output.next().await.is_none());
    }

    #[tokio::test]
    async fn test_untimestamped_stream_passes_both_streams_through() {
        let (tx, input) = tagged_stream();
        send(&tx, OutputStream::Stdout, b"a").await;
        send(&tx, OutputStream::Stderr, b"b\n").await;
        drop(tx);
        assert_eq!(
            collect(LogFilter::default().apply_stream(input)).await,
            b"ab\n"
        );
    }
}
