//! Cross-implementation wire-contract test: firmware codec <-> backend.
//!
//! `src/proto.rs` + serde_json is normative for the bytes (`docs/protocol.md` says so in
//! as many words). The ESPHome component hand-writes the same format in
//! `firmware/components/luna_stream/luna_stream.h` (namespace `luna_stream_codec`). Both
//! sides were only ever tested against themselves, so nothing here trusted either one:
//! this file compiles the *production* firmware codec with `g++` into a small harness
//! (`firmware/components/luna_stream/tests/wire_diff.cpp`, which adds no codec of its own)
//! and compares the two implementations byte for byte in both directions.
//!
//! Directions:
//! - **uplink** — every message the firmware can build (`build_hello`, `build_audio_record`,
//!   `build_event_*`) must equal what `encode_hello_line` / `encode_audio_frame` /
//!   `encode_event` produce, and must parse back to the same values through
//!   `decode_hello_line` / `parse_audio_frame` / `parse_event`.
//! - **downlink** — every control the backend can emit (`encode_control`) must be accepted
//!   by the firmware's real `DownlinkDecoder` with identical field values.
//!
//! Deliberate asymmetries are named and asserted in both directions rather than skipped:
//! the firmware has fixed buffers, so a payload past a documented cap is *refused* (never
//! truncated into a valid-looking message) while serde accepts it. Every other case must
//! agree exactly.
//!
//! No new crate dependencies: the harness is compiled with the system `g++`.
//!
//! Every child process here is bounded, and an expired bound is a named failure rather than a
//! skip or a pass: the `g++` build has 120 s, each `emit`/`check` run has 10 s, the child's
//! whole process group is killed on expiry, and the message is `HARNESS BUILD TIMEOUT after N
//! ms` or `HARNESS TIMEOUT: check <case> after N ms`. `run_check` writes all input and closes
//! the write half before waiting — a `check` that never sees EOF would otherwise wait forever,
//! and that hang is created by the caller, not by the harness.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{Read, Write};
use std::os::unix::process::CommandExt;
use std::panic::{self, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use listenrr::proto::{
    decode_hello_line, encode_audio_frame, encode_control, encode_event, encode_hello_line,
    parse_audio_frame, parse_control_json, parse_event, parse_event_json, AudioFrame, Caps,
    Control, Event, Hello, LedState, AUDIO_RECORD_LEN, PROTOCOL_VERSION,
};

// ---------------------------------------------------------------------------
// harness: compile the firmware codec once, then drive it
// ---------------------------------------------------------------------------

/// The compiled firmware harness, built once per test binary.
static HARNESS: LazyLock<PathBuf> = LazyLock::new(build_harness);

fn harness() -> &'static Path {
    HARNESS.as_path()
}

// ---------------------------------------------------------------------------
// bounded child processes
//
// Every wait in this file has a deadline, and an expired deadline is a NAMED FAILURE — not a
// skip, not a retryable/environmental bucket. If the compiler or the harness did not
// terminate, that is the finding and the message says so.
// ---------------------------------------------------------------------------

/// `g++` on a cold cache; far above anything legitimate, low enough that a wedged compiler
/// cannot park a suite.
const BUILD_TIMEOUT: Duration = Duration::from_secs(120);
/// One `emit` or `check` run. Both are table-driven and finish in milliseconds.
const RUN_TIMEOUT: Duration = Duration::from_secs(10);
/// Only the watchdog probe uses this: it has to expire for the bound to be witnessed.
const WATCHDOG_TIMEOUT: Duration = Duration::from_millis(200);

#[derive(Debug)]
struct Bounded {
    status: ExitStatus,
    stdout: Vec<u8>,
    stderr: Vec<u8>,
}

/// Why a bounded run did not produce output.
#[derive(Debug)]
enum BoundedError {
    /// The deadline expired; the child and its group were killed.
    Timeout { label: String, after_ms: u128 },
    /// The process could not be started at all (missing tool, permissions).
    Spawn { label: String, cause: String },
}

impl std::fmt::Display for BoundedError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BoundedError::Timeout { label, after_ms } => {
                write!(f, "{label} after {after_ms} ms")
            }
            BoundedError::Spawn { label, cause } => {
                write!(
                    f,
                    "{label}: the child process could not be started: {cause}"
                )
            }
        }
    }
}

/// Wait for `child` for at most `deadline`; `None` means it did not terminate in time.
fn wait_bounded(child: &mut Child, deadline: Duration) -> Option<ExitStatus> {
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => return Some(status),
            Ok(None) => {}
            Err(err) => panic!("waiting on a child process failed: {err}"),
        }
        if started.elapsed() >= deadline {
            return None;
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

/// Kill the child's whole process *group*, not just the child: `g++` spawns `cc1plus`, `as` and
/// `ld`, and a grandchild left holding a pipe or the staging path looks busy to whoever runs
/// next. Every child here is spawned with `process_group(0)`, so its pid is the group id.
fn kill_group(child: &mut Child) {
    let pid = child.id();
    let _ = Command::new("kill")
        .args(["-KILL", "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    let _ = child.kill(); // belt and braces if `kill`(1) is absent
    let _ = child.wait(); // reap, so no zombie is left behind
}

/// Run a child with drained pipes and a bounded wait.
///
/// `keep_stdin_open` exists for exactly one caller: the watchdog probe. `check` reads stdin to
/// EOF, so a parent that holds the write half makes the harness wait forever — the hang this
/// deadline has to catch, and one the caller creates.
fn run_bounded(
    cmd: &mut Command,
    input: Option<&[u8]>,
    keep_stdin_open: bool,
    deadline: Duration,
    label: &str,
) -> Result<Bounded, BoundedError> {
    let spawn_error = |cause: String| BoundedError::Spawn {
        label: label.to_string(),
        cause,
    };
    cmd.stdin(if input.is_some() {
        Stdio::piped()
    } else {
        Stdio::null()
    })
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .process_group(0);
    let mut child = cmd.spawn().map_err(|e| spawn_error(e.to_string()))?;

    let mut stdout = child.stdout.take().expect("stdout pipe");
    let mut stderr = child.stderr.take().expect("stderr pipe");
    // Drain both pipes while waiting: a full pipe buffer would block the child, and the
    // deadline would then blame a healthy process.
    let stdout_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stdout.read_to_end(&mut buf);
        buf
    });
    let stderr_reader = std::thread::spawn(move || {
        let mut buf = Vec::new();
        let _ = stderr.read_to_end(&mut buf);
        buf
    });

    let mut held = child.stdin.take();
    if let (Some(pipe), Some(bytes)) = (held.as_mut(), input) {
        pipe.write_all(bytes)
            .unwrap_or_else(|err| panic!("{label}: writing to the child failed: {err}"));
    }
    if !keep_stdin_open {
        // Close the write half before waiting. `check` reads until EOF, so holding this open
        // would hang the harness; the bound belongs to the caller and this is where it is kept.
        drop(held.take());
    }

    match wait_bounded(&mut child, deadline) {
        Some(status) => Ok(Bounded {
            status,
            stdout: stdout_reader.join().expect("stdout reader thread"),
            stderr: stderr_reader.join().expect("stderr reader thread"),
        }),
        None => {
            kill_group(&mut child);
            // Kill first, then let the read halves close; a survivor holding a pipe would
            // otherwise make these joins the next unbounded wait.
            drop(held.take());
            let _ = stdout_reader.join();
            let _ = stderr_reader.join();
            Err(BoundedError::Timeout {
                label: label.to_string(),
                after_ms: deadline.as_millis(),
            })
        }
    }
}

/// Panic with the error's own text: `LABEL after N ms` or `LABEL: could not be started: cause`.
fn unwrap_bounded(result: Result<Bounded, BoundedError>) -> Bounded {
    match result {
        Ok(out) => out,
        Err(err) => panic!("{err}"),
    }
}

fn build_harness() -> PathBuf {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let source = manifest.join("firmware/components/luna_stream/tests/wire_diff.cpp");
    assert!(
        source.is_file(),
        "the differential harness source is missing: {}",
        source.display()
    );

    // A skipped comparison is not a pass: no g++, no green. Both the probe and the compile are
    // bounded, so a wedged toolchain fails as `HARNESS BUILD TIMEOUT after N ms`.
    match run_bounded(
        Command::new("g++").arg("--version"),
        None,
        false,
        BUILD_TIMEOUT,
        "HARNESS BUILD TIMEOUT: g++ --version",
    ) {
        Ok(out) if out.status.success() => {}
        Ok(out) => panic!(
            "g++ --version failed ({}): {}",
            out.status,
            String::from_utf8_lossy(&out.stderr)
        ),
        Err(BoundedError::Spawn { cause, .. }) => panic!(
            "g++ is required to build the wire-contract harness ({}): {cause}\ninstall it, e.g. \
             `nix shell nixpkgs#gcc`, rather than skipping the firmware/backend comparison",
            source.display()
        ),
        Err(err) => panic!("{err}"),
    }

    let target = std::env::var_os("CARGO_TARGET_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest.join("target"));
    std::fs::create_dir_all(&target).expect("create the cargo target directory");
    let binary = target.join("wire_diff");
    // Build into a staging path that is unique per process *and* per invocation, then rename it
    // into place: several runs (other lanes, other worktrees, a re-entrant test binary) share
    // CARGO_TARGET_DIR, and no two of them may write the same output file — a half-written or
    // contended binary must never be executed. g++'s own temporaries go to the system temp dir,
    // which is unique per process by construction.
    let stamp = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    let staging = target.join(format!("wire_diff.{}.{stamp}", std::process::id()));

    // The flags the header's own host-test comment documents, plus the two defines
    // that select the host build and suppress the header's self-test `main`.
    let out = unwrap_bounded(run_bounded(
        Command::new("g++")
            .args([
                "-std=c++17",
                "-Wall",
                "-Wextra",
                "-Werror",
                "-Wno-pragma-once-outside-header",
                "-O2",
                "-DLUNA_STREAM_HOST_TEST",
                "-DLUNA_STREAM_NO_TEST_MAIN",
                "-x",
                "c++",
            ])
            .arg(&source)
            .arg("-o")
            .arg(&staging),
        None,
        false,
        BUILD_TIMEOUT,
        "HARNESS BUILD TIMEOUT",
    ));
    if !out.status.success() {
        let _ = std::fs::remove_file(&staging);
        panic!(
            "g++ failed to build the firmware wire harness:\n{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
    std::fs::rename(&staging, &binary).expect("publish the compiled harness");
    binary
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

fn hex_decode(hex: &str) -> Vec<u8> {
    assert!(
        hex.len().is_multiple_of(2),
        "hex payload has an odd number of digits: {hex}"
    );
    let digits = hex.as_bytes();
    let value = |c: u8| -> u8 {
        match c {
            b'0'..=b'9' => c - b'0',
            b'a'..=b'f' => c - b'a' + 10,
            b'A'..=b'F' => c - b'A' + 10,
            other => panic!("not a hex digit: {:?}", other as char),
        }
    };
    digits
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| (value(pair[0]) << 4) | value(pair[1]))
        .collect()
}

/// What the firmware emitted for one case.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Emission {
    Bytes(Vec<u8>),
    /// The production builder declined an over-cap input: a visible refusal, never a
    /// truncated record that would look like a shorter valid message.
    Refused,
}

fn run_emit() -> BTreeMap<String, Emission> {
    let out = unwrap_bounded(run_bounded(
        Command::new(harness()).arg("emit"),
        None,
        false,
        RUN_TIMEOUT,
        "HARNESS TIMEOUT: emit",
    ));
    let stdout = String::from_utf8(out.stdout).expect("harness stdout is UTF-8");
    let mut cases = BTreeMap::new();
    for line in stdout.lines() {
        assert!(
            !line.starts_with("FAIL"),
            "the firmware harness could not build a message it must be able to build: {line}"
        );
        let mut parts = line.split(' ');
        assert_eq!(parts.next(), Some("UP"), "unexpected emit line: {line}");
        let id = parts
            .next()
            .unwrap_or_else(|| panic!("missing case id: {line}"));
        let rest: Vec<&str> = parts.collect();
        let emission = match rest.as_slice() {
            ["REFUSED"] => Emission::Refused,
            [hex] => Emission::Bytes(hex_decode(hex)),
            _ => panic!("malformed emit line: {line}"),
        };
        let previous = cases.insert(id.to_string(), emission);
        assert!(previous.is_none(), "duplicate emit case id {id}");
    }
    assert_eq!(
        out.status.code(),
        Some(0),
        "`wire_diff emit` exited with {:?}; stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        !cases.is_empty(),
        "the harness emitted no cases at all — a comparison against nothing is a false green"
    );
    cases
}

/// Feed complete `0x03` control records to the firmware decoder; one verdict per case, in
/// the harness's own words (`accept <kind> <hex…>` or `reject`).
fn run_check(cases: &[(String, Vec<u8>)]) -> BTreeMap<String, String> {
    let mut input = Vec::new();
    for (id, record) in cases {
        writeln!(input, "DOWN {id} {}", hex_encode(record)).expect("format a control case");
    }
    // Name what hung: a single case by id, a batch by its first..last case and size.
    let label = match (cases.first(), cases.last()) {
        (Some((first, _)), _) if cases.len() == 1 => format!("HARNESS TIMEOUT: check {first}"),
        (Some((first, _)), Some((last, _))) => format!(
            "HARNESS TIMEOUT: check batch of {} cases ({first}..{last})",
            cases.len()
        ),
        _ => "HARNESS TIMEOUT: check (empty batch)".to_string(),
    };
    let out = unwrap_bounded(run_bounded(
        Command::new(harness()).arg("check"),
        Some(&input),
        false,
        RUN_TIMEOUT,
        &label,
    ));
    assert_eq!(
        out.status.code(),
        Some(0),
        "`wire_diff check` exited with {:?}; stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).expect("harness stdout is UTF-8");
    let mut verdicts = BTreeMap::new();
    for line in stdout.lines() {
        let mut parts = line.splitn(3, ' ');
        assert_eq!(parts.next(), Some("DOWN"), "unexpected check line: {line}");
        let id = parts
            .next()
            .unwrap_or_else(|| panic!("missing case id: {line}"));
        let verdict = parts
            .next()
            .unwrap_or_else(|| panic!("missing verdict: {line}"));
        verdicts.insert(id.to_string(), verdict.to_string());
    }
    assert_eq!(
        verdicts.len(),
        cases.len(),
        "the harness answered {} of {} control cases — every case must be acted on",
        verdicts.len(),
        cases.len()
    );
    verdicts
}

/// What the decoder reported for a record fed in pieces. This is the shape a TCP socket
/// actually delivers, and it is a different claim from `run_check`'s single verdict: here
/// the *sequence* of `feed` outcomes is the assertion. A decoder that latched a partial
/// record into a fresh state, or dropped the bytes after a complete record, would still
/// produce the right final answer to `check` and the wrong sequence here.
#[derive(Debug, Default, PartialEq, Eq)]
struct FragRun {
    /// One entry per `feed` call, in order: `need_more`, `pending` or `error`.
    statuses: Vec<String>,
    /// One entry per control the decoder produced, in the same verdict form `check` uses,
    /// or `error <code>` naming the decoder's error code (2 = payload past
    /// `MAX_CONTROL_JSON`, 3 = the payload was not a control).
    controls: Vec<String>,
}

/// Feed fragments of control records through the real `DownlinkDecoder`, one `feed` per
/// fragment, and return what it said every time.
fn run_check_fragments(cases: &[(String, Vec<Vec<u8>>)]) -> BTreeMap<String, FragRun> {
    let mut input = Vec::new();
    for (id, fragments) in cases {
        assert!(
            !fragments.is_empty(),
            "{id}: a fragment case with no fragments is a whole-record case in disguise"
        );
        let spec: Vec<String> = fragments.iter().map(|f| hex_encode(f)).collect();
        writeln!(input, "DOWN {id} {}", spec.join("|")).expect("format a fragment case");
    }
    let label = match (cases.first(), cases.last()) {
        (Some((first, _)), Some((last, _))) if cases.len() == 1 => {
            format!("HARNESS TIMEOUT: check_frag {first}")
        }
        (Some((first, _)), Some((last, _))) => format!(
            "HARNESS TIMEOUT: check_frag batch of {} cases ({first}..{last})",
            cases.len()
        ),
        _ => "HARNESS TIMEOUT: check_frag (empty batch)".to_string(),
    };
    let out = unwrap_bounded(run_bounded(
        Command::new(harness()).arg("check_frag"),
        Some(&input),
        false,
        RUN_TIMEOUT,
        &label,
    ));
    assert_eq!(
        out.status.code(),
        Some(0),
        "`wire_diff check_frag` exited with {:?}; stderr: {}",
        out.status.code(),
        String::from_utf8_lossy(&out.stderr)
    );
    let stdout = String::from_utf8(out.stdout).expect("harness stdout is UTF-8");
    let mut runs: BTreeMap<String, FragRun> = BTreeMap::new();
    for line in stdout.lines() {
        if line.starts_with("ERR") {
            // A caller-side bug (bad hex, an empty fragment, a feed that made no progress).
            // Surfacing it as a panic keeps it from being read as "the firmware refused".
            panic!("the fragment harness reported a caller-side error, not a verdict: {line}");
        }
        let mut parts = line.splitn(3, ' ');
        let kind = parts.next().unwrap_or_default();
        let id = parts
            .next()
            .unwrap_or_else(|| panic!("missing case id: {line}"));
        let rest = parts
            .next()
            .unwrap_or_else(|| panic!("missing payload on: {line}"));
        let run = runs.entry(id.to_string()).or_default();
        match kind {
            "FRAG" => {
                let (ordinal, status) = rest
                    .split_once(' ')
                    .unwrap_or_else(|| panic!("malformed FRAG line: {line}"));
                let index: usize = ordinal
                    .parse()
                    .unwrap_or_else(|_| panic!("non-numeric feed ordinal: {line}"));
                assert_eq!(
                    index,
                    run.statuses.len(),
                    "{line}: the harness skipped or repeated a feed ordinal"
                );
                run.statuses.push(status.to_string());
            }
            "CTL" => run.controls.push(rest.to_string()),
            other => panic!("unexpected fragment harness line: {line} ({other})"),
        }
    }
    for (id, _) in cases {
        assert!(
            runs.contains_key(id.as_str()),
            "the fragment harness answered nothing for {id}"
        );
    }
    runs
}

/// The verdict string the firmware must produce for a control it acts on.
fn expected_verdict(control: &Control) -> String {
    match control {
        Control::Led { state } => {
            let name = match state {
                LedState::Idle => "idle",
                LedState::Listening => "listening",
                LedState::Speaking => "speaking",
                LedState::Muted => "muted",
            };
            format!("accept led {}", hex_encode(name.as_bytes()))
        }
        Control::Earcon { name } => format!("accept earcon {}", hex_encode(name.as_bytes())),
        Control::Play { url, format } => format!(
            "accept play {} {}",
            hex_encode(url.as_bytes()),
            hex_encode(format.as_bytes())
        ),
        // The firmware's control decoder knows led/earcon/play only. `stop` is tablet-only,
        // so an ESP board meeting it must refuse the record (and, in the real component,
        // drop the connection) rather than guess at it.
        Control::Stop => "reject".to_string(),
    }
}

/// What serde says about a control payload, expressed as the same verdict string.
fn serde_verdict(payload: &[u8]) -> String {
    match parse_control_json(payload) {
        Ok(control) => expected_verdict(&control),
        Err(_) => "reject".to_string(),
    }
}

// ---------------------------------------------------------------------------
// uplink: the cases the firmware harness emits
// ---------------------------------------------------------------------------

/// A 300-character name: the old fixed `char json[256]` body buffer refused this and the
/// component misreported the drop as "not connected or queue full".
const LONG_BUTTON_NAME: usize = 300;
/// A 600-character name: past the retuned firmware cap, so the refusal must be visible.
const OVERCAP_BUTTON_NAME: usize = 600;

fn pattern_name(len: usize) -> String {
    (0..len).map(|i| (b'a' + (i % 26) as u8) as char).collect()
}

/// `pcm[i] = (i * 7 + 3) & 0xFF`, the harness's deterministic pattern.
fn pattern_pcm() -> Vec<u8> {
    (0..2048).map(|i| ((i * 7 + 3) & 0xFF) as u8).collect()
}

/// The `pcm[i] = i` pattern of `docs/protocol.md` §9.1.
fn documented_pcm() -> Vec<u8> {
    (0..2048).map(|i| (i & 0xFF) as u8).collect()
}

fn samples(bytes: &[u8]) -> Vec<i16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| i16::from_le_bytes([pair[0], pair[1]]))
        .collect()
}

fn audio(seq: u32, pcm_bytes: &[u8]) -> AudioFrame {
    AudioFrame {
        seq,
        pcm: samples(pcm_bytes),
    }
}

fn hello(
    source_id: &str,
    room: &str,
    token: Option<&str>,
    has_speaker: bool,
    has_aec: bool,
    fw: &str,
) -> Hello {
    Hello {
        v: PROTOCOL_VERSION,
        source_id: source_id.to_string(),
        room: room.to_string(),
        token: token.map(str::to_string),
        caps: Caps {
            has_speaker,
            has_aec,
        },
        fw: fw.to_string(),
    }
}

/// The message a case represents, so both the byte comparison and the parse-back check use
/// one table (a case cannot be added to one direction only).
#[derive(Debug, Clone, PartialEq)]
enum Case {
    Hello(Hello),
    Audio(AudioFrame),
    Event(Event),
    /// An event name past the firmware's event-body cap: serde accepts it, the firmware
    /// refuses it and says so.
    RefusedEvent(usize),
}

fn emit_cases() -> Vec<(&'static str, Case)> {
    let pattern = pattern_pcm();
    let documented = documented_pcm();
    vec![
        // hello: token present, token null, a value needing escapes, caps on/off.
        (
            "hello_token",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("s3cr3t"),
                true,
                false,
                "0.1.0",
            )),
        ),
        (
            "hello_token_null",
            Case::Hello(hello(
                "study-pi",
                "study",
                None,
                false,
                false,
                "third-party/1.0",
            )),
        ),
        (
            "hello_token_escape",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("t\"k\\en\t\u{e9}"),
                true,
                true,
                "0.1.0",
            )),
        ),
        // JsonWriter's `< 0x20` branch must produce the same `\u00XX` serde_json does.
        (
            "hello_token_control",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("a\u{1}z\u{1f}"),
                true,
                false,
                "0.1.0",
            )),
        ),
        (
            "hello_caps_on",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("s3cr3t"),
                true,
                true,
                "0.1.0",
            )),
        ),
        (
            "hello_caps_off",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("s3cr3t"),
                false,
                false,
                "0.1.0",
            )),
        ),
        // The fourth capability combination: speaker, no AEC. The docs' defaults are
        // false/false and the other three combinations were already here, so a firmware that
        // hard-coded `has_aec = has_speaker` — or swapped the two fields — could still pass.
        (
            "hello_caps_ft",
            Case::Hello(hello(
                "kitchen",
                "kitchen",
                Some("s3cr3t"),
                false,
                true,
                "0.1.0",
            )),
        ),
        // Empty strings wherever the contract does not forbid one. docs/protocol.md §3 marks
        // `source_id`/`room` "required" but never says non-empty, and `fw` documents a default
        // of `""`, so an empty value is legal by the letter of the spec. This is shape
        // coverage: it asserts the two encoders produce the same bytes, not that empty is
        // meaningful. What would slip through without it: an encoder that dropped an empty
        // field, wrote `null` for it, or had an off-by-one in a length check. A present-but-
        // empty token is also distinct from an absent one for authentication (§4: a set server
        // token versus `""` is a mismatch, not a missing token), so the two shapes are separate
        // cases below rather than one.
        (
            "hello_token_empty",
            Case::Hello(hello("kitchen", "kitchen", Some(""), true, false, "0.1.0")),
        ),
        (
            "hello_empty_fields",
            Case::Hello(hello("", "", None, false, false, "")),
        ),
        // audio: seq 0, the u32 ceiling, and the documented pattern.
        ("audio_seq0", Case::Audio(audio(0, &pattern))),
        ("audio_seq_max", Case::Audio(audio(u32::MAX, &pattern))),
        ("audio_pattern_doc", Case::Audio(audio(7, &documented))),
        // events: all four variants, one needing escapes, one long name, one over-cap.
        ("ev_playback_done", Case::Event(Event::PlaybackDone)),
        ("ev_save_buffer", Case::Event(Event::SaveBuffer)),
        (
            "ev_button",
            Case::Event(Event::Button {
                name: "night_mode".to_string(),
            }),
        ),
        (
            "ev_button_escape",
            Case::Event(Event::Button {
                name: "mo\"de\\x\ty\u{1}".to_string(),
            }),
        ),
        // Shape coverage: an empty button name. docs/protocol.md §7 does not say a name must
        // be non-empty, so the only claim made here is that both encoders agree on it — which
        // is also where "omit the key" vs "empty value" bugs show up. Without this case, an
        // encoder that skipped an empty name (producing `{"type":"button"}`, which the backend
        // rejects with "missing field `name`") would look fine on every other case.
        (
            "ev_button_empty",
            Case::Event(Event::Button {
                name: String::new(),
            }),
        ),
        (
            "ev_button_long",
            Case::Event(Event::Button {
                name: pattern_name(LONG_BUTTON_NAME),
            }),
        ),
        ("ev_button_overcap", Case::RefusedEvent(OVERCAP_BUTTON_NAME)),
        // rssi: fixed-point text over several tenths, including 0.
        ("ev_rssi_0", Case::Event(Event::Rssi { dbm: 0.0 })),
        ("ev_rssi_neg5", Case::Event(Event::Rssi { dbm: -0.5 })),
        ("ev_rssi_neg625", Case::Event(Event::Rssi { dbm: -62.5 })),
        ("ev_rssi_neg1000", Case::Event(Event::Rssi { dbm: -100.0 })),
        ("ev_rssi_pos25", Case::Event(Event::Rssi { dbm: 2.5 })),
        // The .0/.5 values above are exactly the ones a fixed-point emitter and a float
        // formatter are most likely to agree on by luck. These are not: the firmware prints
        // `dbm_tenths` as `v/10 . v%10` with its own sign handling (luna_stream.h `tenths`),
        // while serde asks ryu for the shortest float text. -62.3 and -62.9 have a non-round
        // integer part; -0.1 and +0.1 are the two rails straddling zero, where `-v/10` vs
        // `-(v/10)` and a missing sign branch are most likely to differ from serde. A
        // disagreement here means the satellite reports a signal level the backend logs as a
        // different number, which no other case in this file would catch.
        ("ev_rssi_neg623", Case::Event(Event::Rssi { dbm: -62.3 })),
        ("ev_rssi_neg629", Case::Event(Event::Rssi { dbm: -62.9 })),
        ("ev_rssi_neg1", Case::Event(Event::Rssi { dbm: -0.1 })),
        ("ev_rssi_pos1", Case::Event(Event::Rssi { dbm: 0.1 })),
    ]
}

fn expected_emission(case: &Case) -> Emission {
    match case {
        Case::Hello(h) => Emission::Bytes(encode_hello_line(h)),
        Case::Audio(f) => Emission::Bytes(encode_audio_frame(f)),
        Case::Event(e) => Emission::Bytes(encode_event(e)),
        Case::RefusedEvent(_) => Emission::Refused,
    }
}

fn first_mismatch(got: &[u8], want: &[u8]) -> Option<usize> {
    got.iter()
        .zip(want.iter())
        .position(|(a, b)| a != b)
        .or_else(|| {
            if got.len() == want.len() {
                None
            } else {
                Some(got.len().min(want.len()))
            }
        })
}

fn assert_bytes_equal(id: &str, got: &[u8], want: &[u8]) {
    if let Some(at) = first_mismatch(got, want) {
        let context = |bytes: &[u8]| -> String {
            let start = at.saturating_sub(8);
            let end = (at + 9).min(bytes.len());
            String::from_utf8_lossy(&bytes[start..end]).into_owned()
        };
        panic!(
            "{id}: firmware and backend disagree at byte {at} (firmware {} bytes, backend {} bytes)\n  \
             firmware: {}\n  backend:  {}\n  firmware bytes: {}\n  backend bytes:  {}",
            got.len(),
            want.len(),
            context(got),
            context(want),
            hex_encode(got),
            hex_encode(want)
        );
    }
}

/// Run the byte comparison over every uplink case. Extracted from the `#[test]` so
/// `instrument_can_fail` can push a deliberately corrupted firmware payload through *this*
/// function — the same code path that produces the green — instead of poking
/// `first_mismatch`, which would only prove that helper can spot a difference.
/// Returns `(byte-compared, refused)` counts.
fn compare_uplink(emitted: &BTreeMap<String, Emission>) -> (usize, usize) {
    let cases = emit_cases();
    let expected_ids: BTreeSet<&str> = cases.iter().map(|(id, _)| *id).collect();
    let emitted_ids: BTreeSet<&str> = emitted.keys().map(String::as_str).collect();
    assert_eq!(
        expected_ids, emitted_ids,
        "the harness and this test must cover exactly the same uplink cases"
    );

    let mut bytes_cases = 0;
    let mut refused_cases = 0;
    for (id, case) in &cases {
        let got = emitted
            .get(*id)
            .unwrap_or_else(|| panic!("harness emitted no case {id}"));
        match (got, expected_emission(case)) {
            (Emission::Bytes(got), Emission::Bytes(want)) => {
                assert_bytes_equal(id, got, &want);
                bytes_cases += 1;
            }
            (Emission::Refused, Emission::Refused) => {
                refused_cases += 1;
            }
            (other, want) => {
                panic!("{id}: firmware {other:?} but the backend encoder produces {want:?}")
            }
        }
    }
    assert_eq!(
        bytes_cases + refused_cases,
        cases.len(),
        "every uplink case must be compared"
    );
    (bytes_cases, refused_cases)
}

/// Run `body`, returning `Err(text)` if it panicked, with the panic message instead of the
/// default hook's stderr print. An instrument that cannot fail is not an instrument, and
/// "it would fail" is only evidence when something actually made it fail.
fn captured_panic<R>(body: impl FnOnce() -> R) -> Result<R, String> {
    let captured: Arc<Mutex<Option<String>>> = Arc::new(Mutex::new(None));
    let slot = Arc::clone(&captured);
    let previous = panic::take_hook();
    panic::set_hook(Box::new(move |info| {
        let text = info
            .payload()
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                info.payload()
                    .downcast_ref::<&str>()
                    .map(|text| (*text).to_string())
            })
            .unwrap_or_else(|| "panic with a non-string payload".to_string());
        *slot.lock().expect("panic capture slot") = Some(text);
    }));
    let result = panic::catch_unwind(AssertUnwindSafe(body));
    panic::set_hook(previous);
    match result {
        Ok(value) => Ok(value),
        Err(_) => Err(captured
            .lock()
            .expect("panic capture slot")
            .clone()
            .unwrap_or_default()),
    }
}

#[test]
fn uplink_bytes_match_backend_encoders() {
    let emitted = run_emit();
    let (bytes_cases, refused_cases) = compare_uplink(&emitted);
    println!(
        "wire_contract uplink bytes: {bytes_cases} byte-identical cases, {refused_cases} visible refusal(s)"
    );
}

#[test]
fn uplink_messages_parse_backend_side() {
    let emitted = run_emit();
    let cases = emit_cases();

    let mut hellos = 0;
    let mut frames = 0;
    let mut events = 0;
    for (id, case) in &cases {
        let bytes = match emitted.get(*id).expect("harness case") {
            Emission::Bytes(bytes) => bytes,
            Emission::Refused => continue,
        };
        match case {
            Case::Hello(want) => {
                assert_eq!(
                    &decode_hello_line(bytes).expect("hello decodes"),
                    want,
                    "{id}"
                );
                hellos += 1;
            }
            Case::Audio(want) => {
                assert_eq!(
                    &parse_audio_frame(bytes).expect("audio record parses"),
                    want,
                    "{id}"
                );
                assert_eq!(bytes.len(), AUDIO_RECORD_LEN, "{id}: record length");
                frames += 1;
            }
            Case::Event(want) => {
                assert_eq!(&parse_event(bytes).expect("event parses"), want, "{id}");
                events += 1;
            }
            Case::RefusedEvent(len) => {
                // The backend accepts what the firmware's cap refuses: the refusal is the
                // firmware's documented limit, not a protocol rule.
                let overcap = Event::Button {
                    name: pattern_name(*len),
                };
                assert!(
                    parse_event_json(&serde_json::to_vec(&overcap).expect("event serializes"))
                        .is_ok(),
                    "{id}: the backend must be able to send a {len}-char button name"
                );
            }
        }
    }
    assert!(hellos >= 5, "expected at least 5 hello cases, saw {hellos}");
    assert!(frames >= 3, "expected at least 3 audio cases, saw {frames}");
    assert!(events >= 8, "expected at least 8 event cases, saw {events}");
    println!(
        "wire_contract uplink parse: {hellos} hello, {frames} audio, {events} event cases parsed back identically"
    );
}

// ---------------------------------------------------------------------------
// downlink: every control the backend can emit
// ---------------------------------------------------------------------------

fn play(url: &str, format: &str) -> Control {
    Control::Play {
        url: url.to_string(),
        format: format.to_string(),
    }
}

/// A realistic Home Assistant TTS-proxy URL (the shipped playback route).
const HA_TTS_URL: &str = "http://ha.example.lan:8123/api/tts_proxy/8f3c2b1a4d5e6f708192a3b4c5d6e7f8091a2b3c_en-us_1234abcd.flac";

/// The prefix `padded_url` builds on. Every length below is derived from it and asserted,
/// so a "511-byte url" case really is 511 bytes — the two comments this replaces said 344
/// and 500 for strings that were 340 and 496.
const URL_PREFIX: &str = "http://ha.example.lan:8123/";

/// The documented maximum accepted lengths, `docs/protocol.md` §8. They are the firmware's
/// fixed buffers minus the terminator (`name[128]`, `url[512]`, `format[16]`), and they are
/// *independent* of `MAX_CONTROL_JSON`: a control inside all three can still be refused by
/// the total. serde imposes none of them, so an over-cap control is a firmware-only refusal.
const CAP_EARCON_NAME: usize = 127;
const CAP_PLAY_URL: usize = 511;
const CAP_PLAY_FORMAT: usize = 15;
/// `MAX_CONTROL_JSON` (luna_stream.h): the whole payload, checked against the 3-byte length
/// prefix before anything is parsed.
const CAP_CONTROL_JSON: usize = 1024;

/// A URL of exactly `len` bytes. Panics rather than quietly producing a near-miss: at a
/// boundary, "close" is the same as absent.
fn padded_url(len: usize) -> String {
    assert!(len >= URL_PREFIX.len(), "{len} is below the URL prefix");
    let mut url = String::from(URL_PREFIX);
    url.push_str(&pattern_name(len - URL_PREFIX.len()));
    assert_eq!(url.len(), len, "padded_url({len})");
    url
}

/// A `format` value of exactly `len` ASCII bytes.
fn padded_format(len: usize) -> String {
    (0..len).map(|i| (b'a' + (i % 26) as u8) as char).collect()
}

fn downlink_cases() -> Vec<(&'static str, Control)> {
    vec![
        (
            "ctl_led_idle",
            Control::Led {
                state: LedState::Idle,
            },
        ),
        (
            "ctl_led_listening",
            Control::Led {
                state: LedState::Listening,
            },
        ),
        (
            "ctl_led_speaking",
            Control::Led {
                state: LedState::Speaking,
            },
        ),
        (
            "ctl_led_muted",
            Control::Led {
                state: LedState::Muted,
            },
        ),
        (
            "ctl_earcon_ack",
            Control::Earcon {
                name: "ack".to_string(),
            },
        ),
        (
            "ctl_earcon_non_ascii",
            Control::Earcon {
                name: "café-ack".to_string(),
            },
        ),
        // 60 chars: past the old `name[48]` cap, which silently dropped the control.
        (
            "ctl_earcon_long_name",
            Control::Earcon {
                name: pattern_name(60),
            },
        ),
        // 120 chars: still inside the retuned cap.
        (
            "ctl_earcon_120",
            Control::Earcon {
                name: pattern_name(120),
            },
        ),
        // Values the backend escapes in its JSON: the firmware must decode them back to
        // the same bytes, not to a literal backslash sequence.
        (
            "ctl_earcon_escaped",
            Control::Earcon {
                name: "mo\"de\\x\ty\u{1}".to_string(),
            },
        ),
        ("ctl_play_ha_tts", play(HA_TTS_URL, "flac")),
        // A non-ASCII path segment: serde emits it as raw UTF-8, not \u escapes.
        (
            "ctl_play_non_ascii_url",
            play(
                "http://ha.example.lan:8123/api/tts_proxy/café-en-us.flac",
                "flac",
            ),
        ),
        // 340 bytes: past the old `url[320]` cap.
        ("ctl_play_long_url", play(&padded_url(340), "flac")),
        // 496 bytes: inside the retuned cap.
        ("ctl_play_500_url", play(&padded_url(496), "flac")),
        // ---- documented field caps, at the limit (docs/protocol.md §8) ------------------
        // "Accepted at exactly the limit" and "refused one byte past" are the same claim
        // read twice; only the second one cannot be satisfied by a cap set one byte higher
        // or lower than documented. The limit itself must be accepted byte-for-byte — a
        // decoder that reserved room wrongly would cut the last character and still return
        // a valid-looking control.
        (
            "ctl_earcon_at_cap",
            Control::Earcon {
                name: pattern_name(CAP_EARCON_NAME),
            },
        ),
        (
            "ctl_play_url_at_cap",
            play(&padded_url(CAP_PLAY_URL), "flac"),
        ),
        (
            "ctl_play_format_at_cap",
            play(&padded_url(64), &padded_format(CAP_PLAY_FORMAT)),
        ),
        // ---- play.format is a string, not the literal "flac" ---------------------------
        // Every other play case here said "flac", so a decoder that only checked the field
        // existed — or hard-coded flac — passed all of them. These three say otherwise:
        // another legal value, one that needs an escape on the wire, and one that is not
        // ASCII (serde emits raw UTF-8, so the firmware must copy the bytes verbatim).
        ("ctl_play_format_mp3", play(&padded_url(64), "mp3")),
        (
            "ctl_play_format_escaped",
            play(&padded_url(64), "m\"p\\3\t\u{1}"),
        ),
        ("ctl_play_format_non_ascii", play(&padded_url(64), "flác-☃")),
        // ---- empty strings -------------------------------------------------------------
        // Shape coverage. docs/protocol.md is silent on whether `earcon.name`, `play.url`
        // or `play.format` may be empty (§8 gives byte counts and caps, never a minimum),
        // so no rule is invented here: both implementations must simply agree, and serde
        // accepts all three (they are plain `String` fields). Without these, a decoder that
        // treated an empty value as "absent" — returning the zero-initialised `Control`,
        // which for `play` is indistinguishable from a missing field — would still pass.
        // The last one has both fields empty at once, which also exercises the harness's
        // own delimiter handling (`accept play  ` with two empty hex fields).
        (
            "ctl_earcon_empty_name",
            Control::Earcon {
                name: String::new(),
            },
        ),
        ("ctl_play_empty_url", play("", "flac")),
        ("ctl_play_empty_format", play(&padded_url(64), "")),
        ("ctl_play_all_empty", play("", "")),
    ]
}

/// Controls past the firmware's fixed buffers, or of a type it does not implement: serde
/// accepts them, the firmware refuses them loudly. Asserted in both directions so the
/// asymmetry stays a documented cap.
fn downlink_capped_cases() -> Vec<(&'static str, Control)> {
    vec![
        (
            "ctl_cap_earcon_600",
            Control::Earcon {
                name: pattern_name(600),
            },
        ),
        (
            "ctl_cap_play_600_url",
            play(
                &format!("http://ha.example.lan:8123/{}", pattern_name(569)),
                "flac",
            ),
        ),
        // serde carries a NUL inside a String; a C string cannot, so the firmware refuses
        // it rather than cutting the name at the NUL (which would look like a shorter name).
        (
            "ctl_cap_nul_name",
            Control::Earcon {
                name: "a\0b".to_string(),
            },
        ),
        // ---- one byte past each documented cap -----------------------------------------
        // These are the boundary half of the pairs above: `ctl_earcon_at_cap` cannot tell a
        // 127-byte cap from a 200-byte one, these can. Each is a *firmware-only* refusal —
        // serde accepts the same payload (the fields are plain `String`s), which is exactly
        // the class `INTENTIONAL_DIVERGENCES` records at payload level; this table is its
        // record-level twin, so the reason belongs here too:
        //   name   — `Control::name` is `char[MAX_CONTROL_NAME]` (128 incl. terminator)
        //   url    — `Control::url` is `char[MAX_CONTROL_URL]` (512 incl. terminator)
        //   format — `Control::format` is `char[16]` (15 incl. terminator)
        // In every case the alternative to refusing is acting on a truncated string that
        // would look like success: a cut URL fetches the wrong audio, a cut earcon name
        // plays the wrong sound.
        (
            "ctl_cap_earcon_over",
            Control::Earcon {
                name: pattern_name(CAP_EARCON_NAME + 1),
            },
        ),
        (
            "ctl_cap_play_url_over",
            play(&padded_url(CAP_PLAY_URL + 1), "flac"),
        ),
        (
            "ctl_cap_play_format_over",
            play(&padded_url(64), &padded_format(CAP_PLAY_FORMAT + 1)),
        ),
        // ---- a type the firmware does not have ------------------------------------------
        // Not a buffer: `stop` is tablet-only, so the ESP decoder has no branch for it and
        // must refuse the record rather than act on a control it was never built to handle
        // (acting on a guess — treating it as `led idle`, say — would end a conversation
        // nobody asked to end). serde accepts it, because the backend does emit it: only to
        // a `playback="source"` tablet, never to a board. That asymmetry is the reason, and
        // it is the same one `INTENTIONAL_DIVERGENCES` records for this payload.
        ("ctl_cap_stop_tablet_only", Control::Stop),
    ]
}

#[test]
fn downlink_controls_are_acted_on() {
    let cases = downlink_cases();
    let records: Vec<(String, Vec<u8>)> = cases
        .iter()
        .map(|(id, control)| ((*id).to_string(), encode_control(control)))
        .collect();
    let verdicts = run_check(&records);

    for (id, control) in &cases {
        // The backend's own encoder must round-trip its payload, or the comparison below
        // would be measuring the wrong thing.
        let record = encode_control(control);
        let payload = &record[3..];
        assert_eq!(
            parse_control_json(payload).expect("backend control payload parses"),
            *control,
            "{id}: the backend's encoder and parser disagree"
        );
        let got = verdicts
            .get(*id)
            .unwrap_or_else(|| panic!("firmware answered nothing for {id}"));
        assert_eq!(
            got,
            &expected_verdict(control),
            "{id}: firmware must accept the control and report identical field values"
        );
    }

    let capped = downlink_capped_cases();
    let capped_records: Vec<(String, Vec<u8>)> = capped
        .iter()
        .map(|(id, control)| ((*id).to_string(), encode_control(control)))
        .collect();
    let capped_verdicts = run_check(&capped_records);
    for (id, control) in &capped {
        assert!(
            parse_control_json(&encode_control(control)[3..]).is_ok(),
            "{id}: the backend must be able to emit this control"
        );
        assert_eq!(
            capped_verdicts.get(*id).map(String::as_str),
            Some("reject"),
            "{id}: past the firmware's buffers the control must be refused, not truncated into \
             a different-but-valid control"
        );
    }
    println!(
        "wire_contract downlink: {} acted-on controls, {} documented caps refused",
        cases.len(),
        capped.len()
    );
}

// ---------------------------------------------------------------------------
// downlink, fed the way a socket feeds it
// ---------------------------------------------------------------------------

/// One fragmented-record case: the fragments to feed, the `feed` statuses that must follow,
/// and the controls that must come out. A table, not inline `assert`s, so
/// `case_coverage_is_complete` can see whether the family still exists.
struct SplitCase {
    id: &'static str,
    fragments: Vec<Vec<u8>>,
    statuses: Vec<&'static str>,
    controls: Vec<String>,
}

/// The record-splitting cases. Every one of these is the *normal* case on a real TCP
/// connection: `recv` returns what the kernel had, not what the writer wrote.
fn split_feed_cases() -> Vec<SplitCase> {
    let earcon = Control::Earcon {
        name: "ack".to_string(),
    };
    let muted = Control::Led {
        state: LedState::Muted,
    };
    let long_play = play(HA_TTS_URL, "flac");
    let earcon_rec = encode_control(&earcon);
    let muted_rec = encode_control(&muted);
    let play_rec = encode_control(&long_play);
    let earcon_v = expected_verdict(&earcon);
    let muted_v = expected_verdict(&muted);
    let play_v = expected_verdict(&long_play);
    let bytes_of = |rec: &[u8]| -> Vec<Vec<u8>> { rec.iter().map(|byte| vec![*byte]).collect() };
    let one_byte_statuses = |rec: &[u8]| -> Vec<&'static str> {
        (0..rec.len() - 1)
            .map(|_| "need_more")
            .chain(std::iter::once("pending"))
            .collect()
    };

    vec![
        // The 3-byte header split after the tag byte: the decoder is between `Tag` and
        // `LenLo` and must not treat the next byte as a tag.
        SplitCase {
            id: "split_after_tag",
            fragments: vec![earcon_rec[..1].to_vec(), earcon_rec[1..].to_vec()],
            statuses: vec!["need_more", "pending"],
            controls: vec![earcon_v.clone()],
        },
        // The length prefix itself split in half: the low byte is held over between calls,
        // and a decoder that re-read it as the high byte would size the payload wrongly.
        SplitCase {
            id: "split_mid_len",
            fragments: vec![earcon_rec[..2].to_vec(), earcon_rec[2..].to_vec()],
            statuses: vec!["need_more", "pending"],
            controls: vec![earcon_v.clone()],
        },
        // Payload split at a deliberately non-aligned offset (7 bytes into a 30-byte body).
        SplitCase {
            id: "split_payload_unaligned",
            fragments: vec![earcon_rec[..10].to_vec(), earcon_rec[10..].to_vec()],
            statuses: vec!["need_more", "pending"],
            controls: vec![earcon_v.clone()],
        },
        // A large real payload split twice, at an offset that is not a frame boundary.
        SplitCase {
            id: "split_play_uneven",
            fragments: vec![
                play_rec[..37].to_vec(),
                play_rec[37..110].to_vec(),
                play_rec[110..].to_vec(),
            ],
            statuses: vec!["need_more", "need_more", "pending"],
            controls: vec![play_v.clone()],
        },
        // Two back-to-back records split exactly between them: each fragment completes one
        // record, so each `feed` must report `pending` — never a NeedMore that swallows the
        // second record, and never two controls from one.
        SplitCase {
            id: "split_between_records",
            fragments: vec![earcon_rec.clone(), muted_rec.clone()],
            statuses: vec!["pending", "pending"],
            controls: vec![earcon_v.clone(), muted_v.clone()],
        },
        // Resume-after-partial, then a second record in the *same* fragment: after the first
        // control completes, the leftover bytes of that fragment belong to the next record
        // and must still be fed. This is the case where a caller that discarded everything
        // after `Pending` would lose a control silently.
        SplitCase {
            id: "resume_then_second",
            fragments: {
                let mut tail = earcon_rec[12..].to_vec();
                tail.extend_from_slice(&muted_rec);
                vec![earcon_rec[..12].to_vec(), tail]
            },
            statuses: vec!["need_more", "pending", "pending"],
            controls: vec![earcon_v.clone(), muted_v.clone()],
        },
        // The pathological end of the same spectrum: one byte per call. Nothing here is a
        // special case for the decoder, and an off-by-one in any of the four states shows up
        // as a hang or a wrong payload rather than a subtle mis-decode.
        SplitCase {
            id: "one_byte_at_a_time",
            fragments: bytes_of(&muted_rec),
            statuses: one_byte_statuses(&muted_rec),
            controls: vec![muted_v.clone()],
        },
    ]
}

#[test]
fn downlink_split_feeds_decode_incrementally() {
    let cases = split_feed_cases();
    let keyed: Vec<(String, Vec<Vec<u8>>)> = cases
        .iter()
        .map(|c| ((*c.id).to_string(), c.fragments.clone()))
        .collect();
    let runs = run_check_fragments(&keyed);

    let mut fragments_fed = 0usize;
    for case in &cases {
        let run = runs
            .get(case.id)
            .unwrap_or_else(|| panic!("{}: the fragment harness said nothing", case.id));
        let statuses: Vec<&str> = run.statuses.iter().map(String::as_str).collect();
        assert_eq!(
            statuses, case.statuses,
            "{}: the sequence of feed outcomes is the contract (statuses)",
            case.id
        );
        assert_eq!(
            run.controls, case.controls,
            "{}: every fragment of a control must decode to the same field values the \
             whole-record path reports",
            case.id
        );
        // Explicitly: a record completes exactly once. Two `pending`s for one record would
        // mean the decoder re-emitted it; zero would mean it was swallowed.
        let completed = run.statuses.iter().filter(|s| **s == "pending").count();
        assert_eq!(
            completed,
            case.controls.len(),
            "{}: one pending per decoded control",
            case.id
        );
        assert!(
            !run.statuses.iter().any(|s| s == "error"),
            "{}: a well-framed record must never hit a decoder error, got {:?}",
            case.id,
            run.controls
        );
        fragments_fed += case.fragments.len();
    }
    // A split-feed case that got silently flattened to one fragment would still pass every
    // assertion above, so the count of actual `feed` calls is pinned here too.
    assert!(
        fragments_fed >= 15,
        "expected the split cases to exercise many partial feeds, saw {fragments_fed}"
    );
    println!(
        "wire_contract split-feed: {} fragmented records, {} feed calls, every partial \
         reported need_more and each record decoded exactly once",
        cases.len(),
        fragments_fed
    );
}

/// A control record whose payload is padded with an unknown key to exactly `total` bytes.
/// Unknown fields are ignored by both sides (`docs/protocol.md` §3 "Unknown JSON fields are
/// ignored"), so padding can only move the *total* — which is the point.
fn padded_control_record(base: &[u8], total: usize) -> Vec<u8> {
    let payload = &base[3..];
    assert!(
        payload.last() == Some(&b'}'),
        "expected a JSON object to pad"
    );
    let mut padded = payload[..payload.len() - 1].to_vec();
    padded.extend_from_slice(b",\"pad\":\"");
    let filler = total.checked_sub(padded.len() + 2).unwrap_or_else(|| {
        panic!(
            "{base:?} cannot be padded to {total}: already {} bytes",
            base.len() - 3
        )
    });
    padded.extend(std::iter::repeat_n(b'p', filler));
    padded.extend_from_slice(b"\"}");
    assert_eq!(padded.len(), total, "padding produced the wrong length");
    encode_control_record(&padded)
}

/// One total-payload-cap case. A struct for the same reason `SplitCase` is one: the tuple
/// it replaces was five positional fields deep and clippy is right that names are clearer.
struct TotalCapCase {
    id: &'static str,
    /// The exact payload length this case claims to send.
    total: usize,
    records: Vec<Vec<u8>>,
    statuses: Vec<&'static str>,
    controls: Vec<String>,
}

/// The total-payload cap, at 1024 and one byte past, on a control whose every *field* is
/// also at its own documented cap. The field caps are individually satisfied, so the only
/// thing that can refuse this is `MAX_CONTROL_JSON`.
fn total_payload_cases() -> Vec<TotalCapCase> {
    let at_caps = play(&padded_url(CAP_PLAY_URL), &padded_format(CAP_PLAY_FORMAT));
    let record = encode_control(&at_caps);
    let verdict = expected_verdict(&at_caps);
    vec![
        TotalCapCase {
            id: "total_at_cap",
            total: CAP_CONTROL_JSON,
            records: vec![padded_control_record(&record, CAP_CONTROL_JSON)],
            statuses: vec!["pending"],
            controls: vec![verdict.clone()],
        },
        TotalCapCase {
            id: "total_one_past_cap",
            total: CAP_CONTROL_JSON + 1,
            records: vec![padded_control_record(&record, CAP_CONTROL_JSON + 1)],
            // `error`, and specifically PAYLOAD_TOO_LONG — not BAD_JSON. Naming the code is
            // what makes this a test of the total cap rather than of any other refusal.
            statuses: vec!["error"],
            controls: vec!["error 2".to_string()],
        },
    ]
}

#[test]
fn downlink_total_payload_cap_bites() {
    for case in total_payload_cases() {
        let id = case.id;
        let keyed: Vec<(String, Vec<Vec<u8>>)> = vec![(id.to_string(), case.records.clone())];
        // The framing itself must say what it claims, or "1024" is a claim about nothing.
        assert_eq!(
            case.records[0].len(),
            case.total + 3,
            "{id}: record is header + payload"
        );
        assert_eq!(
            u16::from_le_bytes([case.records[0][1], case.records[0][2]]) as usize,
            case.total,
            "{id}: the length prefix must name {} bytes",
            case.total
        );
        // serde imposes no total limit: the backend can and does emit this control. Without
        // this, a "refused" verdict could be a payload both sides reject.
        assert!(
            listenrr::proto::parse_control(&case.records[0]).is_ok(),
            "{id}: the backend must accept a {}-byte control payload",
            case.total
        );
        let run = run_check_fragments(&keyed)
            .remove(id)
            .unwrap_or_else(|| panic!("{id}: no answer"));
        let statuses: Vec<&str> = run.statuses.iter().map(String::as_str).collect();
        assert_eq!(statuses, case.statuses, "{id}: feed outcome");
        assert_eq!(run.controls, case.controls, "{id}: decoder verdict");
    }
    println!(
        "wire_contract total cap: {CAP_CONTROL_JSON} bytes accepted, refused with \
         PAYLOAD_TOO_LONG one byte past, every field cap satisfied"
    );
}

// ---------------------------------------------------------------------------
// the token-less hello, on both sides of the wire
// ---------------------------------------------------------------------------

#[test]
fn absent_key_hello_matches_the_firmware_shape() {
    // 1. Both documented spellings (§3.1) decode to the same value on the receiving side.
    //    The equivalence itself is pinned in `proto::tests::
    //    hello_token_present_but_null_decodes_as_absent` (src/proto.rs); this asserts the
    //    two *documented* payloads rather than the test's own literal, and that `token`
    //    lands as absent rather than as an empty secret.
    let absent = decode_hello_line(DOC_HELLO_ABSENT).expect("§3.1 key-absent hello decodes");
    let nulled = decode_hello_line(DOC_HELLO_NULL).expect("§3.1 token:null hello decodes");
    assert_eq!(
        absent, nulled,
        "the two documented spellings must mean the same hello"
    );
    assert_eq!(
        absent.token, None,
        "an absent token must not become Some(\"\") — §4 treats Some(\"\") as a token mismatch"
    );

    // 2. The absent-key hello's *content* crosses the firmware. The firmware has no hello
    //    parser — `build_hello` (luna_stream.h) is the only hello entry point it has, and
    //    hello is uplink-only, so there is no firmware-side decode to compare against. What
    //    can be compared, and is: feed the fields the absent-key form carries through the
    //    firmware's builder and require the backend to decode the result back to the very
    //    same Hello. If the builder turned an absent token into an empty string, or dropped
    //    a caps field, this is where it shows.
    let emitted = run_emit();
    let Emission::Bytes(line) = emitted
        .get("hello_token_null")
        .expect("the harness emits the token-less hello")
    else {
        panic!("hello_token_null was refused, not built");
    };
    assert_eq!(
        decode_hello_line(line).expect("the firmware's hello line decodes"),
        absent,
        "the bytes the firmware builds for the absent-key hello's fields must carry exactly \
         that hello"
    );

    // 3. The asymmetry, stated and pinned byte by byte. `build_hello` always writes the
    //    `"token":` key — with `nullptr` it writes `null` (luna_stream.h:239-244), and the
    //    component maps an unconfigured token to exactly that (luna_stream.cpp:394) — so the
    //    firmware *cannot* emit the shape §3.1 tells clients to send. docs/protocol.md
    //    prefers the key absent and calls `"token":null` "tolerated, not intended"; the
    //    two are nevertheless compatible because `Hello.token` is `Option<String>` with
    //    `#[serde(default)]`, so JSON `null` and an absent key both decode to `None`
    //    (`proto::tests::hello_token_present_but_null_decodes_as_absent`). That this is the
    //    *only* difference is asserted here rather than described: removing the key must
    //    leave §3.1's bytes exactly.
    let text = std::str::from_utf8(line).expect("hello is UTF-8");
    assert!(
        text.contains("\"token\":null,"),
        "the firmware emits the token key with a null value: {text}"
    );
    assert_eq!(
        text.replace("\"token\":null,", "").as_bytes(),
        [DOC_HELLO_ABSENT, b"\n"].concat(),
        "the token key is the only difference between what the firmware sends and what \
         docs/protocol.md §3.1 prefers"
    );

    // 4. Omitting `caps` entirely takes the documented defaults (§3 table: `{}` ⇒ both
    //    false). Shape coverage on the backend side only — `build_hello` always writes both
    //    caps, so the firmware can never produce this shape and there is nothing to
    //    differential here. Without it, a serde default that drifted to `true` would only
    //    be noticed by a client in production.
    let no_caps = br#"{"v":1,"source_id":"study-pi","room":"study","fw":"third-party/1.0"}"#;
    let decoded = decode_hello_line(no_caps).expect("caps may be omitted entirely");
    assert_eq!(
        decoded.caps,
        Caps {
            has_speaker: false,
            has_aec: false,
        },
        "an omitted caps object means both-false, per the §3 default column"
    );
    assert_eq!(
        decoded.token, None,
        "omitted caps and omitted token together"
    );
    assert_eq!(
        decoded, absent,
        "dropping caps changes nothing else about the hello"
    );
    println!(
        "wire_contract token-less hello: both spellings decode alike, the firmware's bytes \
         differ from §3.1 only by the token key, omitted caps default both-false"
    );
}

#[test]
fn negative_zero_rssi_is_a_documented_hazard() {
    // The firmware's `tenths` emitter formats `dbm_tenths` as an integer division and a
    // remainder, so 0 tenths prints `0.0` and it has no way to print a negative zero.
    // serde formats an `f64` with ryu, which keeps the sign: `-0.0` prints `-0.0`. The two
    // are therefore NOT byte-identical for that one value, and this is the one place in the
    // rssi path where they genuinely differ. It is unreachable from the firmware side — the
    // component computes an integer number of tenths, and `-0` and `0` are the same int —
    // so it is not an intentional divergence to excuse, and it is not a case to add to the
    // byte-comparison table (which would be red by construction). What it *is*, and what
    // this test keeps honest, is a boundary a future change must not cross unnoticed: if
    // serde ever stops emitting `-0.0`, or if the rssi value ever comes from a float
    // computation that can produce negative zero, this assertion says so here instead of
    // turning up as a byte mismatch in `uplink_bytes_match_backend_encoders`.
    let text = serde_json::to_string(&Event::Rssi { dbm: -0.0 }).expect("rssi serializes");
    assert_eq!(
        text, r#"{"type":"rssi","dbm":-0.0}"#,
        "serde's float text for negative zero changed; the firmware's fixed-point emitter \
         prints 0.0 for the same value, which is the one rssi case the byte comparison \
         cannot cover"
    );
    // The same value read back compares equal, which is exactly why the byte comparison is
    // the instrument here and a parse-back check would not be.
    let back = parse_event_json(text.as_bytes()).expect("negative zero parses");
    assert_eq!(
        back,
        Event::Rssi { dbm: 0.0 },
        "-0.0 parses back equal to 0.0, so only the bytes distinguish them"
    );
    println!(
        "wire_contract rssi: -0.0 is a known byte-level divergence, unreachable from the firmware"
    );
}

// ---------------------------------------------------------------------------
// adversarial: serde is the verdict, for both sides
// ---------------------------------------------------------------------------

/// Payloads where the two implementations used to disagree, plus the cases that already
/// agreed and must keep agreeing.
const ADVERSARIAL: &[(&str, &str)] = &[
    // The six measured divergences.
    (
        "div1_nested_earcon_name",
        r#"{"type":"earcon","meta":{"name":"ghost"}}"#,
    ),
    (
        "div2_nested_led_state",
        r#"{"type":"led","debug":{"state":"muted"},"state":"idle"}"#,
    ),
    (
        "div3_duplicate_state",
        r#"{"type":"led","state":"muted","state":"idle"}"#,
    ),
    // The duplicate rule is serde's per-field rule: every key the codec reads must reject
    // a repeat, while a repeat of a key the resolved variant does not read is ignored.
    (
        "dup_known_type",
        r#"{"type":"led","state":"idle","type":"earcon","name":"ack"}"#,
    ),
    (
        "dup_known_name",
        r#"{"type":"earcon","name":"a","name":"b"}"#,
    ),
    (
        "dup_known_format",
        r#"{"type":"play","url":"u","format":"a","format":"b"}"#,
    ),
    (
        "dup_unknown_key_on_variant",
        r#"{"type":"earcon","name":"ack","state":"idle","state":"idle"}"#,
    ),
    // Depth tracking through arrays, and strings whose contents look structural: keys
    // nested in an array element must not be mistaken for top-level keys, and braces or
    // commas inside a string must not move the depth counter.
    (
        "array_nested_key_ignored",
        r#"{"type":"led","x":[{"state":"muted"}],"state":"idle"}"#,
    ),
    (
        "structural_bytes_inside_array_strings",
        r#"{"x":["a,b","{"],"type":"earcon","name":"ack"}"#,
    ),
    // A non-string scalar where a string is required.
    ("state_not_string", r#"{"type":"led","state":1}"#),
    // An empty payload is a framing-valid record with nothing in it.
    ("empty_payload", ""),
    (
        "div4_two_objects",
        r#"{"type":"led","state":"idle"} {"type":"led","state":"muted"}"#,
    ),
    (
        "div5_escaped_non_ascii",
        r#"{"type":"earcon","name":"caf\u00e9"}"#,
    ),
    // \u escape semantics: a surrogate pair is one character (so this must ACCEPT with the
    // same bytes serde produces), while an unpaired surrogate is invalid in both.
    (
        "surrogate_pair_emoji",
        r#"{"type":"earcon","name":"\ud83d\ude00"}"#,
    ),
    (
        "lone_high_surrogate",
        r#"{"type":"earcon","name":"\ud83d"}"#,
    ),
    ("lone_low_surrogate", r#"{"type":"earcon","name":"\ude00"}"#),
    // Unescaped control bytes inside a string are not legal JSON: serde refuses them, so
    // this must refuse too (a raw NUL would otherwise truncate the name silently).
    (
        "raw_nul_in_string",
        "{\"type\":\"earcon\",\"name\":\"a\0b\"}",
    ),
    (
        "raw_tab_in_string",
        "{\"type\":\"earcon\",\"name\":\"a\tb\"}",
    ),
    // Canonical payloads.
    ("canon_led_idle", r#"{"type":"led","state":"idle"}"#),
    (
        "canon_led_listening",
        r#"{"type":"led","state":"listening"}"#,
    ),
    ("canon_led_speaking", r#"{"type":"led","state":"speaking"}"#),
    ("canon_led_muted", r#"{"type":"led","state":"muted"}"#),
    ("canon_earcon", r#"{"type":"earcon","name":"ack"}"#),
    (
        "canon_play",
        r#"{"type":"play","url":"http://ha.example.lan:8123/api/tts_proxy/abc","format":"flac"}"#,
    ),
    // Unknown / malformed / unusual shapes.
    ("unknown_type", r#"{"type":"bogus"}"#),
    ("bad_led_state", r#"{"type":"led","state":"blinking"}"#),
    ("missing_state", r#"{"type":"led"}"#),
    ("missing_name", r#"{"type":"earcon"}"#),
    ("missing_format", r#"{"type":"play","url":"u"}"#),
    ("truncated", r#"{"type":"led","state":"idle""#),
    ("leading_array", r#"[{"type":"led","state":"idle"}]"#),
    (
        "whitespace_padding",
        "  { \"type\" : \"led\" , \"state\" : \"idle\" }  ",
    ),
    ("type_not_string", r#"{"type":1}"#),
    (
        "type_inside_string_value",
        r#"{"type":"earcon","name":"\"type\":\"led\""}"#,
    ),
    (
        "type_key_after_value_string",
        r#"{"name":"x","type":"earcon"}"#,
    ),
    ("empty_object", "{}"),
    (
        "nested_state_object",
        r#"{"type":"led","state":{"nested":"idle"}}"#,
    ),
    ("unclosed_nested", r#"{"type":"led","state":"idle",{"a":1}"#),
    (
        "trailing_garbage",
        r#"{"type":"led","state":"idle"},"x":1}"#,
    ),
    ("raw_utf8_name", "{\"type\":\"earcon\",\"name\":\"café\"}"),
    (
        "escaped_quote_name",
        r#"{"type":"earcon","name":"a{}b\"c"}"#,
    ),
];

/// Payloads where the firmware is deliberately stricter than serde — `(id, payload, reason)`.
/// Each row asserts that serde ACCEPTS *and* that the firmware REFUSES, so the exception is
/// reviewable instead of invisible. Agreement (both refusing, or both accepting the same
/// bytes) belongs in `ADVERSARIAL` above, not here: an exception that should not be an
/// exception is how a real divergence hides.
const INTENTIONAL_DIVERGENCES: &[(&str, &str, &str)] = &[
    (
        "nul_escape_in_name",
        r#"{"type":"earcon","name":"a\u0000b"}"#,
        "Control.name is a NUL-terminated C string, so U+0000 cannot be represented; the only \
         alternative to refusing is playing \"a\" when the backend asked for \"a\\0b\", which is a \
         wrong earcon rather than no earcon",
    ),
    (
        "stop_type_tablet_only",
        r#"{"type":"stop"}"#,
        "stop ends a voice conversation and is sent only to a tablet (`playback=\"source\"`); the \
         ESP component has no playback to stop and no decoder branch for the type, so a board \
         refuses the record instead of inventing a meaning for it. The backend accepts it because \
         it must emit it — the route is gated on the source's playback mode, so a `playback=\"ha\"` \
         source never receives it",
    ),
];

#[test]
fn adversarial_controls_agree_with_serde() {
    let records: Vec<(String, Vec<u8>)> = ADVERSARIAL
        .iter()
        .map(|(id, payload)| ((*id).to_string(), encode_control_record(payload.as_bytes())))
        .collect();
    let verdicts = run_check(&records);

    let mut accepted = 0;
    let mut rejected = 0;
    for (id, payload) in ADVERSARIAL {
        let want = serde_verdict(payload.as_bytes());
        let got = verdicts
            .get(*id)
            .unwrap_or_else(|| panic!("firmware answered nothing for {id}"));
        assert_eq!(
            got, &want,
            "{id}: firmware and serde disagree on {payload}\n  firmware: {got}\n  serde:    {want}"
        );
        if want == "reject" {
            rejected += 1;
        } else {
            accepted += 1;
        }
    }
    println!(
        "wire_contract adversarial: {} payloads agree with serde ({accepted} accepted, {rejected} rejected)",
        ADVERSARIAL.len()
    );

    // The one place the firmware is deliberately stricter. Asserted in both directions, with the
    // reason carried in the table, so a reviewer can tell "conforms to serde" from "refuses on
    // purpose" — the classification is the point, not the pass.
    let records: Vec<(String, Vec<u8>)> = INTENTIONAL_DIVERGENCES
        .iter()
        .map(|(id, payload, _)| ((*id).to_string(), encode_control_record(payload.as_bytes())))
        .collect();
    let verdicts = run_check(&records);
    for (id, payload, reason) in INTENTIONAL_DIVERGENCES {
        assert!(
            !reason.is_empty(),
            "{id}: an intentional divergence must say why it exists"
        );
        // Acceptance, asked of serde directly — not `serde_verdict`, which routes an accepted
        // payload through `expected_verdict` and therefore reports `reject` for any variant the
        // firmware has no branch for at all (`stop`). Using the string there would call that
        // row agreement and send it to `ADVERSARIAL`, where it would fail: the two sides do
        // not agree on it, they differ on purpose.
        assert!(
            parse_control_json(payload.as_bytes()).is_ok(),
            "{id}: serde must ACCEPT this payload, or it is agreement and belongs in \
             ADVERSARIAL, not in the exception table ({reason})"
        );
        assert_eq!(
            verdicts.get(*id).map(String::as_str),
            Some("reject"),
            "{id}: the firmware must refuse on purpose — {reason}"
        );
    }
    println!(
        "wire_contract intentional divergences: {} firmware-stricter payload(s), each with a reason",
        INTENTIONAL_DIVERGENCES.len()
    );

    // Record level, not payload level: `len` delimits the message, so bytes after a
    // complete control record belong to the next record and must not change this verdict.
    // serde's parse_control ignores them; the decoder must too, or a chunk carrying 1½
    // records would look like a rejected control.
    let led = Control::Led {
        state: LedState::Idle,
    };
    let mut with_trailer = encode_control(&led);
    with_trailer.extend_from_slice(&[0x00, 0x00, 0xFF]);
    assert!(
        listenrr::proto::parse_control(&with_trailer).is_ok(),
        "serde must accept a control record followed by the next record's first bytes"
    );
    let verdicts = run_check(&[("record_trailer".to_string(), with_trailer)]);
    let want = expected_verdict(&led);
    assert_eq!(
        verdicts.get("record_trailer").map(String::as_str),
        Some(want.as_str()),
        "the firmware must act on the control and not fail on the following record's bytes"
    );

    // A record whose `len` says 0 while payload bytes follow: serde parses the empty
    // payload and fails, and the decoder must refuse rather than reading past the record.
    let mut zero_len = vec![0x03, 0x00, 0x00];
    zero_len.extend_from_slice(br#"{"type":"led","state":"idle"}"#);
    assert!(
        listenrr::proto::parse_control(&zero_len).is_err(),
        "serde must reject a zero-length control payload"
    );
    let verdicts = run_check(&[("record_zero_len".to_string(), zero_len)]);
    assert_eq!(
        verdicts.get("record_zero_len").map(String::as_str),
        Some("reject"),
        "a zero-length control record must be refused, not filled from the next record"
    );
}

/// `0x03` + `u16` LE payload length + payload, the way the backend frames a control.
fn encode_control_record(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(3 + payload.len());
    out.push(0x03);
    out.extend_from_slice(&(payload.len() as u16).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

// ---------------------------------------------------------------------------
// the byte counts docs/protocol.md asks a reader to trust
// ---------------------------------------------------------------------------

const DOC_HELLO_ABSENT: &[u8] = br#"{"v":1,"source_id":"study-pi","room":"study","caps":{"has_speaker":false,"has_aec":false},"fw":"third-party/1.0"}"#;
const DOC_HELLO_NULL: &[u8] = br#"{"v":1,"source_id":"study-pi","room":"study","token":null,"caps":{"has_speaker":false,"has_aec":false},"fw":"third-party/1.0"}"#;
const DOC_HELLO_S3CR3T: &[u8] = br#"{"v":1,"source_id":"kitchen","room":"kitchen","token":"s3cr3t","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}"#;
const DOC_HELLO_PLACEHOLDER: &[u8] = br#"{"v":1,"source_id":"kitchen","room":"kitchen","token":"<shared secret>","caps":{"has_speaker":true,"has_aec":false},"fw":"0.1.0"}"#;

const DOC_EV_PLAYBACK_DONE: &[u8] = br#"{"type":"playback_done"}"#;
const DOC_EV_RSSI: &[u8] = br#"{"type":"rssi","dbm":-62.5}"#;
const DOC_EV_BUTTON: &[u8] = br#"{"type":"button","name":"night_mode"}"#;
const DOC_EV_SAVE_BUFFER: &[u8] = br#"{"type":"save_buffer"}"#;

const DOC_CTL_LED_IDLE: &[u8] = br#"{"type":"led","state":"idle"}"#;
const DOC_CTL_LED_LISTENING: &[u8] = br#"{"type":"led","state":"listening"}"#;
const DOC_CTL_LED_SPEAKING: &[u8] = br#"{"type":"led","state":"speaking"}"#;
const DOC_CTL_LED_MUTED: &[u8] = br#"{"type":"led","state":"muted"}"#;
const DOC_CTL_EARCON: &[u8] = br#"{"type":"earcon","name":"ack"}"#;
const DOC_CTL_PLAY: &[u8] =
    br#"{"type":"play","url":"http://ha.example.lan:8123/api/tts_proxy/abc","format":"flac"}"#;

#[test]
fn documented_byte_counts_hold() {
    // §3.1: key absent 113 JSON / 114 on the wire, token:null 126 / 127.
    assert_eq!(DOC_HELLO_ABSENT.len(), 113, "§3.1 key-absent hello");
    assert_eq!(
        DOC_HELLO_ABSENT.len() + 1,
        114,
        "§3.1 key-absent hello on the wire"
    );
    assert_eq!(DOC_HELLO_NULL.len(), 126, "§3.1 token:null hello");
    assert_eq!(
        DOC_HELLO_NULL.len() + 1,
        127,
        "§3.1 token:null hello on the wire"
    );
    assert_eq!(
        encode_hello_line(&hello(
            "study-pi",
            "study",
            None,
            false,
            false,
            "third-party/1.0"
        )),
        [DOC_HELLO_NULL, b"\n"].concat(),
        "the backend's encoder emits the token:null spelling the doc counts"
    );

    // §9.4: a literal secret is 120 JSON / 121 on the wire.
    assert_eq!(DOC_HELLO_S3CR3T.len(), 120, "§9.4 hello");
    assert_eq!(
        encode_hello_line(&hello(
            "kitchen",
            "kitchen",
            Some("s3cr3t"),
            true,
            false,
            "0.1.0"
        )),
        [DOC_HELLO_S3CR3T, b"\n"].concat(),
        "§9.4 hello bytes"
    );

    // §3/§9.4: the placeholder example is 129 JSON / 130 on the wire (17-char token).
    assert_eq!(DOC_HELLO_PLACEHOLDER.len(), 129, "§3 placeholder hello");
    assert_eq!(
        DOC_HELLO_PLACEHOLDER.len() + 1,
        130,
        "§3 placeholder hello on the wire"
    );
    assert!(
        decode_hello_line(DOC_HELLO_PLACEHOLDER).is_ok(),
        "§3 placeholder hello must decode"
    );
    assert!(
        decode_hello_line(DOC_HELLO_ABSENT).is_ok(),
        "§3.1 key-absent hello must decode"
    );

    // §7 event table: payload bytes / record bytes.
    let events: [(&str, &[u8], Event, usize, usize); 4] = [
        (
            "playback_done",
            DOC_EV_PLAYBACK_DONE,
            Event::PlaybackDone,
            24,
            27,
        ),
        ("rssi", DOC_EV_RSSI, Event::Rssi { dbm: -62.5 }, 27, 30),
        (
            "button",
            DOC_EV_BUTTON,
            Event::Button {
                name: "night_mode".to_string(),
            },
            37,
            40,
        ),
        ("save_buffer", DOC_EV_SAVE_BUFFER, Event::SaveBuffer, 22, 25),
    ];
    for (name, literal, event, payload_len, record_len) in events {
        assert_eq!(literal.len(), payload_len, "§7 {name} payload bytes");
        let encoded = encode_event(&event);
        assert_eq!(encoded.len(), record_len, "§7 {name} record bytes");
        assert_eq!(&encoded[3..], literal, "§7 {name} payload bytes");
        assert_eq!(
            u16::from_le_bytes([encoded[1], encoded[2]]) as usize,
            payload_len,
            "§7 {name} len field"
        );
    }

    // §8 control table: payload bytes / record bytes.
    let controls: [(&str, &[u8], Control, usize, usize); 6] = [
        (
            "led idle",
            DOC_CTL_LED_IDLE,
            Control::Led {
                state: LedState::Idle,
            },
            29,
            32,
        ),
        (
            "led listening",
            DOC_CTL_LED_LISTENING,
            Control::Led {
                state: LedState::Listening,
            },
            34,
            37,
        ),
        (
            "led speaking",
            DOC_CTL_LED_SPEAKING,
            Control::Led {
                state: LedState::Speaking,
            },
            33,
            36,
        ),
        (
            "led muted",
            DOC_CTL_LED_MUTED,
            Control::Led {
                state: LedState::Muted,
            },
            30,
            33,
        ),
        (
            "earcon",
            DOC_CTL_EARCON,
            Control::Earcon {
                name: "ack".to_string(),
            },
            30,
            33,
        ),
        (
            "play",
            DOC_CTL_PLAY,
            play("http://ha.example.lan:8123/api/tts_proxy/abc", "flac"),
            84,
            87,
        ),
    ];
    for (name, literal, control, payload_len, record_len) in controls {
        assert_eq!(literal.len(), payload_len, "§8 {name} payload bytes");
        let encoded = encode_control(&control);
        assert_eq!(encoded.len(), record_len, "§8 {name} record bytes");
        assert_eq!(&encoded[3..], literal, "§8 {name} payload bytes");
        assert_eq!(
            u16::from_le_bytes([encoded[1], encoded[2]]) as usize,
            payload_len,
            "§8 {name} len field"
        );
    }
    println!("wire_contract docs: 4 hello spellings and 10 event/control byte rows pinned");
}

// ---------------------------------------------------------------------------
// the instrument must be able to fail
// ---------------------------------------------------------------------------

#[test]
fn instrument_can_fail() {
    // 1. A corrupted expected byte must be a mismatch — the byte comparison is not vacuous.
    let emitted = run_emit();
    let (id, bytes) = emitted
        .iter()
        .find_map(|(id, emission)| match emission {
            Emission::Bytes(bytes) if id.starts_with("ev_") => Some((id.clone(), bytes.clone())),
            _ => None,
        })
        .expect("at least one emitted event record");
    assert_eq!(
        first_mismatch(&bytes, &bytes),
        None,
        "{id}: identical bytes must match"
    );
    let mut corrupted = bytes.clone();
    corrupted[0] ^= 0xFF;
    assert_eq!(
        first_mismatch(&corrupted, &bytes),
        Some(0),
        "{id}: a corrupted tag byte must be reported as a mismatch"
    );
    let mut shortened = bytes.clone();
    shortened.pop();
    assert_eq!(
        first_mismatch(&shortened, &bytes),
        Some(bytes.len() - 1),
        "{id}: a shortened record must be reported as a mismatch"
    );

    // 1b. The above says `first_mismatch` can spot a difference. It does not say the
    // comparison the green run depends on would notice one — an instrument can be correct
    // and still never be called. So corrupt a real firmware payload *inside the harness's
    // own output* and drive it through `compare_uplink`, the function
    // `uplink_bytes_match_backend_encoders` runs. It must fail, and fail naming the case.
    let mut corrupted_run = emitted.clone();
    let target = corrupted_run
        .iter_mut()
        .find(|(id, emission)| id.starts_with("ev_") && matches!(emission, Emission::Bytes(_)))
        .map(|(id, _)| id.clone())
        .expect("at least one emitted event record");
    match corrupted_run
        .get_mut(&target)
        .expect("the case is still there")
    {
        // Not byte 0: the tag is the one position a sloppy comparison could special-case.
        // Flip a byte inside the JSON body instead.
        Emission::Bytes(payload) => {
            assert!(
                payload.len() > 8,
                "{target}: too short to corrupt meaningfully"
            );
            payload[7] ^= 0xFF;
        }
        Emission::Refused => unreachable!("filtered for Bytes"),
    }
    let outcome = captured_panic(|| compare_uplink(&corrupted_run));
    let message = match outcome {
        Ok((compared, refused)) => panic!(
            "the byte comparison accepted a corrupted firmware payload for {target} \
             ({compared} compared, {refused} refused) — the green is not measuring anything"
        ),
        Err(message) => message,
    };
    assert!(
        message.contains(&target) && message.contains("firmware and backend disagree"),
        "the failure must name the case and the disagreement, got: {message}"
    );
    // And the same class of corruption through the low-level entry point, so the byte
    // offset in the message is checked rather than trusted.
    let offset = captured_panic(|| assert_bytes_equal("probe", &bytes[..bytes.len() - 1], &bytes))
        .expect_err("a truncated payload must panic");
    assert!(
        offset.contains("disagree at byte"),
        "the message must name the offset, got: {offset}"
    );
    let first_line = message.lines().next().unwrap_or(&message);
    println!("wire_contract instrument: a corrupted firmware payload reddened the real comparison: {first_line}");

    // 2. A truncated control record is rejected by BOTH sides.
    let good = encode_control(&Control::Led {
        state: LedState::Listening,
    });
    let truncated = good[..good.len() - 1].to_vec();
    assert!(
        listenrr::proto::parse_control(&truncated).is_err(),
        "serde must reject a truncated control record"
    );
    let verdicts = run_check(&[("truncated".to_string(), truncated)]);
    assert_eq!(
        verdicts.get("truncated").map(String::as_str),
        Some("reject")
    );

    // 3. A tampered tag byte is rejected by BOTH sides.
    let mut tampered = good.clone();
    tampered[0] = 0x02;
    assert!(
        listenrr::proto::parse_control(&tampered).is_err(),
        "serde must reject a control record with an event tag"
    );
    let verdicts = run_check(&[("tampered".to_string(), tampered)]);
    assert_eq!(verdicts.get("tampered").map(String::as_str), Some("reject"));

    // 4. A payload whose length field overruns the buffer is rejected by both sides.
    let mut overrun = good.clone();
    overrun[1] = overrun[1].wrapping_add(4);
    assert!(
        listenrr::proto::parse_control(&overrun).is_err(),
        "serde must reject a control whose len field overruns the record"
    );
    let verdicts = run_check(&[("overrun".to_string(), overrun)]);
    assert_eq!(verdicts.get("overrun").map(String::as_str), Some("reject"));

    println!("wire_contract instrument: byte comparison, truncation, tag tamper and len overrun all bite");

    // 5. Malformed *input* must never be reported as a firmware verdict. The harness answers
    // ERR, which this file's parser refuses, so a hex-encoding bug on the Rust side cannot
    // hide behind an expected "reject".
    let probe = unwrap_bounded(run_bounded(
        Command::new(harness()).arg("check"),
        Some(b"DOWN bad_hex 0x33\n"),
        false,
        RUN_TIMEOUT,
        "HARNESS TIMEOUT: check bad_hex",
    ));
    let stdout = String::from_utf8_lossy(&probe.stdout);
    assert!(
        stdout.starts_with("ERR bad_hex"),
        "malformed hex must be an error line, not a firmware verdict: {stdout}"
    );

    // 6. The watchdog bites. `check` reads stdin to EOF, so a caller that keeps the write half
    // open makes the harness wait forever — the hang is the caller's, and the bound must expire
    // as a NAMED failure instead of parking the suite.
    let named = run_bounded(
        Command::new(harness()).arg("check"),
        Some(b"DOWN stuck 030000\n"),
        true, // hold the write half open: this reproduces the caller-created hang
        WATCHDOG_TIMEOUT,
        "HARNESS TIMEOUT: check stuck",
    )
    .expect_err("the deadline must expire when the harness is held open");
    assert_eq!(
        named.to_string(),
        format!(
            "HARNESS TIMEOUT: check stuck after {} ms",
            WATCHDOG_TIMEOUT.as_millis()
        ),
        "an expired deadline must report its label and its bound"
    );
    println!("{named}");

    // 7. The kill is on the process *group*, so nothing is left holding the pipes.
    let mut orphan = Command::new(harness())
        .arg("check")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .process_group(0)
        .spawn()
        .expect("spawn a held-open check");
    let pid = orphan.id();
    assert!(
        wait_bounded(&mut orphan, WATCHDOG_TIMEOUT).is_none(),
        "a check with stdin held open must not exit on its own"
    );
    kill_group(&mut orphan);
    let group_alive = Command::new("kill")
        .args(["-0", "--", &format!("-{pid}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false);
    assert!(
        !group_alive,
        "process group {pid} must be gone after a timeout kill, not merely the parent"
    );

    // 8. The verdict tables must have rows in them. Every table-driven test in this file is
    // written as "for each row, assert agreement", so a table emptied to `&[]` — or narrowed
    // to one convenient row — turns those tests green while comparing nothing. Nothing else
    // in the file can see that happening, so it is checked here, where "the instrument can
    // fail" is the subject.
    assert!(
        !ADVERSARIAL.is_empty(),
        "the adversarial table is empty: the differential has no payloads to compare"
    );
    let accepted = ADVERSARIAL
        .iter()
        .filter(|(_, payload)| serde_verdict(payload.as_bytes()) != "reject")
        .count();
    let rejected = ADVERSARIAL.len() - accepted;
    assert!(
        accepted > 0 && rejected > 0,
        "the adversarial table must contain both accepted and refused payloads \
         ({accepted} accepted, {rejected} refused); a table of only one kind proves only that \
         one branch is reachable"
    );
    assert!(
        !INTENTIONAL_DIVERGENCES.is_empty(),
        "the intentional-divergence table is empty: either the firmware is no longer stricter \
         anywhere (so the reasons below are stale) or the rows were dropped"
    );
    for (id, _, reason) in INTENTIONAL_DIVERGENCES {
        assert!(
            !reason.trim().is_empty(),
            "{id}: an exception without a reason is a bug with a label"
        );
    }
    // The remaining tables, so a silently emptied one cannot pass as a smaller suite.
    assert!(
        !downlink_cases().is_empty() && !downlink_capped_cases().is_empty(),
        "the downlink tables are empty"
    );
    assert!(
        !split_feed_cases().is_empty() && !total_payload_cases().is_empty(),
        "the fragmented-feed / total-cap tables are empty"
    );
    println!(
        "wire_contract tables: {} adversarial payloads ({accepted} accepted, {rejected} refused), \
         {} intentional divergence(s), {} downlink controls, {} refused controls, {} split-feed \
         and {} total-cap cases",
        ADVERSARIAL.len(),
        INTENTIONAL_DIVERGENCES.len(),
        downlink_cases().len(),
        downlink_capped_cases().len(),
        split_feed_cases().len(),
        total_payload_cases().len()
    );
}

// ---------------------------------------------------------------------------
// coverage: no variant can silently drop out of the comparison
// ---------------------------------------------------------------------------

#[test]
fn case_coverage_is_complete() {
    let cases = emit_cases();
    assert!(
        cases.len() >= 18,
        "expected at least 18 uplink cases, saw {}",
        cases.len()
    );

    let mut events = BTreeSet::new();
    for (_, case) in &cases {
        match case {
            Case::Event(Event::PlaybackDone) => {
                events.insert("playback_done");
            }
            Case::Event(Event::Rssi { .. }) => {
                events.insert("rssi");
            }
            Case::Event(Event::Button { .. }) | Case::RefusedEvent(_) => {
                events.insert("button");
            }
            Case::Event(Event::SaveBuffer) => {
                events.insert("save_buffer");
            }
            _ => {}
        }
    }
    assert_eq!(
        events.len(),
        4,
        "every Event variant must be compared, saw {events:?}"
    );

    let mut controls = BTreeSet::new();
    let mut led_states = BTreeSet::new();
    for (_, control) in downlink_cases() {
        match control {
            Control::Led { state } => {
                controls.insert("led");
                led_states.insert(format!("{state:?}"));
            }
            Control::Earcon { .. } => {
                controls.insert("earcon");
            }
            Control::Play { .. } => {
                controls.insert("play");
            }
            // `stop` is compared in the refused table, never here: a control the firmware
            // cannot act on is not an acted-on case. Reaching this arm means someone put it
            // into `downlink_cases()`, which would assert the board accepts a control it has
            // no decoder for.
            Control::Stop => unreachable!(
                "stop is tablet-only and belongs in downlink_capped_cases(), not downlink_cases()"
            ),
        }
    }
    assert_eq!(
        controls.len(),
        3,
        "every Control variant must be compared, saw {controls:?}"
    );
    assert_eq!(
        led_states.len(),
        4,
        "every LedState must be compared, saw {led_states:?}"
    );

    // The checks above count *distinct* names, so they are satisfied by one case each and
    // are satisfied forever: deleting every hello case, or every audio case, or nine of the
    // ten rssi cases, leaves all of them green. This is the part that actually polices.
    // Every family below must have a non-zero count, by name, or the test says which one
    // emptied. A suite can shrink quietly; a family cannot.
    let coverage = measure_coverage();
    let mut missing: Vec<&str> = Vec::new();
    for family in REQUIRED_FAMILIES {
        if coverage.counts.get(*family).copied().unwrap_or(0) == 0 {
            missing.push(family);
        }
    }
    assert!(
        missing.is_empty(),
        "these case families have no cases left: {missing:?}\n\
         a differential over an empty family is a pass that verifies nothing; measured: {:?}",
        coverage.counts
    );
    println!(
        "wire_contract coverage: {} uplink cases, 4 events, 3 controls, 4 led states, {} \
         families all non-zero: {:?}",
        cases.len(),
        REQUIRED_FAMILIES.len(),
        coverage.counts
    );
}

/// Every family the comparison must keep covering. Names are `direction.kind[.detail]`, and
/// each one is a thing a real implementation can get wrong on its own — which is the test
/// for whether a family deserves to be named.
const REQUIRED_FAMILIES: &[&str] = &[
    // Whole message kinds in both directions. Without these the file could silently compare
    // only controls, or only audio.
    "uplink.hello",
    "uplink.audio",
    "uplink.event",
    "uplink.refused_over_cap",
    "downlink.control.acted_on",
    "downlink.control.refused",
    "downlink.fragment_feed",
    "downlink.total_payload_cap",
    // Every event variant: each has its own builder and its own payload shape.
    "uplink.event.playback_done",
    "uplink.event.save_buffer",
    "uplink.event.rssi",
    "uplink.event.button",
    // Hello is where capabilities live; all four combinations are separate firmware states,
    // and (false, true) is the one no default and no other case implies.
    "uplink.hello.caps.true_true",
    "uplink.hello.caps.true_false",
    "uplink.hello.caps.false_true",
    "uplink.hello.caps.false_false",
    // The rssi emitter's failure modes: a whole tenth and both rails around zero.
    "uplink.event.rssi.tenths",
    "uplink.event.rssi.near_zero_below",
    "uplink.event.rssi.near_zero_above",
    // Every control variant and every LED state, since each is a separate decoder branch.
    "downlink.control.led",
    "downlink.control.earcon",
    "downlink.control.play",
    "downlink.led_state.idle",
    "downlink.led_state.listening",
    "downlink.led_state.speaking",
    "downlink.led_state.muted",
    // Documented field caps, both sides of each boundary. An at-limit case alone cannot
    // tell 127 from 200; a one-past case alone cannot tell 127 from 128-refusing.
    "downlink.field_cap.name_at_limit",
    "downlink.field_cap.name_one_past",
    "downlink.field_cap.url_at_limit",
    "downlink.field_cap.url_one_past",
    "downlink.field_cap.format_at_limit",
    "downlink.field_cap.format_one_past",
    // A control type the firmware does not implement at all (`stop`, tablet-only). Named
    // separately from the field caps because the refusal is for the type, not its size.
    "downlink.control.refused_type.stop",
    // `play.format` is a string, not a constant, and `play.url` is not always ASCII.
    "downlink.play.format.not_flac",
    "downlink.play.format.escaped",
    "downlink.play.format.non_ascii",
    // Empty values, in both directions: the shape where "absent" and "empty" get confused.
    "uplink.empty_value",
    "downlink.empty_value",
    // The record-splitting paths, each a different state of the incremental decoder.
    "downlink.fragment.header",
    "downlink.fragment.length_prefix",
    "downlink.fragment.payload",
    "downlink.fragment.record_boundary",
    "downlink.fragment.resume_then_second",
    "downlink.fragment.one_byte_at_a_time",
    // The verdict tables themselves.
    "verdict.adversarial",
    "verdict.intentional_divergence",
];

/// What the current tables actually cover, counted by family.
#[derive(Debug, Default)]
struct Coverage {
    counts: BTreeMap<&'static str, usize>,
}

impl Coverage {
    fn hit(&mut self, family: &'static str) {
        *self.counts.entry(family).or_insert(0) += 1;
    }
}

fn measure_coverage() -> Coverage {
    let mut coverage = Coverage::default();

    for (_, case) in emit_cases() {
        match &case {
            Case::Hello(hello) => {
                coverage.hit("uplink.hello");
                coverage.hit(match (hello.caps.has_speaker, hello.caps.has_aec) {
                    (true, true) => "uplink.hello.caps.true_true",
                    (true, false) => "uplink.hello.caps.true_false",
                    (false, true) => "uplink.hello.caps.false_true",
                    (false, false) => "uplink.hello.caps.false_false",
                });
                if hello.source_id.is_empty()
                    || hello.room.is_empty()
                    || hello.fw.is_empty()
                    || hello.token.as_deref() == Some("")
                {
                    coverage.hit("uplink.empty_value");
                }
            }
            Case::Audio(_) => coverage.hit("uplink.audio"),
            Case::Event(_) => coverage.hit("uplink.event"),
            Case::RefusedEvent(_) => coverage.hit("uplink.refused_over_cap"),
        }
        match &case {
            Case::Event(Event::PlaybackDone) => coverage.hit("uplink.event.playback_done"),
            Case::Event(Event::SaveBuffer) => coverage.hit("uplink.event.save_buffer"),
            Case::Event(Event::Button { name }) => {
                coverage.hit("uplink.event.button");
                if name.is_empty() {
                    coverage.hit("uplink.empty_value");
                }
            }
            Case::Event(Event::Rssi { dbm }) => {
                coverage.hit("uplink.event.rssi");
                // In tenths, because that is the unit the firmware emits in.
                let tenths = (dbm * 10.0).round() as i64;
                if tenths % 10 != 0 {
                    coverage.hit("uplink.event.rssi.tenths");
                }
                if tenths < 0 && tenths > -10 {
                    coverage.hit("uplink.event.rssi.near_zero_below");
                }
                if tenths > 0 && tenths < 10 {
                    coverage.hit("uplink.event.rssi.near_zero_above");
                }
            }
            _ => {}
        }
    }

    for (_, control) in downlink_cases() {
        coverage.hit("downlink.control.acted_on");
        match &control {
            Control::Led { state } => {
                coverage.hit("downlink.control.led");
                coverage.hit(match state {
                    LedState::Idle => "downlink.led_state.idle",
                    LedState::Listening => "downlink.led_state.listening",
                    LedState::Speaking => "downlink.led_state.speaking",
                    LedState::Muted => "downlink.led_state.muted",
                });
            }
            Control::Earcon { name } => {
                coverage.hit("downlink.control.earcon");
                if name.is_empty() {
                    coverage.hit("downlink.empty_value");
                }
                if name.len() == CAP_EARCON_NAME {
                    coverage.hit("downlink.field_cap.name_at_limit");
                }
            }
            Control::Play { url, format } => {
                coverage.hit("downlink.control.play");
                if url.is_empty() || format.is_empty() {
                    coverage.hit("downlink.empty_value");
                }
                if url.len() == CAP_PLAY_URL {
                    coverage.hit("downlink.field_cap.url_at_limit");
                }
                if format.len() == CAP_PLAY_FORMAT {
                    coverage.hit("downlink.field_cap.format_at_limit");
                }
                if format != "flac" {
                    coverage.hit("downlink.play.format.not_flac");
                }
                if format.contains(['\\', '"', '\t', '\n', '\r']) {
                    coverage.hit("downlink.play.format.escaped");
                }
                if !format.is_ascii() {
                    coverage.hit("downlink.play.format.non_ascii");
                }
            }
            // Same rule as `case_coverage_is_complete`: `stop` is a refused *type*, so it is
            // counted from the refused table (`downlink.control.refused_type.stop`) and must
            // never appear among the acted-on controls.
            Control::Stop => unreachable!(
                "stop is tablet-only and belongs in downlink_capped_cases(), not downlink_cases()"
            ),
        }
    }

    for (_, control) in downlink_capped_cases() {
        coverage.hit("downlink.control.refused");
        match &control {
            Control::Earcon { name } if name.len() == CAP_EARCON_NAME + 1 => {
                coverage.hit("downlink.field_cap.name_one_past");
            }
            Control::Play { url, .. } if url.len() == CAP_PLAY_URL + 1 => {
                coverage.hit("downlink.field_cap.url_one_past");
            }
            Control::Play { format, .. } if format.len() == CAP_PLAY_FORMAT + 1 => {
                coverage.hit("downlink.field_cap.format_one_past");
            }
            // A refused *type*, not a refused field: the one control the backend emits that
            // the firmware has no branch for at all.
            Control::Stop => {
                coverage.hit("downlink.control.refused_type.stop");
            }
            _ => {}
        }
    }

    for case in split_feed_cases() {
        coverage.hit("downlink.fragment_feed");
        let first = case.fragments.first().map(Vec::len).unwrap_or(0);
        if first == 1 {
            coverage.hit("downlink.fragment.header");
        }
        if first == 2 {
            coverage.hit("downlink.fragment.length_prefix");
        }
        if (3..).contains(&first) && case.statuses.len() == 2 {
            coverage.hit("downlink.fragment.payload");
        }
        if case.statuses.iter().all(|s| *s == "pending") {
            coverage.hit("downlink.fragment.record_boundary");
        }
        if case.controls.len() > 1 && case.statuses.first().copied() == Some("need_more") {
            coverage.hit("downlink.fragment.resume_then_second");
        }
        if case.fragments.len() > 20 {
            coverage.hit("downlink.fragment.one_byte_at_a_time");
        }
    }

    for case in total_payload_cases() {
        coverage.hit("downlink.total_payload_cap");
        let _ = case;
    }

    if !ADVERSARIAL.is_empty() {
        coverage.hit("verdict.adversarial");
    }
    if !INTENTIONAL_DIVERGENCES.is_empty() {
        coverage.hit("verdict.intentional_divergence");
    }

    coverage
}
