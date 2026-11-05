//! Typed configuration: the TOML surface enumerated in plan §4 (S1.1).
//!
//! Everything tunable lives here, never in code (plan principle 4: "thresholds live in
//! config, not code"). [`Config::validate`] runs at load time so a bad file fails at
//! startup with a readable message rather than misbehaving at 3 a.m.
//!
//! Documented interpretation choices (the plan enumerates keys, not their failure modes;
//! the plan itself is unchanged — see `docs/design/CONTRACT-NOTES.md`):
//! - `[[groups]]` is optional. When present, every `sources[].group` MUST name one of
//!   them (catches a typo'd room group). When absent, group names are taken as-is from
//!   the sources, which is what a single-room setup needs.
//! - `ha.internal_url` may be empty, meaning "satellites can reach `ha.url` directly".
//! - Only values that make a trigger impossible or a window meaningless are rejected;
//!   HA/Parakeet reachability is not checked here (that is `replay`'s job, offline).
//!
//! This module is a **contract file**: lanes MUST NOT edit it (see `docs/design/CONTRACT-NOTES.md`).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use serde::Deserialize;

/// Root configuration document.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Config {
    pub server: Server,
    pub ha: Ha,
    pub parakeet: Parakeet,
    pub kws: Kws,
    #[serde(default)]
    pub capture: Capture,
    pub clips: Clips,
    #[serde(default)]
    pub sources: Vec<Source>,
    /// Optional group declarations. See the module docs for the validation rule.
    #[serde(default)]
    pub groups: Vec<Group>,
}

/// `[server]` — the uplink socket and its admission policy (plan R11, S1.4, S7.1).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Server {
    /// `host:port` to listen on; the design default is `0.0.0.0:10700`.
    pub listen: String,
    /// Shared hello secret. `None` ⇒ sources are accepted and one loud warning is
    /// logged at startup (R11). Compared with [`crate::proto::constant_time_eq`].
    #[serde(default)]
    pub auth_token: Option<String>,
    /// Unknown `source_id`s are closed + logged unless this is set (S7.1 flips it).
    #[serde(default)]
    pub allow_unknown_sources: bool,
}

/// `[ha]` — Home Assistant WebSocket control plane (S4.3).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Ha {
    /// WebSocket base, e.g. `ws://ha.example.lan:8123` (also used for REST).
    pub url: String,
    /// Long-lived access token.
    pub token: String,
    /// Assist pipeline to enter at the intent stage.
    pub pipeline_id: String,
    /// URL the *satellites* use to fetch `/api/tts_proxy/…`. Empty ⇒ use `url`.
    #[serde(default)]
    pub internal_url: String,
}

/// `[parakeet]` — the Wyoming ASR service (S3.3).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Parakeet {
    /// `host:port` of the preferred ASR. Measured placement and the numbers behind it:
    /// `docs/deployment.md`, "The ASR: where it runs, and what it costs".
    pub addr: String,
    /// Ordered fallbacks, tried only when `addr` fails, and then only after its cooldown
    /// expires. Empty by default: a deployment with one ASR must not pay a probe on every
    /// utterance. Each entry is validated like `addr`, because a fallback with a typo in it is
    /// not a fallback — it is the reason a bad night has no second tier.
    #[serde(default)]
    pub fallback_addrs: Vec<String>,
}

/// `[kws]` — sherpa-onnx keyword spotting (S2).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Kws {
    /// Model directory (encoder/decoder/joiner + tokens) or model bundle path.
    pub model: String,
    /// Tokenized keyword file produced with sherpa-onnx `text2token` (`luna`, `hey luna`).
    pub keywords_file: String,
    /// Global spotter threshold. Loose on purpose: recall is sacred, the verify gate
    /// rejects the false accepts (plan principle 1).
    #[serde(default = "default_threshold")]
    pub threshold: f32,
    /// Per-keyword boosts, e.g. `luna = 2.0`. Empty ⇒ no boost.
    #[serde(default)]
    pub boosts: HashMap<String, f32>,
    /// Optional second keyword file for the stop lane ("luna stop" spoken over a
    /// playing reply). `None` ⇒ no keyword-based stop; the transcript phrases in
    /// [`Capture::stop_phrases`] still work.
    #[serde(default)]
    pub stop_keywords_file: Option<String>,
    /// Spotter threshold for the stop keywords. `None` ⇒ use [`Kws::threshold`].
    #[serde(default)]
    pub stop_threshold: Option<f32>,
    /// Words that count as the wake word in the ASR transcript (verify gate). Each entry
    /// is one normalised word; `word's` forms are accepted automatically. Include the
    /// spellings the ASR actually produces for your keyword, not only the intended one.
    #[serde(default = "default_wake_words")]
    pub wake_words: Vec<String>,
}

fn default_threshold() -> f32 {
    0.25
}

/// Words the verify gate accepts as the wake word in a transcript: the spotter proposes,
/// the transcript disposes. List every spelling the ASR may give your wake word.
fn default_wake_words() -> Vec<String> {
    ["luna", "lunar", "loona", "lunas"].map(String::from).to_vec()
}

/// Canonical form of a spoken stop phrase (S8): case- and punctuation-insensitive,
/// single-spaced. A config entry is compared against its own normalised form, and the
/// coordinator matches transcripts in the same form, so what an operator writes and
/// what the transcriber produces meet in one form.
pub fn normalise_phrase(s: &str) -> String {
    let lower = s.to_lowercase();
    // Stripping every leading/trailing character that is not alphanumeric or an
    // apostrophe also performs the trim (a space is not a word character), and leaves
    // an apostrophe alone inside a word — "it's" is a phrase, "Stop." is "stop".
    let is_word_char = |c: char| c.is_alphanumeric() || c == '\'';
    lower
        .trim_matches(|c: char| !is_word_char(c))
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

/// `[capture]` — arbitration, endpointing and follow-up windows (S3, S5, S6).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Capture {
    /// Cross-source collection window opened by the first wake in an idle group (S5.1).
    #[serde(default = "default_arbitration_window_ms")]
    pub arbitration_window_ms: u64,
    /// Trailing silence that ends an utterance (S3.2).
    #[serde(default = "default_trailing_silence_ms")]
    pub trailing_silence_ms: u64,
    /// Hard cap on one utterance; equals the ring depth (S3.2).
    #[serde(default = "default_hard_cap_s")]
    pub hard_cap_s: u64,
    /// Backwards padding when VAD finds no speech onset (S3.1 fallback).
    #[serde(default = "default_preroll_ms")]
    pub preroll_ms: u64,
    /// Follow-up window after `playback_done` (S6.1).
    #[serde(default = "default_followup_s")]
    pub followup_s: u64,
    /// Give up on a Speaking session with no `playback_done` (S4.4).
    #[serde(default = "default_speaking_timeout_s")]
    pub speaking_timeout_s: u64,
    /// Noise estimate window preceding the keyword span (S5.1).
    #[serde(default = "default_noise_window_s")]
    pub noise_window_s: u64,
    /// Floor for the shrunk noise window; the fallback takes the *earliest* this many
    /// ms of the window, farthest from the span (S5.1, B2-1).
    #[serde(default = "default_noise_floor_ms")]
    pub noise_floor_ms: u64,
    /// Minimum speech length accepted as a follow-up utterance; 0 disables the check
    /// (S6.2).
    #[serde(default)]
    pub min_utterance_ms: u64,
    /// Silero VAD model path for the backwards pass and live endpointing (S3.1/S3.2).
    /// `None` ⇒ use the [`Kws::model`] directory, which is where the sherpa-onnx model
    /// bundle ships the VAD model. Present because the plan names a Silero VAD
    /// component without saying where its model comes from; optional, so plan-shaped
    /// configs stay valid (documented in `docs/design/CONTRACT-NOTES.md`).
    #[serde(default)]
    pub vad_model: Option<String>,
    /// Transcript phrases that end a conversation (S8). Compared with
    /// [`normalise_phrase`] against whole-word matches in the utterance text.
    /// Defaults to the single bare word `stop`; the list is refused at load if it
    /// is empty or contains an entry that is not already normalised.
    #[serde(default = "default_stop_phrases")]
    pub stop_phrases: Vec<String>,
}

fn default_stop_phrases() -> Vec<String> {
    vec!["stop".to_string()]
}

impl Default for Capture {
    fn default() -> Self {
        Self {
            arbitration_window_ms: default_arbitration_window_ms(),
            trailing_silence_ms: default_trailing_silence_ms(),
            hard_cap_s: default_hard_cap_s(),
            preroll_ms: default_preroll_ms(),
            followup_s: default_followup_s(),
            speaking_timeout_s: default_speaking_timeout_s(),
            noise_window_s: default_noise_window_s(),
            noise_floor_ms: default_noise_floor_ms(),
            min_utterance_ms: 0,
            vad_model: None,
            stop_phrases: default_stop_phrases(),
        }
    }
}

fn default_arbitration_window_ms() -> u64 {
    250
}
fn default_trailing_silence_ms() -> u64 {
    700
}
fn default_hard_cap_s() -> u64 {
    15
}
fn default_preroll_ms() -> u64 {
    300
}
fn default_followup_s() -> u64 {
    6
}
fn default_speaking_timeout_s() -> u64 {
    30
}
fn default_noise_window_s() -> u64 {
    2
}
fn default_noise_floor_ms() -> u64 {
    250
}

/// `[clips]` — evidence retention (S8).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Clips {
    /// Directory for per-trigger clips, `false_accept/`, `missed/`. the backend host (default) or
    /// another trusted host only (plan invariant).
    pub dir: String,
    /// Days of clips to keep; swept daily (S8.1).
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,
}

fn default_retention_days() -> u32 {
    30
}

/// A declared group: one session, one winner (S5.1).
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Group {
    /// Name referenced by `sources[].group`.
    pub name: String,
    /// Fallback `media_player` entity for group members without `ha_entity` (S7.1).
    #[serde(default)]
    pub output_entity: Option<String>,
    /// Fallback HA `device_id` for group members without `device_id` (S7.1).
    #[serde(default)]
    pub output_device_id: Option<String>,
}

/// Where a source's spoken reply is played from (`[[sources]] playback`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Playback {
    /// Home Assistant plays it on the source's `ha_entity` (every ESP32 board).
    #[default]
    Ha,
    /// The backend sends the source a `play` control and the source plays it.
    Source,
}

/// A configured `[[sources]]` entry.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct Source {
    /// Stable id, matched against `hello.source_id`.
    pub id: String,
    pub room: String,
    /// Arbitration group: one session per group (plan invariant).
    pub group: String,
    /// HA device id for `assist_pipeline/run`.
    #[serde(default)]
    pub device_id: Option<String>,
    /// `media_player` entity for TTS playback.
    #[serde(default)]
    pub ha_entity: Option<String>,
    /// Entity whose state mutes (suppresses) this source (S4.5).
    #[serde(default)]
    pub mute_entity: Option<String>,
    #[serde(default = "default_true")]
    pub has_speaker: bool,
    #[serde(default)]
    pub has_aec: bool,
    /// How this source's spoken replies are delivered. `ha` (default) asks Home Assistant to
    /// play them on `ha_entity`; `source` sends the source a `play` control and lets it fetch
    /// and play the audio itself (a client with no HA `media_player`, such as the tablet app).
    #[serde(default)]
    pub playback: Playback,
    /// Device used for non-satellite sources in this group (S7.1).
    #[serde(default)]
    pub fallback_device_id: Option<String>,
}

fn default_true() -> bool {
    true
}

/// Configuration validation failure: every problem found, joined for the operator.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct ConfigError {
    /// Human-readable, one entry per problem.
    pub problems: Vec<String>,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "invalid configuration: {}", self.problems.join("; "))
    }
}

impl std::error::Error for ConfigError {}

impl Config {
    /// Read, parse and validate a TOML file.
    pub fn load(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        let path = path.as_ref();
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("reading {}: {e}", path.display()))?;
        Self::from_toml_str(&text).map_err(|e| anyhow::anyhow!("{}: {e}", path.display()))
    }

    /// Parse and validate TOML text.
    pub fn from_toml_str(text: &str) -> anyhow::Result<Self> {
        let cfg = Self::parse_toml_str(text)?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Parse without validating — for tests that need a deliberately bad config.
    pub fn parse_toml_str(text: &str) -> anyhow::Result<Self> {
        Ok(toml::from_str(text)?)
    }

    /// Look up a configured source by id (ingest's registration path).
    pub fn source(&self, source_id: &str) -> Option<&Source> {
        self.sources.iter().find(|s| s.id == source_id)
    }

    /// Look up a declared group by name.
    pub fn group(&self, name: &str) -> Option<&Group> {
        self.groups.iter().find(|g| g.name == name)
    }

    /// Validate the whole document, reporting every problem at once so one edit fixes
    /// all of them (rules and rationale in the module docs).
    pub fn validate(&self) -> Result<(), ConfigError> {
        let mut p: Vec<String> = Vec::new();

        // [server]
        match parse_host_port(&self.server.listen) {
            None => p.push(format!(
                "server.listen {:?} is not `host:port`",
                self.server.listen
            )),
            Some((_, 0)) => p.push("server.listen port must not be 0".to_string()),
            Some(_) => {}
        }
        if let Some(tok) = &self.server.auth_token {
            if tok.trim().is_empty() {
                p.push("server.auth_token, when set, must not be blank".to_string());
            }
        }

        // [ha]
        require_nonempty(&self.ha.url, "ha.url", &mut p);
        if !self.ha.url.trim().is_empty() {
            // Reject at load what the HA client rejects at connect time, so a typo
            // is a startup error and not every-call-fails-at-runtime.
            if let Some(problem) = ha_url_problem(&self.ha.url) {
                p.push(problem);
            }
        }
        require_nonempty(&self.ha.token, "ha.token", &mut p);
        require_nonempty(&self.ha.pipeline_id, "ha.pipeline_id", &mut p);

        // [parakeet] The preferred endpoint and every fallback are held to the same rule: an
        // address that cannot be parsed is not a tier, it is a silent hole in the plan.
        {
            let check =
                |value: &str, field: String, p: &mut Vec<String>| match parse_host_port(value) {
                    None => p.push(format!("{field} {value:?} is not `host:port`")),
                    Some((_, 0)) => p.push(format!("{field} port must not be 0")),
                    Some(_) => {}
                };
            check(&self.parakeet.addr, "parakeet.addr".to_string(), &mut p);
            for (i, addr) in self.parakeet.fallback_addrs.iter().enumerate() {
                check(addr, format!("parakeet.fallback_addrs[{i}]"), &mut p);
            }
        }

        // [kws]
        require_nonempty(&self.kws.model, "kws.model", &mut p);
        require_nonempty(&self.kws.keywords_file, "kws.keywords_file", &mut p);
        if !(self.kws.threshold.is_finite()
            && self.kws.threshold > 0.0
            && self.kws.threshold <= 1.0)
        {
            p.push(format!(
                "kws.threshold {} must be in (0, 1]",
                self.kws.threshold
            ));
        }
        for (kw, boost) in &self.kws.boosts {
            if !boost.is_finite() || *boost <= 0.0 {
                p.push(format!("kws.boosts.{kw} must be a positive finite number"));
            }
        }
        if let Some(path) = &self.kws.stop_keywords_file {
            if path.trim().is_empty() {
                p.push("kws.stop_keywords_file, when set, must not be blank".to_string());
            }
        }
        if let Some(t) = self.kws.stop_threshold {
            if !(t.is_finite() && t > 0.0 && t <= 1.0) {
                p.push(format!("kws.stop_threshold {t} must be in (0, 1]"));
            }
        }
        // The verify gate compares these against single transcript tokens, so an entry
        // that is empty, multi-word or not already normalised could never match.
        if self.kws.wake_words.is_empty() {
            p.push("kws.wake_words must not be empty (the verify gate would reject every wake)".to_string());
        }
        for word in &self.kws.wake_words {
            if word.is_empty() || word.contains(char::is_whitespace) || normalise_phrase(word) != *word {
                p.push(format!(
                    "kws.wake_words entry {word:?} must be one lowercase word with no \
                     surrounding punctuation"
                ));
            }
        }

        // [capture] — windows must be meaningful, and the noise floor must fit its window.
        let c = &self.capture;
        let hard_cap_ms = c.hard_cap_s.saturating_mul(1000);
        if c.arbitration_window_ms == 0 {
            p.push("capture.arbitration_window_ms must be > 0".to_string());
        }
        if c.trailing_silence_ms == 0 {
            p.push("capture.trailing_silence_ms must be > 0".to_string());
        }
        if c.hard_cap_s == 0 {
            p.push("capture.hard_cap_s must be > 0".to_string());
        }
        if c.followup_s == 0 {
            p.push("capture.followup_s must be > 0".to_string());
        }
        if c.speaking_timeout_s == 0 {
            p.push("capture.speaking_timeout_s must be > 0".to_string());
        }
        if c.noise_window_s == 0 {
            p.push("capture.noise_window_s must be > 0".to_string());
        }
        // An unbounded duration passes every check above and then aborts — or
        // overflows — at first use instead of failing here, where validate exists to
        // fail: `hard_cap_s` becomes a `vec![0; capacity]` ring allocation per
        // connected source (s16 at 32 KiB/s), and the rest become `Instant`
        // deadlines. The ceiling sits far above every design window (ring 15 s,
        // follow-up 6 s) and far below what the 2 GiB / 8-stream budget could ever
        // allocate, so an accepted value cannot crash the first satellite's hello.
        require_at_most(
            c.hard_cap_s,
            MAX_CAPTURE_WINDOW_S,
            "capture.hard_cap_s",
            "s",
            &mut p,
        );
        require_at_most(
            c.followup_s,
            MAX_CAPTURE_WINDOW_S,
            "capture.followup_s",
            "s",
            &mut p,
        );
        require_at_most(
            c.speaking_timeout_s,
            MAX_CAPTURE_WINDOW_S,
            "capture.speaking_timeout_s",
            "s",
            &mut p,
        );
        require_at_most(
            c.noise_window_s,
            MAX_CAPTURE_WINDOW_S,
            "capture.noise_window_s",
            "s",
            &mut p,
        );
        let window_ms = MAX_CAPTURE_WINDOW_S.saturating_mul(1000);
        require_at_most(
            c.trailing_silence_ms,
            window_ms,
            "capture.trailing_silence_ms",
            "ms",
            &mut p,
        );
        require_at_most(
            c.arbitration_window_ms,
            window_ms,
            "capture.arbitration_window_ms",
            "ms",
            &mut p,
        );
        if c.noise_floor_ms == 0 {
            p.push("capture.noise_floor_ms must be > 0".to_string());
        } else if c.noise_floor_ms > c.noise_window_s.saturating_mul(1000) {
            p.push(format!(
                "capture.noise_floor_ms ({} ms) exceeds capture.noise_window_s ({} s)",
                c.noise_floor_ms, c.noise_window_s
            ));
        }
        if c.preroll_ms > hard_cap_ms {
            p.push(format!(
                "capture.preroll_ms ({} ms) exceeds capture.hard_cap_s ({} s)",
                c.preroll_ms, c.hard_cap_s
            ));
        }
        if c.min_utterance_ms > hard_cap_ms {
            p.push(format!(
                "capture.min_utterance_ms ({} ms) exceeds capture.hard_cap_s ({} s)",
                c.min_utterance_ms, c.hard_cap_s
            ));
        }
        // The stop phrases are matched against a transcript that has already been
        // normalised, so an entry that is not in normalised form could never match —
        // a silently dead stop word is worse than a rejected config.
        if c.stop_phrases.is_empty() {
            p.push("capture.stop_phrases must not be empty (a group with no stop word \
                    cannot be interrupted by voice)"
                .to_string());
        }
        for phrase in &c.stop_phrases {
            if phrase.is_empty() {
                p.push("capture.stop_phrases must not contain an empty entry".to_string());
            } else if normalise_phrase(phrase) != *phrase {
                p.push(format!(
                    "capture.stop_phrases entry {phrase:?} is not in normalised form \
                     (lowercase, trimmed, single-spaced): write {:?} instead",
                    normalise_phrase(phrase)
                ));
            }
        }

        // [clips]
        require_nonempty(&self.clips.dir, "clips.dir", &mut p);
        // A root with no parent is everything below it: `Path::parent` answers
        // `None` for `/`, and a bare `.` names the same danger relative to the
        // working directory. The retention sweep walks its root recursively and
        // deletes every old `.wav`/`.json` it finds there — so the config must
        // refuse to arm it over anything that is not a dedicated directory (the
        // sweep's own delete-guard is a separate change).
        let clips_dir = self.clips.dir.trim();
        if !clips_dir.is_empty()
            && (matches!(clips_dir, "." | "./") || Path::new(clips_dir).parent().is_none())
        {
            p.push(format!(
                "clips.dir {:?} is a filesystem root: the retention sweep would scan \
                 (and prune) the whole tree under it — point it at a dedicated directory",
                self.clips.dir
            ));
        }
        if self.clips.retention_days == 0 {
            p.push("clips.retention_days must be > 0 (0 would delete everything)".to_string());
        }

        // [[sources]]
        let mut ids: HashSet<&str> = HashSet::new();
        for (i, s) in self.sources.iter().enumerate() {
            if s.id.trim().is_empty() {
                p.push(format!("sources[{i}].id must not be empty"));
            } else if !ids.insert(s.id.as_str()) {
                p.push(format!("sources[{i}].id {:?} is duplicated", s.id));
            }
            if s.room.trim().is_empty() {
                p.push(format!("sources[{i}].room must not be empty"));
            }
            if s.group.trim().is_empty() {
                p.push(format!("sources[{i}].group must not be empty"));
            }
            // A source that plays replies itself must be able to: with no speaker there is
            // nothing to send `play` to, and the group would sit in Speaking for the full
            // `speaking_timeout_s` with its microphone suppressed.
            if s.playback == Playback::Source && !s.has_speaker {
                p.push(format!(
                    "sources[{i}] ({:?}) has playback = \"source\" but has_speaker = false",
                    s.id
                ));
            }
        }

        // [[groups]] — declared names are authoritative when present.
        let mut names: HashSet<&str> = HashSet::new();
        for (i, g) in self.groups.iter().enumerate() {
            if g.name.trim().is_empty() {
                p.push(format!("groups[{i}].name must not be empty"));
            } else if !names.insert(g.name.as_str()) {
                p.push(format!("groups[{i}].name {:?} is duplicated", g.name));
            }
        }
        if !self.groups.is_empty() {
            for s in &self.sources {
                if !s.group.trim().is_empty() && !names.contains(s.group.as_str()) {
                    p.push(format!(
                        "source {:?} references undeclared group {:?}",
                        s.id, s.group
                    ));
                }
            }
        }

        if p.is_empty() {
            Ok(())
        } else {
            Err(ConfigError { problems: p })
        }
    }
}

fn require_nonempty(value: &str, key: &str, problems: &mut Vec<String>) {
    if value.trim().is_empty() {
        problems.push(format!("{key} must not be empty"));
    }
}

/// The largest capture window [`Config::validate`] accepts, in seconds.
///
/// `capture.hard_cap_s` is the ring depth, and a ring is allocated per connected
/// source (`ingest.rs` derives it with `ms_to_samples(hard_cap_s * 1000)`). At
/// 16 kHz mono s16 that is 32 KiB per second of window, so even this ceiling
/// (8 sources × 300 s ≈ 75 MiB) fits the 2 GiB / 8-stream LXC budget — while a
/// `u64`-scale value would abort on the allocation, or overflow the multiplication
/// first, long after `health` said the deployment was valid.
const MAX_CAPTURE_WINDOW_S: u64 = 300;

/// Reject a duration field above `limit`. The zero and cross-field checks catch
/// meaningless windows; this catches the ones that are not merely meaningless but
/// unallocatable, whose failure mode is a crash at first use rather than a
/// readable startup error.
fn require_at_most(value: u64, limit: u64, key: &str, unit: &str, problems: &mut Vec<String>) {
    if value > limit {
        problems.push(format!(
            "{key} ({value} {unit}) exceeds the maximum capture window of \
             {MAX_CAPTURE_WINDOW_S} s"
        ));
    }
}

/// The syntax half of `ha::socket_parts` (src/ha.rs), duplicated here so
/// `Config::validate` rejects exactly the URLs the HA client would refuse to dial:
/// `ws://` and `http://` (the alias ha.rs accepts) pass; `wss://`/`https://` need
/// TLS this build does not enable; anything else is not a scheme ha.rs knows.
fn ha_url_problem(url: &str) -> Option<String> {
    let url = url.trim();
    let Some((scheme, rest)) = url.split_once("://") else {
        return Some(format!(
            "ha.url {url:?} must start with ws:// or http:// (ha.rs cannot dial it)"
        ));
    };
    match scheme.to_ascii_lowercase().as_str() {
        "ws" | "http" => {}
        "wss" | "https" => {
            return Some(format!(
                "ha.url {url:?} needs TLS, which this build does not enable; use ws:// on the LAN"
            ));
        }
        other => return Some(format!("ha.url {url:?} has unsupported scheme {other:?}")),
    }
    let authority = rest.trim_end_matches('/');
    let authority = authority.split('/').next().unwrap_or("");
    if authority.is_empty() {
        return Some(format!("ha.url {url:?} has no host"));
    }
    None
}

/// Split `host:port`. Rejects an empty host, a missing colon, and a non-numeric or
/// out-of-range port. Bracketed IPv6 literals are accepted. A host carrying URL
/// syntax is not a host: `ws://ha.example.lan:1` splits into host `ws://ha.example.lan` + port `1`,
/// which used to validate and then failed at bind or connect — validation must
/// reject what the runtime rejects. The host is returned trimmed.
fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        // [::1]:10700
        let close = rest.find(']')?;
        let host = &rest[..close];
        let tail = rest[close + 1..].strip_prefix(':')?;
        (host.to_string(), tail)
    } else {
        let (host, port) = s.rsplit_once(':')?;
        (host.to_string(), port)
    };
    let host = host.trim();
    if host.is_empty()
        || host.contains("://")
        || host.contains(['/', '?', '#'])
        || host.chars().any(char::is_whitespace)
    {
        return None;
    }
    let port: u16 = port.trim().parse().ok()?;
    Some((host.to_string(), port))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The plan's own shape, with every key set explicitly.
    const VALID: &str = r#"
[server]
listen = "0.0.0.0:10700"
auth_token = "household-shared-secret"
allow_unknown_sources = false

[ha]
url = "ws://ha.example.lan:8123"
token = "ha-long-lived-token"
pipeline_id = "01J9ZEXAMPLE"
internal_url = "http://192.168.1.20:8123"

[parakeet]
addr = "127.0.0.1:10200"

[kws]
model = "/opt/listenrr/models/kws-zipformer"
keywords_file = "/etc/listenrr/keywords.txt"
threshold = 0.35
boosts = { luna = 2.0, "hey luna" = 1.5 }

[capture]
arbitration_window_ms = 250
trailing_silence_ms = 700
hard_cap_s = 15
preroll_ms = 300
followup_s = 6
speaking_timeout_s = 30
noise_window_s = 2
noise_floor_ms = 250
min_utterance_ms = 300

[clips]
dir = "/var/lib/listenrr/clips"
retention_days = 30

[[groups]]
name = "kitchen"
output_entity = "media_player.kitchen_speaker"
output_device_id = "aa:bb:cc:dd:ee:ff"

[[sources]]
id = "kitchen"
room = "kitchen"
group = "kitchen"
device_id = "aa:bb:cc:dd:ee:ff"
ha_entity = "media_player.kitchen_satellite"
mute_entity = "switch.kitchen_mute"
has_speaker = true
has_aec = false

[[sources]]
id = "hallway"
room = "hallway"
group = "kitchen"
device_id = "11:22:33:44:55:66"
ha_entity = "media_player.hallway_satellite"
has_aec = true
fallback_device_id = "aa:bb:cc:dd:ee:ff"
"#;

    /// Only the keys the plan marks required; everything else must default.
    const MINIMAL: &str = r#"
[server]
listen = "127.0.0.1:10700"

[ha]
url = "ws://localhost:8123"
token = "t"
pipeline_id = "p"

[parakeet]
addr = "127.0.0.1:10200"

[kws]
model = "m"
keywords_file = "k"

[clips]
dir = "/tmp/clips"

[[sources]]
id = "s1"
room = "r"
group = "g"
"#;

    fn valid() -> Config {
        Config::from_toml_str(VALID).expect("valid fixture must parse and validate")
    }

    /// Replace one occurrence of `old` in the valid document and require a rejection
    /// whose message contains `needle`.
    #[track_caller]
    fn expect_reject(old: &str, new: &str, needle: &str) {
        let text = VALID.replacen(old, new, 1);
        assert_ne!(text, VALID, "replacement had no effect: {old}");
        let err = Config::from_toml_str(&text)
            .err()
            .unwrap_or_else(|| panic!("expected rejection mentioning {needle:?}"));
        let msg = err.to_string();
        assert!(msg.contains(needle), "error {msg:?} lacks {needle:?}");
    }

    #[test]
    fn valid_document_loads_completely() {
        let cfg = valid();
        assert_eq!(cfg.server.listen, "0.0.0.0:10700");
        assert_eq!(
            cfg.server.auth_token.as_deref(),
            Some("household-shared-secret")
        );
        assert!(!cfg.server.allow_unknown_sources);
        assert_eq!(cfg.ha.url, "ws://ha.example.lan:8123");
        assert_eq!(cfg.ha.token, "ha-long-lived-token");
        assert_eq!(cfg.ha.pipeline_id, "01J9ZEXAMPLE");
        assert_eq!(cfg.ha.internal_url, "http://192.168.1.20:8123");
        assert_eq!(cfg.parakeet.addr, "127.0.0.1:10200");
        assert_eq!(cfg.kws.model, "/opt/listenrr/models/kws-zipformer");
        assert_eq!(cfg.kws.keywords_file, "/etc/listenrr/keywords.txt");
        assert_eq!(cfg.kws.threshold, 0.35);
        assert_eq!(cfg.kws.boosts.get("luna"), Some(&2.0));
        assert_eq!(cfg.kws.boosts.get("hey luna"), Some(&1.5));
        let c = &cfg.capture;
        assert_eq!(
            (c.arbitration_window_ms, c.trailing_silence_ms, c.hard_cap_s),
            (250, 700, 15)
        );
        assert_eq!(
            (c.preroll_ms, c.followup_s, c.speaking_timeout_s),
            (300, 6, 30)
        );
        assert_eq!(
            (c.noise_window_s, c.noise_floor_ms, c.min_utterance_ms),
            (2, 250, 300)
        );
        assert_eq!(cfg.clips.dir, "/var/lib/listenrr/clips");
        assert_eq!(cfg.clips.retention_days, 30);
        assert_eq!(cfg.sources.len(), 2);
        let k = cfg.source("kitchen").unwrap();
        assert_eq!(k.room, "kitchen");
        assert_eq!(k.group, "kitchen");
        assert_eq!(k.device_id.as_deref(), Some("aa:bb:cc:dd:ee:ff"));
        assert_eq!(
            k.ha_entity.as_deref(),
            Some("media_player.kitchen_satellite")
        );
        assert_eq!(k.mute_entity.as_deref(), Some("switch.kitchen_mute"));
        assert!(k.has_speaker);
        assert!(!k.has_aec);
        assert_eq!(k.fallback_device_id, None);
        let h = cfg.source("hallway").unwrap();
        assert!(h.has_aec);
        assert_eq!(h.fallback_device_id.as_deref(), Some("aa:bb:cc:dd:ee:ff"));
        assert_eq!(cfg.source("nope"), None);
        let g = cfg.group("kitchen").unwrap();
        assert_eq!(
            g.output_entity.as_deref(),
            Some("media_player.kitchen_speaker")
        );
        assert_eq!(g.output_device_id.as_deref(), Some("aa:bb:cc:dd:ee:ff"));
        assert_eq!(cfg.group("nope"), None);
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn plan_defaults_apply_when_keys_are_omitted() {
        let cfg = Config::from_toml_str(MINIMAL).expect("minimal doc must validate");
        assert_eq!(cfg.server.auth_token, None);
        assert!(!cfg.server.allow_unknown_sources);
        assert_eq!(cfg.ha.internal_url, "");
        assert_eq!(cfg.kws.threshold, 0.25);
        assert!(cfg.kws.boosts.is_empty());
        assert_eq!(cfg.capture, Capture::default());
        let c = &cfg.capture;
        assert_eq!(c.arbitration_window_ms, 250);
        assert_eq!(c.trailing_silence_ms, 700);
        assert_eq!(c.hard_cap_s, 15);
        assert_eq!(c.preroll_ms, 300);
        assert_eq!(c.followup_s, 6);
        assert_eq!(c.speaking_timeout_s, 30);
        assert_eq!(c.noise_window_s, 2);
        assert_eq!(c.noise_floor_ms, 250);
        assert_eq!(c.min_utterance_ms, 0);
        assert_eq!(c.vad_model, None, "VAD model is optional");
        assert_eq!(cfg.clips.retention_days, 30);
        assert_eq!(cfg.groups.len(), 0);
        let s = cfg.source("s1").unwrap();
        assert!(s.has_speaker, "has_speaker defaults true");
        assert!(!s.has_aec, "has_aec defaults false");
        assert_eq!(s.device_id, None);
        assert_eq!(s.ha_entity, None);
        assert_eq!(s.mute_entity, None);
        assert_eq!(s.fallback_device_id, None);
    }

    #[test]
    fn capture_default_is_the_plan_table() {
        let c = Capture::default();
        assert_eq!(c.arbitration_window_ms, 250);
        assert_eq!(c.trailing_silence_ms, 700);
        assert_eq!(c.hard_cap_s, 15);
        assert_eq!(c.preroll_ms, 300);
        assert_eq!(c.followup_s, 6);
        assert_eq!(c.speaking_timeout_s, 30);
        assert_eq!(c.noise_window_s, 2);
        assert_eq!(c.noise_floor_ms, 250);
        assert_eq!(c.min_utterance_ms, 0);
    }

    #[test]
    fn loads_from_a_file_and_reports_a_missing_or_bad_one() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("listenrr.toml");
        std::fs::write(&path, VALID).unwrap();
        let cfg = Config::load(&path).expect("file loads");
        assert_eq!(cfg, valid());

        let err = Config::load(dir.path().join("nope.toml"))
            .err()
            .unwrap()
            .to_string();
        assert!(err.contains("nope.toml"), "{err}");

        let bad = dir.path().join("bad.toml");
        std::fs::write(&bad, "listen = ").unwrap();
        let err = Config::load(&bad).err().unwrap().to_string();
        assert!(err.contains("bad.toml"), "{err}");

        // A file that parses but fails validation names the file too.
        let invalid = dir.path().join("invalid.toml");
        std::fs::write(
            &invalid,
            VALID.replace("noise_floor_ms = 250", "noise_floor_ms = 9000"),
        )
        .unwrap();
        let err = Config::load(&invalid).err().unwrap().to_string();
        assert!(err.contains("invalid.toml"), "{err}");
        assert!(err.contains("noise_floor_ms"), "{err}");
    }

    #[test]
    fn rejects_bad_listen_addresses() {
        expect_reject(
            "listen = \"0.0.0.0:10700\"",
            "listen = \"0.0.0.0:70000\"",
            "server.listen",
        );
        expect_reject(
            "listen = \"0.0.0.0:10700\"",
            "listen = \"ha.example.lan\"",
            "server.listen",
        );
        expect_reject(
            "listen = \"0.0.0.0:10700\"",
            "listen = \"\"",
            "server.listen",
        );
        expect_reject(
            "listen = \"0.0.0.0:10700\"",
            "listen = \":10700\"",
            "server.listen",
        );
        let mut cfg = valid();
        cfg.server.listen = "0.0.0.0:0".into();
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("port must not be 0"));
        // Bracketed IPv6 is fine.
        cfg.server.listen = "[::1]:10700".into();
        assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
    }

    #[test]
    fn rejects_blank_auth_token_but_allows_absent() {
        let mut cfg = valid();
        cfg.server.auth_token = Some("   ".into());
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("auth_token"));
        cfg.server.auth_token = None;
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn rejects_empty_ha_parakeet_and_kws_fields() {
        expect_reject(
            "token = \"ha-long-lived-token\"",
            "token = \"\"",
            "ha.token",
        );
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"  \"",
            "ha.url",
        );
        expect_reject(
            "pipeline_id = \"01J9ZEXAMPLE\"",
            "pipeline_id = \"\"",
            "ha.pipeline_id",
        );
        expect_reject(
            "addr = \"127.0.0.1:10200\"",
            "addr = \"parakeet\"",
            "parakeet.addr",
        );
        expect_reject(
            "addr = \"127.0.0.1:10200\"",
            "addr = \"127.0.0.1:10200\"\nfallback_addrs = [\"cpu-box\"]",
            "parakeet.fallback_addrs[0]",
        );
        // A portless or zero-port fallback is just as unreachable as a portless primary.
        expect_reject(
            "addr = \"127.0.0.1:10200\"",
            "addr = \"127.0.0.1:10200\"\nfallback_addrs = [\"127.0.0.1:0\"]",
            "fallback_addrs[0] port must not be 0",
        );
        // Absent is legal and means one tier: a deployment with a single ASR must not be
        // forced to name itself twice to be valid.
        let single = Config::from_toml_str(VALID).expect("one tier is valid");
        assert!(single.parakeet.fallback_addrs.is_empty());
        let two = Config::from_toml_str(&VALID.replacen(
            "addr = \"127.0.0.1:10200\"",
            "addr = \"127.0.0.1:10200\"\nfallback_addrs = [\"127.0.0.1:10201\", \"127.0.0.1:10202\"]",
            1,
        ))
        .expect("ordered fallbacks parse");
        assert_eq!(
            two.parakeet.fallback_addrs,
            vec!["127.0.0.1:10201".to_string(), "127.0.0.1:10202".to_string()],
            "the order the operator wrote is the order the tiers are tried in"
        );
        expect_reject(
            "model = \"/opt/listenrr/models/kws-zipformer\"",
            "model = \"\"",
            "kws.model",
        );
        expect_reject(
            "keywords_file = \"/etc/listenrr/keywords.txt\"",
            "keywords_file = \"\"",
            "kws.keywords_file",
        );
    }

    #[test]
    fn internal_url_may_be_empty() {
        // Documented choice: empty means "satellites can reach ha.url".
        let text = VALID.replace(
            "internal_url = \"http://192.168.1.20:8123\"",
            "internal_url = \"\"",
        );
        assert!(Config::from_toml_str(&text).is_ok());
        let mut cfg = valid();
        cfg.ha.internal_url = String::new();
        assert!(cfg.validate().is_ok());
    }

    /// Every source in the shipped fixture predates `playback`, so none of them may change:
    /// the default has to be the Home Assistant route.
    #[test]
    fn playback_defaults_to_the_home_assistant_route() {
        let cfg = valid();
        assert!(cfg.sources.iter().all(|s| s.playback == Playback::Ha));
        let minimal = Config::from_toml_str(MINIMAL).unwrap();
        assert_eq!(minimal.sources[0].playback, Playback::Ha);
    }

    /// A tablet has no Home Assistant `media_player`, so a `source` entry needs no `ha_entity`.
    #[test]
    fn a_source_that_plays_itself_needs_no_ha_entity() {
        let text = VALID.replace(
            "ha_entity = \"media_player.hallway_satellite\"\n",
            "playback = \"source\"\n",
        );
        assert_ne!(text, VALID);
        let cfg = Config::from_toml_str(&text).expect("a self-playing source with no ha_entity");
        let hallway = cfg.source("hallway").unwrap();
        assert_eq!(hallway.playback, Playback::Source);
        assert_eq!(hallway.ha_entity, None);
        assert_eq!(
            cfg.source("kitchen").unwrap().playback,
            Playback::Ha,
            "and its neighbour is untouched"
        );
    }

    /// With no speaker there is nothing to send `play` to: the group would sit in Speaking
    /// with its microphone suppressed until the timeout, so the config is refused up front and
    /// the message names the offending source.
    #[test]
    fn a_self_playing_source_without_a_speaker_is_rejected_by_name() {
        let text = VALID.replace(
            "has_aec = true\n",
            "has_aec = true\nhas_speaker = false\nplayback = \"source\"\n",
        );
        assert_ne!(text, VALID);
        let err = Config::from_toml_str(&text).err().unwrap().to_string();
        assert!(err.contains("\"hallway\""), "{err}");
        assert!(err.contains("has_speaker"), "{err}");
        // The same speaker-less source on the HA route is still fine: only `source` needs one.
        let ha_route = VALID.replace("has_aec = true\n", "has_aec = true\nhas_speaker = false\n");
        assert!(Config::from_toml_str(&ha_route).is_ok());
    }

    #[test]
    fn an_unknown_playback_value_is_rejected() {
        let text = VALID.replace(
            "has_aec = true\n",
            "has_aec = true\nplayback = \"carrier-pigeon\"\n",
        );
        assert!(Config::from_toml_str(&text).is_err());
    }

    #[test]
    fn rejects_out_of_range_thresholds_and_boosts() {
        for bad in ["0.0", "1.5", "-0.1", "nan", "inf"] {
            let text = VALID.replace("threshold = 0.35", &format!("threshold = {bad}"));
            let err = Config::from_toml_str(&text)
                .err()
                .unwrap_or_else(|| panic!("threshold {bad} was accepted"));
            assert!(err.to_string().contains("kws.threshold"), "{err}");
        }
        let text = VALID.replace(
            "boosts = { luna = 2.0, \"hey luna\" = 1.5 }",
            "boosts = { luna = 0.0 }",
        );
        assert!(Config::from_toml_str(&text)
            .err()
            .unwrap()
            .to_string()
            .contains("kws.boosts.luna"));
    }

    /// The canonical form a stop phrase must be written in (S8): the transcript is
    /// compared against these strings, so case, punctuation and spacing differences
    /// in the config would make a stop word silently un-matchable.
    #[test]
    fn normalises_stop_phrases() {
        for (input, expected) in [
            ("Stop.", "stop"),
            (" Stop ", "stop"),
            ("stop the music", "stop the music"),
            ("it's", "it's"),
            ("", ""),
            ("...", ""),
            ("STOP   IT", "stop it"),
            ("-- stop --", "stop"),
            ("\tsilence\t", "silence"),
        ] {
            assert_eq!(normalise_phrase(input), expected, "normalise {input:?}");
        }
    }

    /// Omitting the stop keys must leave a working default stop word and no keyword
    /// lane — the plan's shape stays valid without mentioning stop at all.
    #[test]
    fn stop_options_default_when_absent() {
        let cfg = Config::from_toml_str(MINIMAL).expect("minimal must parse");
        assert_eq!(cfg.capture.stop_phrases, vec!["stop".to_string()]);
        assert_eq!(cfg.kws.stop_keywords_file, None);
        assert_eq!(cfg.kws.stop_threshold, None);
        assert_eq!(valid().capture.stop_phrases, vec!["stop".to_string()]);
    }

    /// The verify gate's wake words default to the shipped keyword set, are replaceable,
    /// and an entry the gate could never match is refused at load.
    #[test]
    fn wake_words_default_override_and_validate() {
        let cfg = Config::from_toml_str(MINIMAL).expect("minimal must parse");
        assert_eq!(cfg.kws.wake_words, ["luna", "lunar", "loona", "lunas"]);

        let text = VALID.replacen("threshold = 0.35\n", "threshold = 0.35\nwake_words = [\"computer\"]\n", 1);
        assert_ne!(text, VALID);
        let cfg = Config::from_toml_str(&text).expect("a custom wake word must be accepted");
        assert_eq!(cfg.kws.wake_words, ["computer"]);

        for bad in ["[]", "[\"\"]", "[\"Computer\"]", "[\"hey computer\"]", "[\"computer.\"]"] {
            expect_reject("threshold = 0.35\n", &format!("threshold = 0.35\nwake_words = {bad}\n"), "kws.wake_words");
        }
    }

    /// An entry that cannot survive comparison with its own normalised form is a dead
    /// stop word, so it is refused at load and named; a normalised list is accepted.
    #[test]
    fn rejects_unnormalised_or_empty_stop_phrases() {
        for bad in ["Stop", "", " stop", "stop."] {
            let text = VALID.replacen(
                "min_utterance_ms = 300\n",
                &format!("min_utterance_ms = 300\nstop_phrases = [\"{bad}\"]\n"),
                1,
            );
            assert_ne!(text, VALID);
            let err = Config::from_toml_str(&text)
                .err()
                .unwrap_or_else(|| panic!("stop phrase {bad:?} was accepted"));
            assert!(err.to_string().contains("capture.stop_phrases"), "{err}");
        }
        // An empty list would leave a group with no spoken way out.
        expect_reject(
            "min_utterance_ms = 300",
            "min_utterance_ms = 300\nstop_phrases = []",
            "capture.stop_phrases",
        );
        // The stop spotter is held to the same range as the wake spotter.
        expect_reject(
            "threshold = 0.35",
            "threshold = 0.35\nstop_threshold = 0.0",
            "kws.stop_threshold",
        );
        expect_reject(
            "threshold = 0.35",
            "threshold = 0.35\nstop_threshold = 1.5",
            "kws.stop_threshold",
        );
        expect_reject(
            "keywords_file = \"/etc/listenrr/keywords.txt\"",
            "keywords_file = \"/etc/listenrr/keywords.txt\"\nstop_keywords_file = \"  \"",
            "kws.stop_keywords_file",
        );
        // Multi-word phrases are fine as long as they are normalised.
        let text = VALID.replacen(
            "min_utterance_ms = 300\n",
            "min_utterance_ms = 300\nstop_phrases = [\"stop\", \"stop it\"]\n",
            1,
        );
        assert_ne!(text, VALID);
        let cfg = Config::from_toml_str(&text).expect("normalised phrases must be accepted");
        assert_eq!(cfg.capture.stop_phrases, vec!["stop", "stop it"]);
    }

    #[test]
    fn rejects_incoherent_capture_windows() {
        // The rule the plan states outright: the noise floor must fit inside its window.
        expect_reject(
            "noise_floor_ms = 250",
            "noise_floor_ms = 3000",
            "noise_floor_ms",
        );
        expect_reject(
            "noise_floor_ms = 250",
            "noise_floor_ms = 0",
            "noise_floor_ms",
        );
        expect_reject(
            "arbitration_window_ms = 250",
            "arbitration_window_ms = 0",
            "arbitration_window_ms",
        );
        expect_reject(
            "trailing_silence_ms = 700",
            "trailing_silence_ms = 0",
            "trailing_silence_ms",
        );
        expect_reject("hard_cap_s = 15", "hard_cap_s = 0", "hard_cap_s");
        expect_reject("followup_s = 6", "followup_s = 0", "followup_s");
        expect_reject(
            "speaking_timeout_s = 30",
            "speaking_timeout_s = 0",
            "speaking_timeout_s",
        );
        expect_reject("noise_window_s = 2", "noise_window_s = 0", "noise_window_s");
        // A preroll or min-utterance longer than the hard cap cannot be honoured.
        expect_reject("preroll_ms = 300", "preroll_ms = 20000", "preroll_ms");
        expect_reject(
            "min_utterance_ms = 300",
            "min_utterance_ms = 20000",
            "min_utterance_ms",
        );
    }

    /// A duration big enough to survive the zero and cross-field checks must still
    /// be rejected: `hard_cap_s` becomes a per-source ring allocation and the rest
    /// become `Instant` deadlines, so an unbounded value aborts — or overflows the
    /// `ms_to_samples` multiplication — at the first satellite's hello, long after
    /// `health` said the deployment was valid.
    #[test]
    fn rejects_huge_capture_durations() {
        expect_reject(
            "hard_cap_s = 15",
            "hard_cap_s = 1000000000",
            "hard_cap_s",
        );
        // The arithmetic trap itself: `u64::MAX` is a validation error, not a panic.
        expect_reject(
            "hard_cap_s = 15",
            "hard_cap_s = 18446744073709551615",
            "hard_cap_s",
        );
        expect_reject("followup_s = 6", "followup_s = 100000", "followup_s");
        expect_reject(
            "speaking_timeout_s = 30",
            "speaking_timeout_s = 100000",
            "speaking_timeout_s",
        );
        expect_reject(
            "noise_window_s = 2",
            "noise_window_s = 100000",
            "noise_window_s",
        );
        expect_reject(
            "trailing_silence_ms = 700",
            "trailing_silence_ms = 9223372036854775807",
            "trailing_silence_ms",
        );
        expect_reject(
            "arbitration_window_ms = 250",
            "arbitration_window_ms = 9223372036854775807",
            "arbitration_window_ms",
        );
        // The ceiling is a ceiling, not a scare: the largest accepted window still
        // validates, and one second past it does not.
        let mut cfg = valid();
        cfg.capture.hard_cap_s = MAX_CAPTURE_WINDOW_S;
        assert!(cfg.validate().is_ok(), "{:?}", cfg.validate());
        cfg.capture.hard_cap_s = MAX_CAPTURE_WINDOW_S + 1;
        assert!(cfg
            .validate()
            .unwrap_err()
            .to_string()
            .contains("hard_cap_s"));
    }

    /// What `ha::socket_parts` refuses at connect must be a load-time error: an
    /// operator who copies the `https://` HA URL today gets a green `health` and a
    /// failed call on every intent.
    #[test]
    fn rejects_urls_the_ha_client_would_refuse() {
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"ha.example.lan:8123\"",
            "must start with ws://",
        );
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"https://ha.example.lan:8123\"",
            "TLS",
        );
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"wss://ha.example.lan:8123\"",
            "TLS",
        );
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"mqtt://ha.example.lan\"",
            "unsupported scheme",
        );
        expect_reject(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"ws://\"",
            "no host",
        );
        // `http://` is the alias ha.rs accepts, so validation accepts it too.
        let text = VALID.replace(
            "url = \"ws://ha.example.lan:8123\"",
            "url = \"http://ha.example.lan:8123\"",
        );
        assert!(Config::from_toml_str(&text).is_ok());
    }

    #[test]
    fn rejects_bad_clips_settings() {
        expect_reject(
            "dir = \"/var/lib/listenrr/clips\"",
            "dir = \"\"",
            "clips.dir",
        );
        // A clips dir with no parent is the whole filesystem (or everything under
        // the working directory): the retention sweep would scan — and prune — it.
        expect_reject("dir = \"/var/lib/listenrr/clips\"", "dir = \"/\"", "clips.dir");
        expect_reject("dir = \"/var/lib/listenrr/clips\"", "dir = \".\"", "clips.dir");
        // Whitespace is blank, not a path.
        expect_reject(
            "dir = \"/var/lib/listenrr/clips\"",
            "dir = \"  \"",
            "clips.dir",
        );
        expect_reject(
            "retention_days = 30",
            "retention_days = 0",
            "retention_days",
        );
    }

    #[test]
    fn rejects_duplicate_and_blank_source_identity() {
        expect_reject("id = \"hallway\"", "id = \"kitchen\"", "is duplicated");
        expect_reject("id = \"hallway\"", "id = \"  \"", "must not be empty");
        expect_reject("room = \"hallway\"", "room = \"\"", "sources[1].room");
        let mut cfg = valid();
        cfg.sources[1].group = "   ".into();
        let msg = cfg.validate().unwrap_err().to_string();
        assert!(msg.contains("group"), "{msg}");
    }

    #[test]
    fn group_references_resolve_only_when_groups_are_declared() {
        // Documented choice: with [[groups]] present, every source.group must exist.
        let text = VALID.replace(
            "id = \"hallway\"\nroom = \"hallway\"\ngroup = \"kitchen\"",
            "id = \"hallway\"\nroom = \"hallway\"\ngroup = \"attic\"",
        );
        let err = Config::from_toml_str(&text).err().unwrap().to_string();
        assert!(err.contains("undeclared group"), "{err}");

        // Without [[groups]] the same file is a valid single-room config.
        let no_groups = text.replace(
            "[[groups]]\nname = \"kitchen\"\noutput_entity = \"media_player.kitchen_speaker\"\noutput_device_id = \"aa:bb:cc:dd:ee:ff\"\n\n",
            "",
        );
        let cfg = Config::from_toml_str(&no_groups).expect("implicit groups must validate");
        assert!(cfg.groups.is_empty());
        assert_eq!(cfg.source("hallway").unwrap().group, "attic");
    }

    #[test]
    fn rejects_duplicate_group_names() {
        let text = VALID.replace(
            "output_device_id = \"aa:bb:cc:dd:ee:ff\"\n\n[[sources]]",
            "output_device_id = \"aa:bb:cc:dd:ee:ff\"\n\n[[groups]]\nname = \"kitchen\"\n\n[[sources]]",
        );
        let err = Config::from_toml_str(&text).err().unwrap().to_string();
        assert!(err.contains("duplicated"), "{err}");
    }

    #[test]
    fn reports_all_problems_at_once() {
        let text = VALID
            .replace("token = \"ha-long-lived-token\"", "token = \"\"")
            .replace("noise_floor_ms = 250", "noise_floor_ms = 9000")
            .replace("id = \"hallway\"", "id = \"kitchen\"");
        let err = Config::from_toml_str(&text).err().unwrap().to_string();
        assert!(err.contains("ha.token"), "{err}");
        assert!(err.contains("noise_floor_ms"), "{err}");
        assert!(err.contains("duplicated"), "{err}");
    }

    #[test]
    fn malformed_toml_is_an_error_not_a_panic() {
        assert!(Config::parse_toml_str("this is not toml [[[").is_err());
        // Unknown keys are tolerated (forward-compatible config); a missing required key
        // is not.
        assert!(Config::parse_toml_str("[server]\nlisten = \"0.0.0.0:1\"\nextra = 1\n").is_err());
    }

    #[test]
    fn host_port_parser_edges() {
        assert_eq!(parse_host_port("0.0.0.0:10700").unwrap().1, 10700);
        assert_eq!(
            parse_host_port("ha.example.lan:1").unwrap().0,
            "ha.example.lan"
        );
        assert_eq!(parse_host_port("[::1]:10700").unwrap().0, "::1");
        assert_eq!(parse_host_port("  127.0.0.1 : 10700 ").unwrap().1, 10700);
        // Surrounding space is trimmed, not part of the host.
        assert_eq!(
            parse_host_port("  127.0.0.1 : 10700 ").unwrap().0,
            "127.0.0.1"
        );
        // URL syntax is not a host: these used to validate and then failed at bind
        // or connect — `ws://ha.example.lan:1` splits into host `ws://ha.example.lan` + port `1`.
        assert!(parse_host_port("ws://ha.example.lan:10700").is_none());
        assert!(parse_host_port("http://ha.example.lan:10700").is_none());
        assert!(parse_host_port("ha.example.lan/path:10700").is_none());
        assert!(parse_host_port("ha.example.lan?x:10700").is_none());
        assert!(parse_host_port("ha.example.lan#x:10700").is_none());
        assert!(parse_host_port("fro st:10700").is_none());
        assert!(parse_host_port("").is_none());
        assert!(parse_host_port("no-colon").is_none());
        assert!(parse_host_port(":10700").is_none());
        assert!(parse_host_port("h:").is_none());
        assert!(parse_host_port("h:abc").is_none());
        assert!(parse_host_port("h:65536").is_none());
        assert!(parse_host_port("[::1]10700").is_none());
    }
}
