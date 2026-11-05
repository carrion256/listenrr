//! The coordinator: the only stateful component (plan S4.4, S5, S6).
//!
//! One session per group (plan invariant). The first [`WakeEvent`] in an idle group
//! opens the `capture.arbitration_window_ms` (250 ms) collection window; candidates are
//! compared with [`snr`] and the highest ratio wins (which cancels mic-gain
//! differences); losers are suppressed for the session. Transitions (S4.4):
//!
//! ```text
//! Idle → Capturing (optimistic LED + earcon) → Processing → Speaking (group KWS
//!         suppressed) → (playback_done | speaking_timeout_s) → Idle
//! verify-gate fail → LED cancel, Idle, false-accept clip
//! ```
//!
//! Follow-up (S6.1): `playback_done` → `FollowUp { until = now + followup_s }` and a
//! `CoordOut::Endpoint` with no `kw_span` starts a listener on the winner (live VAD, no
//! keyword). `FollowUpSpeech` (first speech) stretches the window to `hard_cap_s`; the
//! listener's endpoint (`EndpointReached`) hands the turn to ASR under the same conversation
//! id and the session's deadline becomes the processing guard until playback opens a new
//! window; expiry → `Idle` + LED idle. A wake inside the window starts a fresh session
//! (S6.1); a Speaking session suppresses the whole group's KWS (TTS self-trigger).
//!
//! Stop (stop-command plan 4.4/4.5): a stop detection (`CoordIn::Stop`) from a member of a
//! `Processing` or `Speaking` group ends that session at once — `StopPlayback` to the winner
//! first when a reply is playing — and every transition that changes the derived stop-listen
//! set (members of Processing/Speaking groups, minus muted) publishes `StopListenChanged`,
//! always before `SuppressionChanged`; a verified turn text that is exactly a configured
//! stop phrase (`capture.stop_phrases`, after normalisation) ends the session without
//! entering the pipeline.
//!
//! **Testability**: [`Coordinator::handle`] is synchronous and takes its decisions from an
//! injected [`Clock`], so the transition-table and arbitration-matrix tests (AC1) need no
//! tokio and no threads. Everything the coordinator cannot do itself is returned as a
//! [`CoordOut`] for the wiring layer to execute.
//!
//! LANE-OWNED SEAM (Lane A, S4.4/S5/S6). Public signatures are frozen — implement the
//! bodies; add only private items. Report a mismatch instead of editing. The one
//! authorised exception is the stop-command plan's §5 A4 amendment: `CoordIn::Stop`,
//! `CoordOut::StopListenChanged` and `CoordOut::StopPlayback` are owner-approved additions.

use std::collections::{HashMap, HashSet};
use std::ops::Range;
use std::time::{Duration, Instant};

use crate::capture::VadEngine;
use crate::config::Config;
use crate::ingest::IngestEvent;
use crate::kws::WakeEvent;
use crate::ms_to_samples;
use crate::proto::{self, Control};

/// Session lifecycle boundaries that must each be logged (S4.5, AC5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LatencyStage {
    /// Wake detected → utterance endpoint.
    WakeToEndpoint,
    /// Endpoint → transcript available.
    EndpointToTranscript,
    /// Transcript → HA tts-end seen.
    TranscriptToTtsEnd,
    /// tts-end → Home Assistant accepting `play_media` for the satellite: the closest the
    /// backend can observe to playback starting (the true start is a firmware timestamp, AC5).
    TtsEndToPlaybackStart,
    /// tts-end → the satellite's own `playback_done`. This is playback latency **plus the
    /// length of the clip**, so it is a duration, not a latency; it used to be logged under
    /// the Start name, which made every reply look 20 s slow.
    TtsEndToPlaybackDone,
}

/// Session state for one group (plan S4.4).
#[derive(Debug, Clone, PartialEq)]
pub enum Session {
    /// Nothing in flight; the next wake opens a collection window.
    Idle,
    /// A winner is capturing. `start` is the utterance start in the winner's sample
    /// domain.
    Capturing {
        /// Winning source.
        winner: String,
        /// Sample-index capture start.
        start: u64,
    },
    /// Transcript/verify/pipeline in flight for `conversation_id`.
    Processing {
        /// Winning source.
        winner: String,
        /// Assist conversation id, reused by follow-up turns (AC7).
        conversation_id: String,
    },
    /// TTS playing on the winner; ends on `playback_done` or `speaking_timeout_s`.
    Speaking {
        /// Winning source.
        winner: String,
        /// Conversation being answered.
        conversation_id: String,
        /// Monotonic deadline after which the session is abandoned (S4.4).
        until: Instant,
    },
    /// Window open for a wake-free follow-up turn (S6.1).
    FollowUp {
        /// Source allowed to speak without a keyword.
        winner: String,
        /// Same conversation as the answered turn (AC7).
        conversation_id: String,
        /// Monotonic deadline after which the group returns to Idle.
        until: Instant,
    },
}

/// A monotonic clock, injectable so transition tests are deterministic (plan §3:
/// `Instant` for cross-source comparison and latency only).
pub trait Clock: Send {
    /// Current monotonic instant.
    fn now(&self) -> Instant;
}

/// The real monotonic clock, used by `run`.
#[derive(Debug, Clone, Copy, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }
}

/// Everything the coordinator reacts to (plan S4.4/S5/S6 transition table).
#[derive(Debug, Clone, PartialEq)]
pub enum CoordIn {
    /// A wake candidate from the KWS thread (S5.1).
    Wake(WakeEvent),
    /// A protocol event from ingest (S4.4/S4.5/S8.2).
    Ingest(IngestEvent),
    /// Capture finished endpointing an utterance (S3.2).
    EndpointReached {
        /// Source that spoke.
        source: String,
        /// Utterance start in that source's sample domain.
        utterance_start: u64,
        /// Endpoint (one past last speech) in the same domain.
        endpoint: u64,
    },
    /// The follow-up listener (S6.1) heard speech begin on the winner, so the window must not
    /// close under it. Sent at most once per follow-up capture.
    FollowUpSpeech {
        /// Source that started speaking.
        source: String,
    },
    /// A transcript arrived for a captured utterance (S3.3), before the verify gate.
    Transcript {
        /// Source that spoke.
        source: String,
        /// Conversation this belongs to.
        conversation_id: String,
        /// Raw transcript text.
        text: String,
    },
    /// The verify gate accepted the transcript (S4.2) — only now may HA be called.
    Verified {
        /// Source that spoke.
        source: String,
        /// Conversation this belongs to.
        conversation_id: String,
        /// Stripped text to hand the pipeline.
        text: String,
    },
    /// The verify gate rejected it: LED cancel, Idle, false-accept clip (S4.2).
    VerifyRejected {
        /// Source that spoke.
        source: String,
        /// Conversation this rejection belongs to. Every other completion in this enum names
        /// the turn it completes, and a rejection needs it for the same reason: the verifier
        /// answers after a round trip, and by then the group may be serving a different turn.
        /// Without it, a late rejection can only be attributed by `source` — which is a
        /// different turn's name for the same thing.
        conversation_id: String,
        /// Rejected transcript, for the clip sidecar.
        transcript: String,
    },
    /// The HA pipeline finished; carries the TTS url if there is one (S4.3).
    PipelineFinished {
        /// Source that spoke.
        source: String,
        /// Conversation that was run.
        conversation_id: String,
        /// `tts-end` url, if HA produced one.
        tts_url: Option<String>,
        /// `tts-end` mime_type, forwarded so playback names the real format (R-C).
        mime_type: Option<String>,
    },
    /// A mute entity changed (S4.5) — suppress or unsuppress that source.
    MuteChanged {
        /// Configured `mute_entity`.
        entity: String,
        /// New muted state.
        muted: bool,
    },
    /// A stop-phrase detection from the stop-only spotter (stop-command plan 4.4): ends
    /// a `Processing` or `Speaking` session at once.
    Stop(crate::kws::StopEvent),
    /// Periodic pump: expires Speaking/FollowUp and closes stalled collection windows.
    Tick,
}

/// An action the wiring layer must execute on the coordinator's behalf. The coordinator
/// performs no I/O itself, which is what keeps its tests synchronous.
#[derive(Debug, Clone, PartialEq)]
pub enum CoordOut {
    /// Send a control message to one source (LED, earcon) (S4.4).
    Control {
        /// Target source.
        source: String,
        /// The control to encode with [`crate::proto::encode_control`].
        control: Control,
    },
    /// Start/stop the live forward endpointer for a source (S3.2, S6.1).
    Endpoint {
        /// Source to endpoint.
        source: String,
        /// `Some` for a wake turn (keyword span known), `None` for a follow-up turn
        /// (300 ms preroll only, S6.1).
        kw_span: Option<Range<u64>>,
        /// Conversation this utterance belongs to.
        conversation_id: String,
    },
    /// Transcribe a captured span (S3.3).
    Transcribe {
        /// Source that spoke.
        source: String,
        /// Conversation being served.
        conversation_id: String,
        /// Utterance start, sample index.
        utterance_start: u64,
        /// Endpoint, sample index (one past last speech).
        endpoint: u64,
    },
    /// Run the verify gate before HA is touched (S4.2, plan invariant).
    Verify {
        /// Source that spoke.
        source: String,
        /// Conversation being served.
        conversation_id: String,
        /// Transcript to check.
        transcript: String,
        /// `false` for a follow-up turn, which has no keyword to require (S6.1).
        wake_initiated: bool,
    },
    /// Enter the Assist pipeline at the intent stage (S4.3).
    RunPipeline {
        /// Source that spoke.
        source: String,
        /// Conversation id, reused across follow-up turns (AC7).
        conversation_id: String,
        /// Verified, stripped text.
        text: String,
    },
    /// Play TTS on the winner's `media_player` (S4.3).
    PlayMedia {
        /// Source to play on.
        source: String,
        /// Resolved `media_player` entity (source `ha_entity`, else group fallback).
        entity_id: String,
        /// TTS url from `tts-end`.
        url: String,
        /// Conversation being answered.
        conversation_id: String,
        /// `tts-end` mime_type from [`crate::ha::PipelineOutcome`]; `None` leaves
        /// `play_media`'s fallback (R-C).
        mime_type: Option<String>,
    },
    /// Stop the reply playing on the session winner (stop-command plan 4.4). Emitted
    /// only for a `Speaking` session, before it ends. `entity` is resolved exactly like
    /// [`CoordOut::PlayMedia`]'s: empty means "nowhere to stop".
    StopPlayback {
        /// Source whose playback stops (the session winner, not necessarily the member
        /// that said the phrase).
        source: String,
        /// Resolved `media_player` entity (source `ha_entity`, else group fallback).
        entity: String,
    },
    /// Write a false-accept clip + sidecar (S4.2, S8.1).
    FalseAcceptClip {
        /// Source to clip.
        source: String,
        /// Rejected transcript for the sidecar.
        transcript: String,
    },
    /// Write the last 10 s of a source to `clips/missed/` (S8.2, AC11).
    MissedWakeClip {
        /// Source whose `save_buffer` fired.
        source: String,
    },
    /// Publish the KWS suppression set (whole group while Speaking, S5.2; muted sources,
    /// S4.5).
    SuppressionChanged {
        /// Sources whose frames must not reach the spotter.
        suppressed: Vec<String>,
    },
    /// The derived stop-listen set (stop-command plan 4.2): which sources' frames feed
    /// the stop spotter — members of `Processing`/`Speaking` groups, minus muted.
    /// Published on every transition that changes it, always before
    /// [`CoordOut::SuppressionChanged`].
    StopListenChanged {
        /// Sources the stop engine listens to.
        sources: Vec<String>,
    },
    /// One structured latency line (AC5's stage list).
    Latency {
        /// Which stage boundary this measures.
        stage: LatencyStage,
        /// Conversation the measurement belongs to.
        conversation_id: String,
        /// Elapsed milliseconds.
        ms: u64,
    },
}

/// Owns every group's [`Session`], the candidate collection for the arbitration window,
/// and the suppression sets (plan §3: one session per group, everything else stateless).
pub struct Coordinator {
    cfg: Config,
    clock: Box<dyn Clock>,
    sessions: HashMap<String, Session>,
    candidates: HashMap<String, Vec<WakeEvent>>,
    suppressed: HashSet<String>,
    muted: HashSet<String>,
    /// Per-group deadline closing the `capture.arbitration_window_ms` collection window.
    windows: HashMap<String, Instant>,
    /// The conversation each busy group is serving, reused by follow-up turns (AC7).
    conversations: HashMap<String, String>,
    /// Groups whose *current* turn came from a follow-up window: it has no keyword to
    /// require, so the verify gate runs with `wake_initiated: false` (S6.1).
    followup_turns: HashSet<String>,
    /// Groups whose winner disconnected **while its turn was still owed work** — a capture
    /// that has not yet delivered its endpoint, or a transcription already in flight. The
    /// audio that connection delivered is still worth acting on, so the session survives the
    /// departure; this is what records that the satellite itself is not there to be played
    /// back to. Cleared with the rest of the turn state by [`Self::end_session`], and never
    /// consulted for admission or presence.
    gone: HashSet<String>,
    /// Stage boundaries for the latency lines (S4.5, AC5).
    woke_at: HashMap<String, Instant>,
    endpointed_at: HashMap<String, Instant>,
    transcribed_at: HashMap<String, Instant>,
    tts_at: HashMap<String, Instant>,
    conversations_issued: u64,
    /// What [`Coordinator::session`] returns for a group it has never seen: `session()`
    /// borrows `&self`, so the documented "unknown groups are Idle" answer needs a value
    /// that outlives the call.
    idle: Session,
}

impl Coordinator {
    /// A coordinator for every group named in `cfg`, all Idle, driven by `clock`.
    pub fn new(cfg: &Config, clock: Box<dyn Clock>) -> Self {
        let mut sessions: HashMap<String, Session> = HashMap::new();
        for group in &cfg.groups {
            sessions.insert(group.name.clone(), Session::Idle);
        }
        // Sources may name a group the config never declared (the plan allows that when
        // there is no `[[groups]]` table); they still get exactly one session.
        for source in &cfg.sources {
            sessions.insert(source.group.clone(), Session::Idle);
        }
        Self {
            cfg: cfg.clone(),
            clock,
            sessions,
            candidates: HashMap::new(),
            suppressed: HashSet::new(),
            muted: HashSet::new(),
            windows: HashMap::new(),
            conversations: HashMap::new(),
            followup_turns: HashSet::new(),
            gone: HashSet::new(),
            woke_at: HashMap::new(),
            endpointed_at: HashMap::new(),
            transcribed_at: HashMap::new(),
            tts_at: HashMap::new(),
            conversations_issued: 0,
            idle: Session::Idle,
        }
    }

    /// Current state of a group (debug, tests, the metric line). Unknown groups are
    /// `Idle`, never a panic.
    pub fn session(&self, group: &str) -> &Session {
        self.sessions.get(group).unwrap_or(&self.idle)
    }

    /// Apply one input and return the actions to perform — the whole transition table
    /// (S4.4, S5.1, S5.2, S6.1).
    pub fn handle(&mut self, input: CoordIn) -> Vec<CoordOut> {
        match input {
            CoordIn::Wake(wake) => self.on_wake(wake),
            CoordIn::EndpointReached {
                source,
                utterance_start,
                endpoint,
            } => self.on_endpoint_reached(&source, utterance_start, endpoint),
            CoordIn::FollowUpSpeech { source } => self.on_followup_speech(&source),
            CoordIn::Transcript {
                source,
                conversation_id,
                text,
            } => self.on_transcript(&source, &conversation_id, text),
            CoordIn::Verified {
                source,
                conversation_id,
                text,
            } => self.on_verified(&source, &conversation_id, text),
            CoordIn::VerifyRejected {
                source,
                conversation_id,
                transcript,
            } => self.on_verify_rejected(&source, &conversation_id, transcript),
            CoordIn::PipelineFinished {
                source,
                conversation_id,
                tts_url,
                mime_type,
            } => self.on_pipeline_finished(&source, &conversation_id, tts_url, mime_type),
            CoordIn::Ingest(event) => self.on_ingest(event),
            CoordIn::MuteChanged { entity, muted } => self.on_mute_changed(&entity, muted),
            CoordIn::Stop(event) => self.on_stop(&event.source),
            CoordIn::Tick => self.on_tick(),
        }
    }

    /// True when `source`'s frames must not reach the spotter (muted, or its group is
    /// Speaking, or it lost arbitration) (S4.5, S5.2).
    pub fn is_suppressed(&self, source: &str) -> bool {
        self.muted.contains(source) || self.suppressed.contains(source)
    }

    /// The current suppression set, for the `watch` channel feeding ingest and KWS.
    pub fn suppressed(&self) -> Vec<String> {
        let mut set: Vec<String> = self
            .muted
            .iter()
            .chain(self.suppressed.iter())
            .cloned()
            .collect();
        set.sort();
        set.dedup();
        set
    }

    /// The derived stop-listen set (stop-command plan 4.1): every member of a group whose
    /// session is `Processing` or `Speaking`, minus muted sources. Capturing and FollowUp
    /// contribute nothing (BC2): there the transcript path carries a spoken stop. Derived
    /// from the sessions on every call and never stored, so it cannot drift from them.
    fn stop_listen(&self) -> Vec<String> {
        let mut sources: Vec<String> = self
            .sessions
            .iter()
            .filter(|(_, session)| {
                matches!(
                    session,
                    Session::Processing { .. } | Session::Speaking { .. }
                )
            })
            .flat_map(|(group, _)| self.members(group))
            .filter(|source| !self.muted.contains(source))
            .collect();
        sources.sort();
        sources.dedup();
        sources
    }

    /// The publication pair (stop-command plan 4.2): the stop-listen set always before the
    /// suppression set, so a mute starves the stop engine before the suppression watch
    /// even moves. The growth direction has no privacy cost.
    fn set_outs(&self) -> Vec<CoordOut> {
        vec![
            CoordOut::StopListenChanged {
                sources: self.stop_listen(),
            },
            CoordOut::SuppressionChanged {
                suppressed: self.suppressed(),
            },
        ]
    }

    /// Return a group to Idle and clear its suppression (used by the wiring layer after
    /// playback completes, and by tests that need to reset a group).
    pub fn release(&mut self, group: &str) {
        self.end_session(group);
    }

    // ── transition table ────────────────────────────────────────────────────────

    /// S5.1: the first wake in an idle group opens the collection window and wins
    /// optimistically (LED + earcon before anything is verified); later candidates in the
    /// window are recorded for the sidecar and suppressed for the session (S5.2).
    fn on_wake(&mut self, wake: WakeEvent) -> Vec<CoordOut> {
        if self.is_suppressed(&wake.source) {
            tracing::debug!(
                source = %wake.source,
                "wake from a suppressed source: ignored"
            );
            return Vec::new();
        }
        let group = self.group_of(&wake.source);
        let now = self.clock.now();
        match self.session(&group).clone() {
            // S6.1: an open follow-up window is not a busy group. A wake word inside it
            // starts a new session from scratch — new winner, new conversation, new
            // arbitration window — exactly as if the group had been idle.
            Session::Idle | Session::FollowUp { .. } => {
                let conversation_id = self.new_conversation(&group);
                self.sessions.insert(
                    group.clone(),
                    Session::Capturing {
                        winner: wake.source.clone(),
                        start: wake.kw_span.start,
                    },
                );
                self.candidates.insert(group.clone(), vec![wake.clone()]);
                self.windows
                    .insert(group.clone(), now + arbitration_window(&self.cfg));
                self.woke_at.insert(group.clone(), wake.arrived);
                self.followup_turns.remove(&group);
                // A new turn is a new turn: whatever the previous winner left behind —
                // including the fact that it disconnected mid-turn — describes a finished
                // conversation, not this one.
                self.gone.remove(&group);
                tracing::info!(
                    source = %wake.source,
                    conversation_id = %conversation_id,
                    kw_span = ?wake.kw_span,
                    "wake accepted: capturing"
                );
                vec![
                    // Optimistic: the satellite acknowledges before anything is verified
                    // (S4.4); the verify gate's failure path cancels it.
                    CoordOut::Control {
                        source: wake.source.clone(),
                        control: Control::Led {
                            state: proto::LedState::Listening,
                        },
                    },
                    CoordOut::Control {
                        source: wake.source.clone(),
                        control: Control::Earcon {
                            name: EARCON_ACK.to_string(),
                        },
                    },
                    CoordOut::Endpoint {
                        source: wake.source.clone(),
                        kw_span: Some(wake.kw_span),
                        conversation_id,
                    },
                ]
            }
            Session::Capturing { winner, start } => {
                let window_open = self
                    .windows
                    .get(&group)
                    .is_some_and(|deadline| now <= *deadline);
                if !window_open {
                    tracing::debug!(
                        source = %wake.source,
                        winner = %winner,
                        "wake after the collection window: ignored (group is busy)"
                    );
                    return Vec::new();
                }
                if wake.source == winner {
                    // The same source firing twice is one wake, not a second candidate.
                    if let Some(candidates) = self.candidates.get_mut(&group) {
                        if !candidates.iter().any(|c| c.kw_span == wake.kw_span) {
                            candidates.push(wake.clone());
                        }
                    }
                    tracing::debug!(source = %wake.source, "duplicate wake from the winner");
                    return Vec::new();
                }
                // A second satellite heard the same phrase: it is a candidate for the
                // sidecar (S8.1) and suppressed for this session (S5.2).
                self.candidates
                    .entry(group.clone())
                    .or_default()
                    .push(wake.clone());
                let changed = self.suppressed.insert(wake.source.clone());
                tracing::info!(
                    candidate = %wake.source,
                    winner = %winner,
                    group = %group,
                    "arbitration: candidate suppressed for the session"
                );
                let _ = start;
                if changed {
                    vec![CoordOut::SuppressionChanged {
                        suppressed: self.suppressed(),
                    }]
                } else {
                    Vec::new()
                }
            }
            busy => {
                tracing::debug!(
                    source = %wake.source,
                    group = %group,
                    ?busy,
                    "wake in a busy group: ignored (S5.2)"
                );
                Vec::new()
            }
        }
    }

    /// S4.4: the winner's endpoint ends capture and starts the transcript; in `FollowUp`
    /// the same signal is the onset of the follow-up turn (S6.1).
    fn on_endpoint_reached(
        &mut self,
        source: &str,
        utterance_start: u64,
        endpoint: u64,
    ) -> Vec<CoordOut> {
        let group = self.group_of(source);
        let now = self.clock.now();
        // Every state matched below is a turn that was opened under a conversation id, and
        // this message is the completion of a capture `CoordOut::Endpoint` started under that
        // same id. A group with no conversation has no turn to complete: the endpoint belongs
        // to one that already ended, and crediting it to whatever state the group is in now
        // is the same class of mistake as re-resolving a ring by `source_id` — an artifact of
        // a finished turn, attributed to the live one.
        let Some(conversation_id) = self.conversations.get(&group).cloned() else {
            tracing::debug!(
                source = %source,
                group = %group,
                "endpoint for a group with no turn in flight: dropped"
            );
            return Vec::new();
        };
        match self.session(&group).clone() {
            Session::Capturing { winner, start } if winner == source => {
                // C-5, defense in depth: `EndpointReached` carries no conversation id,
                // so the endpoint a capture that already timed out delivers late is
                // attributed to whatever turn is live now. Its own arithmetic is what
                // gives it away — an endpoint at or before this capture's start is a
                // reversed or empty range, and transcribing that reads zero samples and
                // strands the live turn in `Processing`. Refuse it and stay `Capturing`:
                // this capture is still owed its own endpoint.
                if endpoint <= start {
                    tracing::debug!(
                        source = %source,
                        group = %group,
                        capture_start = start,
                        endpoint,
                        "endpoint not after the capture's start: from a dead turn, dropped"
                    );
                    return Vec::new();
                }
                let ms = Self::elapsed(self.woke_at.get(&group).copied(), now);
                self.sessions.insert(
                    group.clone(),
                    Session::Processing {
                        winner: winner.clone(),
                        conversation_id: conversation_id.clone(),
                    },
                );
                self.endpointed_at.insert(group.clone(), now);
                self.windows.remove(&group);
                let mut out = Vec::new();
                if let Some(ms) = ms {
                    out.push(CoordOut::Latency {
                        stage: LatencyStage::WakeToEndpoint,
                        conversation_id: conversation_id.clone(),
                        ms,
                    });
                }
                out.push(CoordOut::Transcribe {
                    source: source.to_string(),
                    conversation_id,
                    // The earlier of the keyword's start and the start of the speech segment
                    // it sits in (S3.1). The segment start is what lets a command spoken
                    // *before* the keyword reach the ASR ("what's the weather, Luna?"); the
                    // keyword start is the floor that keeps the keyword itself in the input
                    // when the backward pass lands after it. This used to be `max`, which
                    // clipped the request to begin at the keyword and made everything said
                    // before it unreachable.
                    utterance_start: if self.followup_turns.contains(&group) {
                        utterance_start
                    } else {
                        start.min(utterance_start)
                    },
                    endpoint,
                });
                // Plan 4.3(i): entering Processing puts this group's members into the
                // derived stop-listen set, so it is published here. There is no
                // suppression counterpart: Processing never suppresses.
                out.push(CoordOut::StopListenChanged {
                    sources: self.stop_listen(),
                });
                out
            }
            Session::FollowUp {
                winner,
                conversation_id,
                ..
            } if winner == source => {
                // S6.1: onset on the winner, no keyword required. The session stays
                // `FollowUp` — the frozen `Session` enum has no separate "follow-up in
                // flight" state — but its deadline is now the processing guard, not the
                // follow-up window. This turn still has ASR, the verify gate, the HA pipeline
                // and TTS ahead of it, which routinely outlast `followup_s`; re-arming to the
                // window killed the session mid-turn and dropped the answer. If the turn
                // finishes, playback opens a fresh window; if a step never answers, this is
                // the same bound a wake-initiated `Processing` turn has (C-2).
                self.sessions.insert(
                    group.clone(),
                    Session::FollowUp {
                        winner: winner.clone(),
                        conversation_id: conversation_id.clone(),
                        until: now + PROCESSING_TIMEOUT,
                    },
                );
                self.followup_turns.insert(group.clone());
                self.endpointed_at.insert(group, now);
                tracing::info!(source = %source, "follow-up onset: capturing");
                vec![CoordOut::Transcribe {
                    source: source.to_string(),
                    conversation_id,
                    utterance_start,
                    endpoint,
                }]
            }
            other => {
                tracing::debug!(
                    source = %source,
                    group = %group,
                    ?other,
                    "endpoint from a source that is not capturing: ignored"
                );
                Vec::new()
            }
        }
    }

    /// S3.3 → S4.2: a transcript is never handed to HA directly, it goes through the
    /// verify gate first (plan invariant).
    ///
    /// Acts only for the turn that is live: ASR answers over a network, and by then the group
    /// may have timed out, been reset, or started a new turn. Verifying a transcript that
    /// belongs to none of them would put a dead utterance on the path to HA.
    fn on_transcript(
        &mut self,
        source: &str,
        conversation_id: &str,
        text: String,
    ) -> Vec<CoordOut> {
        let group = self.group_of(source);
        if self
            .serve_turn(&group, source, conversation_id, "transcript")
            .is_none()
        {
            return Vec::new();
        }
        let now = self.clock.now();
        let ms = Self::elapsed(self.endpointed_at.get(&group).copied(), now);
        self.transcribed_at.insert(group.clone(), now);
        let wake_initiated = !self.followup_turns.contains(&group);
        let mut out = Vec::new();
        if let Some(ms) = ms {
            out.push(CoordOut::Latency {
                stage: LatencyStage::EndpointToTranscript,
                conversation_id: conversation_id.to_string(),
                ms,
            });
        }
        out.push(CoordOut::Verify {
            source: source.to_string(),
            conversation_id: conversation_id.to_string(),
            transcript: text,
            wake_initiated,
        });
        out
    }

    /// S4.2 passed: only now may the Assist pipeline be entered.
    ///
    /// Guarded the same way as [`Self::on_transcript`]: the verify gate answers after its own
    /// round trip, and an unguarded version here enters HA for an utterance whose session has
    /// already been abandoned — the assistant then answers a request nobody is waiting for.
    ///
    /// A turn whose entire content was the keyword is addressed to us and asks nothing, and AC1
    /// deliberately passes those forms with an empty command. That is not a reason to enter the
    /// pipeline with `""`: an empty `input.text` reaches the conversation agent as an empty user
    /// message, and this household's OpenAI agent answers it — measured 2026-11-02 in an
    /// `ha: intent-end` line — with "Sorry, I had a problem talking to OpenAI: Error code: 400
    /// … No user message found in messages". Saying the wake word alone made the house
    /// apologise. So an empty request ends the session cleanly instead, minus the false-accept
    /// clip: the wake word really was said, and filing it under `clips/false_accept/` would
    /// corrupt the false-accept rate the keyword calibration is measured on.
    fn on_verified(&mut self, source: &str, conversation_id: &str, text: String) -> Vec<CoordOut> {
        let group = self.group_of(source);
        if self
            .serve_turn(&group, source, conversation_id, "verified transcript")
            .is_none()
        {
            return Vec::new();
        }
        if text.trim().is_empty() {
            tracing::info!(
                source = %source,
                conversation_id = %conversation_id,
                "verified turn asked nothing; not entering the pipeline",
            );
            return self.end_session(&group);
        }
        // R2/plan 4.5: the turn text itself being exactly a configured stop phrase ends
        // the session without entering the pipeline. Config validation keeps the list in
        // normalised form, so both sides of the comparison are normalised; an exact match
        // only — "stop the music in the kitchen" and "stopwatch" are commands, not stops.
        let normalised = crate::config::normalise_phrase(&text);
        if self.cfg.capture.stop_phrases.contains(&normalised) {
            tracing::info!(
                source = %source,
                conversation_id = %conversation_id,
                "stop phrase: ending the session without the pipeline",
            );
            return self.end_session(&group);
        }
        vec![CoordOut::RunPipeline {
            source: source.to_string(),
            conversation_id: conversation_id.to_string(),
            text,
        }]
    }

    /// S4.2 failed: LED cancel, Idle, and the evidence for `clips/false_accept/`.
    ///
    /// The rejection names the conversation it belongs to for exactly this reason: ending a
    /// session by `source` alone let a late rejection from a turn that had already been
    /// abandoned tear down the *next* turn the group had started, taking its LED and its
    /// capture with it.
    fn on_verify_rejected(
        &mut self,
        source: &str,
        conversation_id: &str,
        transcript: String,
    ) -> Vec<CoordOut> {
        let group = self.group_of(source);
        if self
            .serve_turn(&group, source, conversation_id, "verify rejection")
            .is_none()
        {
            return Vec::new();
        }
        let mut out = self.end_session(&group);
        out.push(CoordOut::FalseAcceptClip {
            source: source.to_string(),
            transcript,
        });
        out
    }

    /// S4.3/S4.4: with a `tts-end` url the group enters `Speaking` (and the whole group
    /// is KWS-suppressed, S5.2); without one there is nothing to play and the group goes
    /// straight back to Idle.
    ///
    /// Two things must be true before any of that happens. The pipeline result has to belong
    /// to the turn this group is actually serving — an unguarded version resurrected an `Idle`
    /// group into `Speaking`, and suppressed its whole group's KWS, on the strength of an
    /// answer to a request that had already been abandoned. And the winner has to still be
    /// there: a turn whose satellite disconnected mid-transcription is worth finishing (its
    /// audio was already delivered, and the transcript is the record of it), but there is no
    /// endpoint left to play TTS on, and `Speaking` without one spends `speaking_timeout_s`
    /// with the group deaf.
    fn on_pipeline_finished(
        &mut self,
        source: &str,
        conversation_id: &str,
        tts_url: Option<String>,
        mime_type: Option<String>,
    ) -> Vec<CoordOut> {
        let group = self.group_of(source);
        if self
            .serve_turn(&group, source, conversation_id, "pipeline result")
            .is_none()
        {
            return Vec::new();
        }
        let now = self.clock.now();
        let ms = Self::elapsed(self.transcribed_at.get(&group).copied(), now);
        let Some(url) = tts_url else {
            tracing::debug!(source = %source, "pipeline produced no TTS: returning to Idle");
            return self.end_session(&group);
        };
        if self.gone.contains(&group) {
            tracing::debug!(
                source = %source,
                group = %group,
                conversation_id = %conversation_id,
                "pipeline answered a turn whose source has gone: the transcript stands, but \
                 there is no endpoint to play on, so the group returns to Idle"
            );
            return self.end_session(&group);
        }
        let entity_id = self.output_entity(source, &group);
        self.sessions.insert(
            group.clone(),
            Session::Speaking {
                winner: source.to_string(),
                conversation_id: conversation_id.to_string(),
                until: now + Duration::from_secs(self.cfg.capture.speaking_timeout_s),
            },
        );
        self.suppress_group(&group);
        self.tts_at.insert(group.clone(), now);
        let mut out = Vec::new();
        if let Some(ms) = ms {
            out.push(CoordOut::Latency {
                stage: LatencyStage::TranscriptToTtsEnd,
                conversation_id: conversation_id.to_string(),
                ms,
            });
        }
        out.push(CoordOut::Control {
            source: source.to_string(),
            control: Control::Led {
                state: proto::LedState::Speaking,
            },
        });
        out.push(CoordOut::PlayMedia {
            source: source.to_string(),
            entity_id,
            url,
            conversation_id: conversation_id.to_string(),
            mime_type,
        });
        // Plan 4.3(ii): the transition into (or, for a follow-up turn, past) Speaking
        // is where the stop-listen set and the suppression set both move.
        out.extend(self.set_outs());
        out
    }

    fn on_ingest(&mut self, event: IngestEvent) -> Vec<CoordOut> {
        match event {
            IngestEvent::Event { source, event } => match event {
                // S6.1: playback done opens the follow-up window; S4.4's timeout arm is
                // in `on_tick`.
                proto::Event::PlaybackDone => self.on_playback_done(&source),
                // S8.2 / AC11: the satellite's "that was for you" button.
                proto::Event::SaveBuffer => vec![CoordOut::MissedWakeClip { source }],
                // RSSI is ingest's metric to report (S5.3); a generic button has no
                // defined session effect.
                proto::Event::Rssi { .. } | proto::Event::Button { .. } => Vec::new(),
            },
            IngestEvent::SourceGone { source } => {
                let group = self.group_of(&source);
                match self.session(&group).clone() {
                    // A capture this group is still owed an endpoint from, or a transcription
                    // already handed to ASR: both belong to the connection that just went
                    // away, and the audio it delivered is already in *that* connection's ring.
                    // Ending the session here threw it away twice: the endpoint arrived one
                    // poll later to an `Idle` group and was discarded as "not capturing", and
                    // an utterance a satellite had spoken in full was never transcribed. The
                    // turn stands, marked gone, so work already in flight can finish —
                    // `on_pipeline_finished` will not try to play to a satellite that is not
                    // there, and `on_tick` bounds the case where no endpoint ever arrives.
                    Session::Capturing { winner, .. } | Session::Processing { winner, .. }
                        if winner == source =>
                    {
                        tracing::debug!(
                            source = %source,
                            group = %group,
                            "winner disconnected with work in flight: the turn stands, its \
                             audio was already delivered"
                        );
                        self.gone.insert(group);
                        Vec::new()
                    }
                    // Nothing is owed to a disconnected winner here. `Speaking` needs a
                    // satellite that can report `playback_done`, and a `FollowUp` window whose
                    // speaker has walked away is not a window: both end, and both take the
                    // group's suppression down with them.
                    Session::Speaking { winner, .. } | Session::FollowUp { winner, .. }
                        if winner == source =>
                    {
                        tracing::debug!(source = %source, "winner disconnected: session ends");
                        self.end_session(&group)
                    }
                    _ => {
                        // A suppressed candidate that went away must not stay suppressed.
                        let changed = self.suppressed.remove(&source);
                        if changed {
                            vec![CoordOut::SuppressionChanged {
                                suppressed: self.suppressed(),
                            }]
                        } else {
                            Vec::new()
                        }
                    }
                }
            }
        }
    }

    /// S4.4/S6.1: `playback_done` → FollowUp with `capture.followup_s`, and the group's
    /// KWS comes back (the winner has to be able to hear the follow-up).
    fn on_playback_done(&mut self, source: &str) -> Vec<CoordOut> {
        let group = self.group_of(source);
        let now = self.clock.now();
        let Session::Speaking {
            winner,
            conversation_id,
            ..
        } = self.session(&group).clone()
        else {
            tracing::debug!(source = %source, "playback_done outside Speaking: ignored");
            return Vec::new();
        };
        if winner != source {
            tracing::debug!(source = %source, winner = %winner, "playback_done from a source that is not the winner: ignored");
            return Vec::new();
        }
        // The satellite's completion report ends the clip, so this measures tts-end →
        // playback *done* (a duration). Playback start is logged where Home Assistant
        // accepts `play_media`; the true start would be a firmware-side timestamp (AC5).
        let ms = Self::elapsed(self.tts_at.get(&group).copied(), now);
        self.sessions.insert(
            group.clone(),
            Session::FollowUp {
                winner: winner.clone(),
                conversation_id: conversation_id.clone(),
                until: now + Duration::from_secs(self.cfg.capture.followup_s),
            },
        );
        self.unsuppress_group(&group);
        let mut out = Vec::new();
        if let Some(ms) = ms {
            out.push(CoordOut::Latency {
                stage: LatencyStage::TtsEndToPlaybackDone,
                conversation_id: conversation_id.clone(),
                ms,
            });
        }
        // S6.1: the window is only half of the feature. Without a capture watching the
        // winner's ring for speech with no keyword, `FollowUp` simply expired after
        // `followup_s` and anyone who answered Luna was ignored. The capture is keyed by the
        // same conversation id, so the turn it ends is the follow-up of this one (AC7).
        out.push(CoordOut::Endpoint {
            source: winner,
            kw_span: None,
            conversation_id,
        });
        // Plan 4.3(iii): Speaking -> FollowUp takes the group out of both derived sets.
        out.extend(self.set_outs());
        out
    }

    /// S6.1: the follow-up listener heard speech begin on the winner.
    ///
    /// The window is `followup_s` long, but an answer that starts at second 5 is not over at
    /// second 6, and a window that closed under the speaker would discard exactly the people
    /// who took a moment to think. From the first speech the session may run for as long as an
    /// utterance can (`hard_cap_s`); the endpoint then hands the turn on and the processing
    /// guard takes over (see [`Self::on_endpoint_reached`]).
    fn on_followup_speech(&mut self, source: &str) -> Vec<CoordOut> {
        let group = self.group_of(source);
        let Session::FollowUp {
            winner,
            conversation_id,
            ..
        } = self.session(&group).clone()
        else {
            tracing::debug!(source = %source, "follow-up speech outside a FollowUp window: ignored");
            return Vec::new();
        };
        if winner != source {
            tracing::debug!(source = %source, winner = %winner, "follow-up speech from a source that is not the winner: ignored");
            return Vec::new();
        }
        let now = self.clock.now();
        self.sessions.insert(
            group,
            Session::FollowUp {
                winner,
                conversation_id,
                until: now + Duration::from_secs(self.cfg.capture.hard_cap_s),
            },
        );
        tracing::info!(source = %source, "follow-up speech: window extended to the utterance cap");
        Vec::new()
    }

    /// S4.5: a mute entity suppresses exactly its source.
    fn on_mute_changed(&mut self, entity: &str, muted: bool) -> Vec<CoordOut> {
        let sources: Vec<String> = self
            .cfg
            .sources
            .iter()
            .filter(|s| s.mute_entity.as_deref() == Some(entity))
            .map(|s| s.id.clone())
            .collect();
        if sources.is_empty() {
            return Vec::new();
        }
        let mut out = Vec::new();
        for source in sources {
            let changed = if muted {
                self.muted.insert(source.clone())
            } else {
                self.muted.remove(&source)
            };
            if !changed {
                continue;
            }
            if !muted {
                // A source unmuted mid-answer must stay out of the spotter: its group
                // is playing TTS, `suppress_group` skipped it while it was muted, and
                // S5.2's self-trigger protection is exactly about this mic hearing that
                // speaker. Session end (`unsuppress_group`) takes it back down.
                let group = self.group_of(&source);
                if matches!(self.session(&group), Session::Speaking { .. }) {
                    self.suppressed.insert(source.clone());
                }
            }
            out.push(CoordOut::Control {
                source,
                control: Control::Led {
                    state: if muted {
                        proto::LedState::Muted
                    } else {
                        proto::LedState::Idle
                    },
                },
            });
        }
        if !out.is_empty() {
            // The mute just changed membership in any busy group, so the derived
            // stop-listen set moves here too — and moves first (plan 4.2/4.3(v)).
            out.extend(self.set_outs());
        }
        out
    }

    /// Stop (stop-command plan 4.4): a stop detection ends a `Processing` or `Speaking`
    /// session at once. Anything else — `Idle`, `Capturing`, `FollowUp` (BC2: there the
    /// transcript path is the stop path), or a source outside the busy group — is ignored.
    ///
    /// `StopPlayback` goes first and is addressed to the session's **winner**, not to the
    /// member that said the phrase: the winner is the source whose audio must stop, and a
    /// second satellite in the room is allowed to call the stop. Everything else (LED idle
    /// on non-muted members, unsuppression, both set publications) is [`Self::end_session`]'s
    /// per 4.3(iv) — `on_stop` does not call [`Self::set_outs`] itself, so each set is
    /// published exactly once.
    fn on_stop(&mut self, source: &str) -> Vec<CoordOut> {
        let group = self.group_of(source);
        let (winner, was_speaking) = match self.session(&group) {
            Session::Processing { winner, .. } => (winner.clone(), false),
            Session::Speaking { winner, .. } => (winner.clone(), true),
            other => {
                tracing::debug!(
                    source = %source,
                    group = %group,
                    ?other,
                    "stop outside a Processing or Speaking group: ignored"
                );
                return Vec::new();
            }
        };
        if !self.members(&group).iter().any(|m| m == source) {
            tracing::debug!(
                source = %source,
                group = %group,
                winner = %winner,
                "stop from a source outside the busy group: ignored"
            );
            return Vec::new();
        }
        tracing::info!(
            source = %source,
            winner = %winner,
            group = %group,
            speaking = was_speaking,
            "stop heard: ending the session"
        );
        let mut out = Vec::new();
        if was_speaking {
            out.push(CoordOut::StopPlayback {
                source: winner.clone(),
                entity: self.output_entity(&winner, &group),
            });
        }
        out.extend(self.end_session(&group));
        out
    }

    /// The periodic pump: the 250 ms collection window closes, `Speaking` gives up after
    /// `speaking_timeout_s`, `FollowUp` after `followup_s` (S4.4, S6.1).
    fn on_tick(&mut self) -> Vec<CoordOut> {
        let now = self.clock.now();
        let mut out = Vec::new();
        let mut expired = Vec::new();
        for (group, session) in &self.sessions {
            match session {
                Session::Speaking { until, .. } | Session::FollowUp { until, .. } => {
                    if now >= *until {
                        expired.push(group.clone());
                    }
                }
                // B4-1: a capture that never endpoints is bounded by the hard cap. Beyond
                // `capture.hard_cap_s` of audio the utterance cannot exist (the ring is
                // that deep), so a `Capturing` group older than the cap is dead and must
                // not hold its group — and its suppression — forever.
                Session::Capturing { .. } => {
                    if let Some(woke) = self.woke_at.get(group) {
                        if now.duration_since(*woke)
                            >= Duration::from_secs(self.cfg.capture.hard_cap_s)
                        {
                            expired.push(group.clone());
                        }
                    }
                }
                // A turn whose winner disconnected is owed only what it already has: the
                // endpoint its capture is computing, or the transcript ASR is computing from
                // audio that is no longer being produced. One hard cap of that is generous —
                // neither request can outlast the audio it reads — and without it a
                // `Processing` group whose ASR call never returns would hold its group, and
                // its suppression, with no satellite left to answer.
                Session::Processing { .. } if self.gone.contains(group) => {
                    if let Some(endpointed) = self.endpointed_at.get(group) {
                        if now.duration_since(*endpointed)
                            >= Duration::from_secs(self.cfg.capture.hard_cap_s)
                        {
                            expired.push(group.clone());
                        }
                    }
                }
                // C-2: the same hazard with the winner still connected. Every ASR/HA
                // failure path in `main.rs` logs and sends no completion, so a group
                // waiting on a call that will never answer stays `Processing` — deaf,
                // LED up, wakes dropped as busy — until the satellite disconnects.
                // [`PROCESSING_TIMEOUT`] is above a legitimate turn's two round trips,
                // so only a genuinely dead one expires. Through `end_session` like
                // every other expiry: LED idle, unsuppress, per-turn state dropped.
                Session::Processing { .. } => {
                    let started = self
                        .endpointed_at
                        .get(group)
                        .or_else(|| self.woke_at.get(group));
                    if let Some(at) = started {
                        if now.duration_since(*at) >= PROCESSING_TIMEOUT {
                            expired.push(group.clone());
                        }
                    }
                }
                _ => {}
            }
        }
        for group in expired {
            let was_followup = matches!(self.session(&group), Session::FollowUp { .. });
            tracing::info!(group = %group, followup = was_followup, "session timed out: Idle");
            out.extend(self.end_session(&group));
        }
        // Close collection windows whose stragglers never arrived.
        let closed: Vec<String> = self
            .windows
            .iter()
            .filter(|(_, deadline)| now >= **deadline)
            .map(|(group, _)| group.clone())
            .collect();
        for group in closed {
            self.windows.remove(&group);
            let count = self
                .candidates
                .get(&group)
                .map_or(0, |candidates| candidates.len());
            tracing::debug!(group = %group, candidates = count, "arbitration window closed");
        }
        out
    }

    // ── shared state helpers ────────────────────────────────────────────────────

    /// Which group a source belongs to. An unconfigured source is a group of its own, so
    /// `allow_unknown_sources` still gets exactly one session (S7.1).
    fn group_of(&self, source: &str) -> String {
        self.cfg
            .source(source)
            .map(|s| s.group.clone())
            .unwrap_or_else(|| source.to_string())
    }

    /// The winner and conversation of the turn `group` is serving, but only when that turn is
    /// the one `source`/`conversation_id` name.
    ///
    /// This is the coordinator's own version of the rule the rest of the backend follows: a
    /// completion is acted on by the identity it carries, never by re-resolving `source`
    /// against whatever state is live now. A transcript, a verification, a rejection and a
    /// pipeline result all arrive after a round trip, and in the meantime a group can have
    /// timed out, been released, or opened a new turn — answering any of them by `source`
    /// alone lets a finished turn's late message rewrite the live one.
    ///
    /// `Processing` and `FollowUp` are the two states a completion can legitimately land in:
    /// `Processing` is a wake-initiated turn between endpoint and playback, `FollowUp` is the
    /// same for a turn that began without a keyword (S6.1). Anything else — `Idle`, or a
    /// `Capturing` belonging to a later request — is not that turn, and is answered by
    /// dropping the message with a log line naming what was dropped and why. Never silently:
    /// a dropped completion is otherwise indistinguishable from a lost socket.
    fn serve_turn(
        &self,
        group: &str,
        source: &str,
        conversation_id: &str,
        what: &str,
    ) -> Option<(String, String)> {
        match self.session(group) {
            Session::Processing {
                winner,
                conversation_id: live,
            }
            | Session::FollowUp {
                winner,
                conversation_id: live,
                ..
            } if winner == source && live == conversation_id => {
                Some((winner.clone(), live.clone()))
            }
            Session::Processing {
                winner,
                conversation_id: live,
            }
            | Session::FollowUp {
                winner,
                conversation_id: live,
                ..
            } => {
                tracing::debug!(
                    source = %source,
                    group = %group,
                    conversation_id = %conversation_id,
                    winner = %winner,
                    serving = %live,
                    "{what} for a turn this group is not serving: dropped"
                );
                None
            }
            other => {
                tracing::debug!(
                    source = %source,
                    group = %group,
                    conversation_id = %conversation_id,
                    state = ?other,
                    "{what} for a group with no turn in flight: dropped"
                );
                None
            }
        }
    }

    /// Every configured member of a group, including the winner.
    fn members(&self, group: &str) -> Vec<String> {
        let mut members: Vec<String> = self
            .cfg
            .sources
            .iter()
            .filter(|s| s.group == group)
            .map(|s| s.id.clone())
            .collect();
        if members.is_empty() {
            members.push(group.to_string());
        }
        members
    }

    /// Suppress the whole group while its session is Speaking (S5.2: TTS self-trigger).
    ///
    /// Already-muted members are skipped, and [`Self::unsuppress_group`] removes every
    /// member unconditionally: mute is operator intent with a different lifetime than
    /// session suppression, and copying it into `suppressed` here left a member deaf
    /// after mute → session → unmute — the session that could clear the entry is gone
    /// by then. `is_suppressed` ORs `muted` in, so a muted member stays suppressed
    /// while it is muted regardless.
    fn suppress_group(&mut self, group: &str) {
        for member in self.members(group) {
            if self.muted.contains(&member) {
                continue;
            }
            self.suppressed.insert(member);
        }
    }

    /// Lift the session's suppression from every group member, muted or not: a muted
    /// member is still reported suppressed through `muted` itself, so nothing may
    /// outlive the session that put it here.
    fn unsuppress_group(&mut self, group: &str) {
        for member in self.members(group) {
            self.suppressed.remove(&member);
        }
    }

    /// The one transition that ends a session: Idle, LED idle, group unsuppressed, all
    /// per-turn state dropped. Idempotent, and returns the control actions to perform.
    fn end_session(&mut self, group: &str) -> Vec<CoordOut> {
        let ending = self.session(group).clone();
        let was_idle = ending == Session::Idle;
        // Plan 4.3(iv): ending a Processing or Speaking session takes this group out of
        // the derived stop-listen set, so that set must be published here. The
        // suppression guard below cannot be reused for it: a Processing->Idle end with
        // no arbitration loser leaves `suppressed` unchanged, and that is exactly the
        // case that still has to publish. Captured before the state is cleared.
        let was_stop_listen = matches!(
            ending,
            Session::Processing { .. } | Session::Speaking { .. }
        );
        self.sessions.insert(group.to_string(), Session::Idle);
        self.windows.remove(group);
        self.candidates.remove(group);
        self.conversations.remove(group);
        self.followup_turns.remove(group);
        self.gone.remove(group);
        self.woke_at.remove(group);
        self.endpointed_at.remove(group);
        self.transcribed_at.remove(group);
        self.tts_at.remove(group);
        if was_idle {
            return Vec::new();
        }
        let mut out = Vec::new();
        // Whoever was lit gets told to go dark.
        for member in self.members(group) {
            if self.muted.contains(&member) {
                continue;
            }
            out.push(CoordOut::Control {
                source: member,
                control: Control::Led {
                    state: proto::LedState::Idle,
                },
            });
        }
        if was_stop_listen {
            // Stop-listen first (plan 4.2 order), under its own guard, never the
            // suppression one below (plan 4.3(iv)).
            out.push(CoordOut::StopListenChanged {
                sources: self.stop_listen(),
            });
        }
        let before = self.suppressed.len();
        self.unsuppress_group(group);
        if self.suppressed.len() != before {
            out.push(CoordOut::SuppressionChanged {
                suppressed: self.suppressed(),
            });
        }
        out
    }

    /// One conversation id per turn, reused across follow-up turns (AC7).
    fn new_conversation(&mut self, group: &str) -> String {
        self.conversations_issued += 1;
        let id = format!("listenrr-{}", self.conversations_issued);
        self.conversations.insert(group.to_string(), id.clone());
        id
    }

    /// `media_player` for TTS: the source's own `ha_entity`, else the group's fallback
    /// (S7.1). Empty means "nowhere to play" and `ha.rs` logs it.
    fn output_entity(&self, source: &str, group: &str) -> String {
        self.cfg
            .source(source)
            .and_then(|s| s.ha_entity.clone())
            .or_else(|| self.cfg.group(group).and_then(|g| g.output_entity.clone()))
            .unwrap_or_default()
    }

    fn elapsed(from: Option<Instant>, now: Instant) -> Option<u64> {
        from.map(|at| now.saturating_duration_since(at).as_millis() as u64)
    }
}

/// `capture.arbitration_window_ms` in a [`Duration`].
fn arbitration_window(cfg: &Config) -> Duration {
    Duration::from_millis(cfg.capture.arbitration_window_ms)
}

/// The acknowledgement earcon named by the plan's control message (S4.4).
const EARCON_ACK: &str = "ack";

/// How long a `Processing` group is kept while its winner is still **connected** (C-2).
///
/// A connected turn is legitimately owed two round trips: the transcription (the
/// Wyoming client's budget is 10 s) and the Home Assistant pipeline run (bounded at
/// 60 s by `ha.rs`), so this is 10 + 60 plus 20 s of margin. It must stay **above
/// both** of those bounds — expiring earlier discards a valid slow answer as stale,
/// which is worse than the hang it prevents. It exists because the error paths send no
/// completion at all: `main.rs` logs "transcription failed: the session will time out"
/// and sends nothing, and without this arm one failed or hung ASR/HA call deafens the
/// room until the satellite disconnects. The shorter `capture.hard_cap_s` bound stays
/// for a winner that has gone: nothing there can outlast the audio in its ring.
const PROCESSING_TIMEOUT: Duration = Duration::from_secs(90);

/// Signal-to-noise ratio used to elect a winner (plan S5.1).
///
/// `RMS(kw_span ± 300 ms) / RMS(noise window)`.
///
/// * `ring_range` — the winner's candidate window: the audio spanning
///   `kw_span.start - 300 ms` up to `kw_span.end + 300 ms`, extracted from the ring so
///   that `ring_range[0]` is `kw_span.start - ms_to_samples(300)`. Only that portion is
///   used, so a longer aligned slice is safe.
/// * `kw_span` — the keyword span in the same sample domain; sizes the numerator and
///   rejects a degenerate span (empty or zero-length → `0.0`, never a panic).
/// * `noise_range` — the already-shrunk noise samples from [`shrink_noise_window`].
///
/// A silent (all-zero or empty) denominator yields a large finite ratio rather than NaN,
/// so a quiet room still elects the closer satellite.
pub fn snr(ring_range: &[i16], kw_span: &Range<u64>, noise_range: &[i16]) -> f64 {
    let span_samples = kw_span.end.saturating_sub(kw_span.start);
    if span_samples == 0 {
        return 0.0; // degenerate span: never a winner, never a panic
    }
    // The aligned window is `kw_span ± 300 ms`; a longer slice is truncated to it.
    let window = (span_samples + 2 * ms_to_samples(SNR_PADDING_MS)) as usize;
    let signal = rms(&ring_range[..ring_range.len().min(window)]);
    let noise = rms(noise_range);
    // A silent denominator must still elect the closer satellite: clamp it to an epsilon
    // rather than dividing by zero, so the answer is a large finite number, never NaN.
    signal / noise.max(SILENT_DENOMINATOR_EPSILON)
}

/// Which noise window [`shrink_noise_window`] actually used, for the S8.1 sidecar and the
/// candidate log ("the window actually used is logged with the candidates", S5.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WindowChoice {
    /// The whole window was already unvoiced.
    Full,
    /// Shortened backwards until the remainder was unvoiced.
    Shrunk,
    /// Nothing in the window was unvoiced: the floor fallback, the earliest
    /// `floor_samples`, farthest from the span.
    FloorFallback,
}

/// [`shrink_noise_window`] plus the choice it made, so a test (and the candidate log) can
/// see *which* window was used.
fn shrink_noise_window_choice(
    noise_range: Range<u64>,
    vad: &mut dyn VadEngine,
    floor_samples: u64,
) -> (Range<u64>, WindowChoice) {
    let len = noise_range.end.saturating_sub(noise_range.start);
    if len == 0 {
        return (noise_range, WindowChoice::Full);
    }
    let floor = floor_samples.max(1).min(len);
    // S5.1/S3.1: a *fresh* pass. The caller owns the instance (it holds the window's
    // audio), so it is reset here rather than shared with the forward endpointer.
    vad.reset();
    let mut end = noise_range.end;
    while end - noise_range.start > floor {
        let step = floor.min(end - noise_range.start - floor);
        if !vad.process(&[]) {
            // What remains is unvoiced: that is the estimate.
            return (
                noise_range.start..end,
                if end == noise_range.end {
                    WindowChoice::Full
                } else {
                    WindowChoice::Shrunk
                },
            );
        }
        end -= step;
    }
    (
        noise_range.start..noise_range.start + floor,
        WindowChoice::FloorFallback,
    )
}

/// Root mean square in the `i16` domain; empty input is silence.
fn rms(samples: &[i16]) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    let sum: f64 = samples
        .iter()
        .map(|&s| {
            let v = f64::from(s);
            v * v
        })
        .sum();
    (sum / samples.len() as f64).sqrt()
}

/// The `± 300 ms` padding of the S5.1 candidate window.
const SNR_PADDING_MS: u64 = 300;
/// What a zero RMS denominator is clamped to: small enough that a quiet room produces a
/// huge but finite, comparable ratio.
const SILENT_DENOMINATOR_EPSILON: f64 = 1e-9;

/// Shrink the noise-estimate window until it is unvoiced (plan S5.1, B2-1).
///
/// The full window is the `capture.noise_window_s` (2 s) preceding the keyword span. A
/// fresh Silero VAD pass walks it from the span backwards and the window is shortened
/// until what remains is unvoiced — otherwise a mid-sentence wake or TV audio puts speech
/// in the denominator, ratios collapse toward ~1, and arbitration elects the wrong room.
/// If nothing in the window is unvoiced, the floor fallback returns the **earliest**
/// `floor_samples` of the window (normally `capture.noise_floor_ms` = 250 ms), i.e. the
/// part farthest from the span.
///
/// Always returns a non-empty sub-range of `noise_range`; the returned range is the
/// "window actually used" that S8.1's sidecar logs with the candidates.
///
/// The caller supplies an instance already loaded with the window's audio (that is what a
/// `VadEngine` is: [`VadEngine::process`] reports the block just fed), and each call here
/// asks for the next block's verdict, walking away from the span.
pub fn shrink_noise_window(
    noise_range: Range<u64>,
    vad: &mut dyn VadEngine,
    floor_samples: u64,
) -> Range<u64> {
    let (used, choice) = shrink_noise_window_choice(noise_range.clone(), vad, floor_samples);
    tracing::debug!(
        requested_samples = noise_range.end.saturating_sub(noise_range.start),
        used_samples = used.end - used.start,
        used_ms = crate::samples_to_ms(used.end - used.start),
        ?choice,
        "noise estimate window"
    );
    used
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::LedState;
    use std::sync::{Arc, Mutex};

    /// What a real caller hands [`shrink_noise_window`]: an instance already loaded with
    /// the window's audio, each `process` call reporting the next chunk walking backwards
    /// from the span. Scripted here so the arbitration tests stay offline and deterministic.
    #[derive(Debug, Default)]
    struct ScriptedVad {
        verdicts: std::collections::VecDeque<bool>,
        resets: usize,
        calls: usize,
    }

    impl VadEngine for ScriptedVad {
        fn process(&mut self, _samples: &[f32]) -> bool {
            self.calls += 1;
            self.verdicts.pop_front().unwrap_or(false)
        }

        fn reset(&mut self) {
            self.resets += 1;
        }
    }

    /// A clock the tests move by hand: AC1's transition table must need no tokio and no
    /// wall-clock sleeps (plan §3).
    #[derive(Debug, Clone)]
    struct FakeClock {
        now: Arc<Mutex<Instant>>,
    }

    impl FakeClock {
        fn new() -> Self {
            Self {
                now: Arc::new(Mutex::new(Instant::now())),
            }
        }

        fn advance(&self, delta: Duration) {
            let mut now = self.now.lock().unwrap();
            *now += delta;
        }
    }

    impl Clock for FakeClock {
        fn now(&self) -> Instant {
            *self.now.lock().unwrap()
        }
    }

    /// Two satellites in `kitchen`, one in `living`, one of them mute-able. All the
    /// arbitration knobs are the plan's defaults (250 ms window, 30 s speaking, 6 s
    /// follow-up, 2 s noise window, 250 ms floor).
    fn config() -> Config {
        Config::from_toml_str(
            r#"
[server]
listen = "127.0.0.1:10799"

[ha]
url = "ws://ha.example.lan:8123"
token = "ha"
pipeline_id = "p"

[parakeet]
addr = "asr.example.lan:10200"

[kws]
model = "/models/kws"
keywords_file = "/models/keywords.txt"

[clips]
dir = "/tmp/listenrr-clips"

[[sources]]
id = "kitchen-a"
room = "kitchen"
group = "kitchen"
ha_entity = "media_player.kitchen"

[[sources]]
id = "kitchen-b"
room = "kitchen"
group = "kitchen"

[[sources]]
id = "living-a"
room = "living"
group = "living"
ha_entity = "media_player.living"
mute_entity = "input_boolean.mute_living"

[[groups]]
name = "kitchen"
output_entity = "media_player.kitchen_group"

[[groups]]
name = "living"
"#,
        )
        .expect("the test config is valid")
    }

    fn coordinator() -> (Coordinator, FakeClock) {
        let clock = FakeClock::new();
        let cfg = config();
        (
            Coordinator::new(&cfg, Box::new(clock.clone())),
            clock.clone(),
        )
    }

    fn wake(source: &str, start: u64, arrived: Instant) -> WakeEvent {
        WakeEvent {
            source: source.to_string(),
            kw_span: start..start + ms_to_samples(500),
            arrived,
        }
    }

    fn has(outs: &[CoordOut], want: &CoordOut) -> bool {
        outs.contains(want)
    }

    fn led(source: &str, state: LedState) -> CoordOut {
        CoordOut::Control {
            source: source.to_string(),
            control: Control::Led { state },
        }
    }

    /// Wake → endpoint → transcript → verify → pipeline, stopping wherever the test wants.
    fn drive_to_speaking(c: &mut Coordinator, source: &str) -> Vec<CoordOut> {
        let _ = c.handle(CoordIn::Wake(wake(source, 8000, Instant::now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: source.to_string(),
            utterance_start: 8000,
            endpoint: 40000,
        });
        let conversation = match c.session(&group_of(source)) {
            Session::Processing {
                conversation_id, ..
            } => conversation_id.clone(),
            other => panic!("expected Processing, got {other:?}"),
        };
        let _ = c.handle(CoordIn::Transcript {
            source: source.to_string(),
            conversation_id: conversation.clone(),
            text: "luna what's the weather".to_string(),
        });
        let _ = c.handle(CoordIn::Verified {
            source: source.to_string(),
            conversation_id: conversation.clone(),
            text: "what's the weather".to_string(),
        });
        c.handle(CoordIn::PipelineFinished {
            source: source.to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/1.mp3".to_string()),
            mime_type: None,
        })
    }

    fn group_of(source: &str) -> String {
        match source {
            "kitchen-a" | "kitchen-b" => "kitchen".to_string(),
            other => other.to_string(),
        }
    }

    /// The conversation id the group's live session carries — the identity a completion must
    /// name to be believed as belonging to that turn. Reading it from the session rather than
    /// hard-coding `listenrr-N` keeps a test honest when the numbering of an earlier turn in
    /// the same test changes.
    fn conversation_of(c: &Coordinator, group: &str) -> String {
        match c.session(group) {
            Session::Processing {
                conversation_id, ..
            }
            | Session::FollowUp {
                conversation_id, ..
            } => conversation_id.clone(),
            other => panic!("group {group} is serving no turn: {other:?}"),
        }
    }

    #[test]
    fn a_wake_in_an_idle_group_opens_the_session_optimistically() {
        let (mut c, clock) = coordinator();
        assert_eq!(c.session("kitchen"), &Session::Idle);
        let outs = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        assert!(
            has(&outs, &led("kitchen-a", LedState::Listening)),
            "the LED goes up before anything is verified: {outs:?}"
        );
        assert!(
            has(
                &outs,
                &CoordOut::Control {
                    source: "kitchen-a".to_string(),
                    control: Control::Earcon {
                        name: "ack".to_string()
                    }
                }
            ),
            "the satellite acknowledges: {outs:?}"
        );
        let conversation = match c.session("kitchen") {
            Session::Capturing { winner, start } => {
                assert_eq!(winner, "kitchen-a");
                assert_eq!(*start, 8000);
                "listenrr-1"
            }
            other => panic!("expected Capturing, got {other:?}"),
        };
        assert!(
            has(
                &outs,
                &CoordOut::Endpoint {
                    source: "kitchen-a".to_string(),
                    kw_span: Some(8000..16000),
                    conversation_id: conversation.to_string(),
                }
            ),
            "capture is told to start on the winner: {outs:?}"
        );
    }

    #[test]
    fn one_wake_per_group_and_the_losing_satellite_is_suppressed_for_the_session() {
        let (mut c, clock) = coordinator();
        let first = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        assert!(has(
            &first,
            &CoordOut::Endpoint {
                source: "kitchen-a".to_string(),
                kw_span: Some(8000..16000),
                conversation_id: "listenrr-1".to_string()
            }
        ));

        // Inside the 250 ms window the second satellite heard the same phrase.
        let second = c.handle(CoordIn::Wake(wake("kitchen-b", 8200, clock.now())));
        assert_eq!(
            second,
            vec![CoordOut::SuppressionChanged {
                suppressed: vec!["kitchen-b".to_string()]
            }],
            "one session, one winner; the candidate is only suppressed"
        );
        assert!(
            matches!(c.session("kitchen"), Session::Capturing { winner, .. } if winner == "kitchen-a"),
            "the group still has exactly one session and one winner"
        );
        assert!(c.is_suppressed("kitchen-b"));
        assert!(!c.is_suppressed("kitchen-a"));

        // Its next wake is ignored outright, and no second capture starts.
        assert!(
            c.handle(CoordIn::Wake(wake("kitchen-b", 9000, clock.now())))
                .is_empty()
        );
        assert_eq!(c.suppressed(), vec!["kitchen-b".to_string()]);
    }

    #[test]
    fn different_groups_run_sessions_in_parallel() {
        let (mut c, clock) = coordinator();
        let kitchen = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let living = c.handle(CoordIn::Wake(wake("living-a", 8000, clock.now())));
        assert!(matches!(
            c.session("kitchen"),
            Session::Capturing { winner, .. } if winner == "kitchen-a"
        ));
        assert!(matches!(
            c.session("living"),
            Session::Capturing { winner, .. } if winner == "living-a"
        ));
        let kitchen_conv = match &kitchen[..] {
            [
                _,
                _,
                CoordOut::Endpoint {
                    conversation_id, ..
                },
            ] => conversation_id.clone(),
            other => panic!("unexpected outputs: {other:?}"),
        };
        let living_conv = match &living[..] {
            [
                _,
                _,
                CoordOut::Endpoint {
                    conversation_id, ..
                },
            ] => conversation_id.clone(),
            other => panic!("unexpected outputs: {other:?}"),
        };
        assert_ne!(
            kitchen_conv, living_conv,
            "parallel sessions must not share a conversation"
        );
    }

    #[test]
    fn a_suppressed_source_is_ignored() {
        let (mut c, clock) = coordinator();
        // Lost arbitration first…
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::Wake(wake("kitchen-b", 8200, clock.now())));
        assert!(
            c.handle(CoordIn::Wake(wake("kitchen-b", 9000, clock.now())))
                .is_empty()
        );

        // …and a muted source behaves the same way from the mute entity alone.
        let (mut c2, clock2) = coordinator();
        let outs = c2.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: true,
        });
        assert!(
            has(&outs, &led("living-a", LedState::Muted)),
            "a muted satellite is told: {outs:?}"
        );
        assert!(c2.is_suppressed("living-a"));
        assert!(
            c2.handle(CoordIn::Wake(wake("living-a", 8000, clock2.now())))
                .is_empty()
        );
        assert_eq!(c2.session("living"), &Session::Idle);

        let outs = c2.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: false,
        });
        assert!(has(&outs, &led("living-a", LedState::Idle)));
        assert!(!c2.is_suppressed("living-a"));
        assert!(
            !c2.handle(CoordIn::Wake(wake("living-a", 8000, clock2.now())))
                .is_empty()
        );
    }

    #[test]
    fn a_busy_group_ignores_wakes_and_the_whole_group_is_suppressed_while_speaking() {
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        assert!(matches!(c.session("kitchen"), Session::Speaking { .. }));
        // TTS self-trigger: the whole group is muted at the spotter (S5.2).
        assert_eq!(
            c.suppressed(),
            vec!["kitchen-a".to_string(), "kitchen-b".to_string()]
        );
        // …and a wake from anywhere in the group is dropped.
        assert!(
            c.handle(CoordIn::Wake(wake("kitchen-b", 20000, Instant::now())))
                .is_empty()
        );
        let (mut c2, clock2) = coordinator();
        drive_to_speaking(&mut c2, "kitchen-a");
        assert!(
            c2.handle(CoordIn::Wake(wake("kitchen-a", 20000, clock2.now())))
                .is_empty()
        );
        assert!(matches!(c2.session("kitchen"), Session::Speaking { .. }));
    }

    #[test]
    fn a_full_turn_walks_the_table_to_playback_then_followup_then_idle() {
        let (mut c, clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let Session::Speaking {
            winner,
            conversation_id,
            until,
        } = c.session("kitchen").clone()
        else {
            panic!("expected Speaking");
        };
        assert_eq!(winner, "kitchen-a");
        assert_eq!(
            conversation_id, "listenrr-1",
            "the same id is reused by the follow-up turn (AC7)"
        );
        assert_eq!(until, clock.now() + Duration::from_secs(30));

        // The winner's media_player is its own entity, not the group fallback.
        let outs = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(matches!(c.session("kitchen"), Session::FollowUp { .. }));
        assert!(
            outs.contains(&CoordOut::SuppressionChanged { suppressed: vec![] }),
            "the group's mic comes back for the follow-up: {outs:?}"
        );
        assert!(!c.is_suppressed("kitchen-a"));
        assert!(
            has(
                &outs,
                &CoordOut::Endpoint {
                    source: "kitchen-a".to_string(),
                    kw_span: None,
                    conversation_id: conversation_id.clone(),
                }
            ),
            "a listener starts on the winner for the follow-up (S6.1): {outs:?}"
        );

        // A follow-up turn on the winner reuses the conversation and has no keyword.
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 50000,
            endpoint: 70000,
        });
        assert!(
            has(
                &outs,
                &CoordOut::Transcribe {
                    source: "kitchen-a".to_string(),
                    conversation_id: conversation_id.clone(),
                    utterance_start: 50000,
                    endpoint: 70000,
                }
            ),
            "the follow-up is transcribed under the same conversation: {outs:?}"
        );
        let outs = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation_id.clone(),
            text: "and tomorrow".to_string(),
        });
        assert!(
            has(
                &outs,
                &CoordOut::Verify {
                    source: "kitchen-a".to_string(),
                    conversation_id: conversation_id.clone(),
                    transcript: "and tomorrow".to_string(),
                    wake_initiated: false,
                }
            ),
            "a follow-up turn has no keyword to require (S6.1): {outs:?}"
        );

        // The turn is in flight (ASR, verify, pipeline and TTS still ahead of it), so the
        // 6 s follow-up window must not kill it: re-arming to the window dropped the answer
        // to every follow-up whose pipeline took longer than `followup_s`.
        clock.advance(Duration::from_secs(7));
        let outs = c.handle(CoordIn::Tick);
        assert!(
            matches!(c.session("kitchen"), Session::FollowUp { .. }),
            "a turn in flight outlives the follow-up window: {outs:?}"
        );
        // If a step never answers, the processing guard ends it like any other turn: LED
        // idle, group released.
        clock.advance(Duration::from_secs(91));
        let outs = c.handle(CoordIn::Tick);
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "the LED goes dark when the guard expires: {outs:?}"
        );
    }

    #[test]
    fn playback_done_starts_a_followup_listener_under_the_same_conversation() {
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(
            has(
                &outs,
                &CoordOut::Endpoint {
                    source: "kitchen-a".to_string(),
                    kw_span: None,
                    conversation_id: "listenrr-1".to_string(),
                }
            ),
            "nothing listened during the window, so every follow-up was ignored: {outs:?}"
        );
    }

    #[test]
    fn speech_in_the_followup_window_stretches_it_so_a_late_answer_is_not_cut_off() {
        let (mut c, clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));

        // Speech starts 5 s into the 6 s window…
        clock.advance(Duration::from_secs(5));
        let _ = c.handle(CoordIn::Tick);
        let _ = c.handle(CoordIn::FollowUpSpeech {
            source: "kitchen-a".to_string(),
        });
        // …and is still going at 10 s, well past where the window would have closed.
        clock.advance(Duration::from_secs(5));
        let outs = c.handle(CoordIn::Tick);
        assert!(
            matches!(c.session("kitchen"), Session::FollowUp { .. }),
            "the window closed under a speaker it had already heard: {outs:?}"
        );
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 50_000,
            endpoint: 90_000,
        });
        assert!(
            outs.iter().any(|o| matches!(o, CoordOut::Transcribe { .. })),
            "and the utterance it was listening to is transcribed: {outs:?}"
        );
    }

    #[test]
    fn a_followup_window_nobody_speaks_into_still_closes() {
        let (mut c, clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        clock.advance(Duration::from_secs(7));
        let outs = c.handle(CoordIn::Tick);
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(has(&outs, &led("kitchen-a", LedState::Idle)), "{outs:?}");
    }

    #[test]
    fn followup_speech_outside_a_followup_window_changes_nothing() {
        let (mut c, _clock) = coordinator();
        // Idle group: a late message from a listener whose window already closed.
        let outs = c.handle(CoordIn::FollowUpSpeech {
            source: "kitchen-a".to_string(),
        });
        assert!(outs.is_empty(), "{outs:?}");
        assert_eq!(c.session("kitchen"), &Session::Idle);

        // Speaking group: speech while the answer is still playing must not open a window.
        drive_to_speaking(&mut c, "kitchen-a");
        let before = c.session("kitchen").clone();
        let outs = c.handle(CoordIn::FollowUpSpeech {
            source: "kitchen-a".to_string(),
        });
        assert!(outs.is_empty(), "{outs:?}");
        assert_eq!(c.session("kitchen"), &before);
    }

    /// "What's the weather, Luna?": the command is before the keyword, so the ASR must be
    /// given the audio from the start of the speech segment, not clipped to the keyword.
    #[test]
    fn a_wake_turn_transcribes_from_the_start_of_the_speech_before_the_keyword() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 40_000, clock.now())));
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 12_000, // the VAD found speech starting well before the keyword
            endpoint: 90_000,
        });
        assert!(
            has(
                &outs,
                &CoordOut::Transcribe {
                    source: "kitchen-a".to_string(),
                    conversation_id: "listenrr-1".to_string(),
                    utterance_start: 12_000,
                    endpoint: 90_000,
                }
            ),
            "everything said before the keyword reaches the ASR: {outs:?}"
        );
    }

    /// The floor: whatever the backward pass says, the keyword must stay in the ASR input,
    /// or the verify gate has no wake token to find and rejects a real wake.
    #[test]
    fn a_wake_turn_never_starts_after_its_keyword() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 40_000, clock.now())));
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 55_000, // a backward bound that landed after the keyword began
            endpoint: 90_000,
        });
        assert!(
            has(
                &outs,
                &CoordOut::Transcribe {
                    source: "kitchen-a".to_string(),
                    conversation_id: "listenrr-1".to_string(),
                    utterance_start: 40_000,
                    endpoint: 90_000,
                }
            ),
            "the keyword's own start is the latest the request may begin: {outs:?}"
        );
    }

    #[test]
    fn a_wake_during_the_followup_window_opens_a_new_session() {
        // S6.1: a wake inside the follow-up window is a new session, not a swallowed one.
        let (mut c, clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(matches!(c.session("kitchen"), Session::FollowUp { .. }));

        let outs = c.handle(CoordIn::Wake(wake("kitchen-a", 50_000, clock.now())));
        assert!(
            matches!(c.session("kitchen"), Session::Capturing { .. }),
            "a fresh wake must restart the turn: {outs:?}"
        );
        assert!(
            has(&outs, &led("kitchen-a", LedState::Listening)),
            "the new turn lights its LED again: {outs:?}"
        );
        // It is a normal session from here: it times out like any other (B4-1).
        clock.advance(Duration::from_secs(31));
        let _ = c.handle(CoordIn::Tick);
        assert_eq!(c.session("kitchen"), &Session::Idle);
    }

    #[test]
    fn speaking_gives_up_after_the_configured_timeout() {
        let (mut c, clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        // No playback_done ever arrives (the satellite died mid-announcement).
        clock.advance(Duration::from_secs(29));
        assert!(
            c.handle(CoordIn::Tick).is_empty(),
            "still inside the window"
        );
        assert!(matches!(c.session("kitchen"), Session::Speaking { .. }));
        clock.advance(Duration::from_secs(2));
        let outs = c.handle(CoordIn::Tick);
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "the group is released: {outs:?}"
        );
        assert!(c.suppressed().is_empty());
    }

    #[test]
    fn a_verify_rejection_cancels_the_led_and_keeps_the_evidence() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 12000,
        });
        let conversation = conversation_of(&c, "kitchen");
        let outs = c.handle(CoordIn::VerifyRejected {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            transcript: "turn on the television".to_string(),
        });
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "the optimistic LED is cancelled: {outs:?}"
        );
        assert!(
            has(
                &outs,
                &CoordOut::FalseAcceptClip {
                    source: "kitchen-a".to_string(),
                    transcript: "turn on the television".to_string()
                }
            ),
            "the false accept is written for tuning: {outs:?}"
        );
        assert_eq!(c.session("kitchen"), &Session::Idle);
        // HA was never entered for it.
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::RunPipeline { .. }))
        );
    }

    #[test]
    fn a_transcript_never_reaches_ha_without_the_verify_gate() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40000,
        });
        let outs = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: "listenrr-1".to_string(),
            text: "luna what's the weather".to_string(),
        });
        assert!(has(
            &outs,
            &CoordOut::Verify {
                source: "kitchen-a".to_string(),
                conversation_id: "listenrr-1".to_string(),
                transcript: "luna what's the weather".to_string(),
                wake_initiated: true
            }
        ));
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::RunPipeline { .. }))
        );
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: "listenrr-1".to_string(),
            text: "what's the weather".to_string(),
        });
        assert_eq!(
            outs,
            vec![CoordOut::RunPipeline {
                source: "kitchen-a".to_string(),
                conversation_id: "listenrr-1".to_string(),
                text: "what's the weather".to_string()
            }]
        );
    }

    /// The bare wake word, end to end through the state machine. AC1 passes "hey luna" with an
    /// empty command, and entering the pipeline with that empty string is what made this
    /// household's agent reply "Sorry, I had a problem talking to OpenAI: Error code: 400 …
    /// No user message found in messages" (measured 2026-11-02, `ha: intent-end`). Nothing was
    /// asked, so nothing is asked of HA — and the turn is not filed as a false accept either,
    /// because the wake word genuinely was said and those clips feed the false-accept rate the
    /// keyword calibration is measured on.
    #[test]
    fn a_turn_that_asks_nothing_never_enters_the_pipeline() {
        let (mut c, clock) = coordinator();
        for (conversation, asks) in [("listenrr-1", ""), ("listenrr-2", "   ")] {
            let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
            let _ = c.handle(CoordIn::EndpointReached {
                source: "kitchen-a".to_string(),
                utterance_start: 8000,
                endpoint: 40000,
            });
            let _ = c.handle(CoordIn::Transcript {
                source: "kitchen-a".to_string(),
                conversation_id: conversation.to_string(),
                text: "hey luna".to_string(),
            });
            let outs = c.handle(CoordIn::Verified {
                source: "kitchen-a".to_string(),
                conversation_id: conversation.to_string(),
                text: asks.to_string(),
            });
            assert!(
                !outs
                    .iter()
                    .any(|o| matches!(o, CoordOut::RunPipeline { .. })),
                "an empty request must not reach HA: {outs:?}"
            );
            assert!(
                !outs
                    .iter()
                    .any(|o| matches!(o, CoordOut::FalseAcceptClip { .. })),
                "the wake word was really said; this is not a false accept: {outs:?}"
            );
            assert_eq!(
                c.session("kitchen"),
                &Session::Idle,
                "the turn ends cleanly, LED and all"
            );
        }
    }

    #[test]
    fn a_pipeline_without_tts_returns_to_idle_rather_than_speaking() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40000,
        });
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: "listenrr-1".to_string(),
            tts_url: None,
            mime_type: None,
        });
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(has(&outs, &led("kitchen-a", LedState::Idle)));
        assert!(!outs.iter().any(|o| matches!(o, CoordOut::PlayMedia { .. })));
    }

    #[test]
    fn play_media_uses_the_group_output_entity_when_the_source_has_none() {
        let (mut c, clock) = coordinator();
        // kitchen-b has no `ha_entity`; the declared group's fallback answers (S7.1).
        let _ = c.handle(CoordIn::Wake(wake("kitchen-b", 8000, clock.now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: "kitchen-b".to_string(),
            utterance_start: 8000,
            endpoint: 40000,
        });
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-b".to_string(),
            conversation_id: "listenrr-1".to_string(),
            tts_url: Some("http://ha.example.lan/api/tts_proxy/2.mp3".to_string()),
            // R-C: the mime the pipeline reported must ride along to `PlayMedia`.
            mime_type: Some("audio/mpeg".to_string()),
        });
        assert!(
            has(
                &outs,
                &CoordOut::PlayMedia {
                    source: "kitchen-b".to_string(),
                    entity_id: "media_player.kitchen_group".to_string(),
                    url: "http://ha.example.lan/api/tts_proxy/2.mp3".to_string(),
                    conversation_id: "listenrr-1".to_string(),
                    mime_type: Some("audio/mpeg".to_string()),
                }
            ),
            "{outs:?}"
        );
    }

    /// Wake → endpoint, stopping in `Processing`, and returning the conversation that turn is
    /// serving. The completion messages all name this id, so a test that wants a *live* turn
    /// needs it, and a test that wants a *dead* one needs it before it dies.
    fn drive_to_processing(c: &mut Coordinator, source: &str) -> String {
        let _ = c.handle(CoordIn::Wake(wake(source, 8000, Instant::now())));
        let _ = c.handle(CoordIn::EndpointReached {
            source: source.to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        conversation_of(c, &group_of(source))
    }

    /// 5C, the ordering that used to cost an utterance. Ingest reports the departure the
    /// moment the socket closes; the capture discovers that its own connection is gone on its
    /// next poll — up to `CAPTURE_POLL` later — and only then delivers the endpoint. Ending
    /// the session on the first message meant the second arrived at an `Idle` group and was
    /// discarded as "not capturing": a satellite that had spoken a whole sentence was never
    /// transcribed, and the log said nothing but "not capturing".
    ///
    /// The audio belongs to the connection that just went away and is still in its ring, so
    /// what is owed is the transcript.
    #[test]
    fn a_source_that_goes_away_mid_capture_still_ends_its_utterance() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        // The first conversation this coordinator issues, named by the `CoordOut::Endpoint`
        // the capture was started under — the id its `EndpointReached` comes back under.
        let conversation = "listenrr-1".to_string();

        let outs = c.handle(CoordIn::Ingest(IngestEvent::SourceGone {
            source: "kitchen-a".to_string(),
        }));
        assert!(
            matches!(
                c.session("kitchen"),
                Session::Capturing { winner, .. } if winner == "kitchen-a"
            ),
            "the turn outlives its winner's departure — it still owes an endpoint: {outs:?}"
        );
        assert!(
            outs.is_empty(),
            "and the group is not torn down, its LED put out, or its state cleared while \
             that endpoint is in flight: {outs:?}"
        );

        // The capture's own report, one poll late, for the same conversation.
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        assert!(
            has(
                &outs,
                &CoordOut::Transcribe {
                    source: "kitchen-a".to_string(),
                    conversation_id: conversation.clone(),
                    utterance_start: 8000,
                    endpoint: 40_000,
                }
            ),
            "the sentence the disconnected satellite already spoke is transcribed: {outs:?}"
        );
        assert_eq!(
            c.session("kitchen"),
            &Session::Processing {
                winner: "kitchen-a".to_string(),
                conversation_id: conversation
            },
            "under the conversation the turn was opened with, not a newly invented one"
        );

        // The bound on keeping it: the turn outlives its winner only as far as the work it was
        // owed can plausibly take. An ASR call that never returns would otherwise hold the
        // group — its LED, its arbitration state, its suppression — with no satellite left to
        // answer, so the same hard cap that bounds a capture bounds this.
        clock.advance(Duration::from_secs(c.cfg.capture.hard_cap_s) + Duration::from_secs(1));
        let outs = c.handle(CoordIn::Tick);
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "a turn whose winner is gone and whose answer never came is not kept forever: \
             {outs:?}"
        );
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "and the group's LED comes down with it: {outs:?}"
        );
    }

    /// 5F: a completion acts for the turn whose `conversation_id` it carries and for no other.
    /// Each of these arrives after a round trip — ASR, the verify gate, HA — and in the
    /// meantime the group may have been released or have started a new turn. Acting on
    /// `source` alone let a finished turn's late message end, or resurrect, the live one.
    ///
    /// One assertion per handler, so removing any one guard reddens exactly that line.
    #[test]
    fn a_completion_only_acts_for_the_turn_it_belongs_to() {
        // Positive control, first: addressed to the conversation the group really is serving,
        // these same messages do act. Without it, a guard that dropped everything would pass.
        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what's the weather".to_string(),
        });
        assert!(
            has(
                &outs,
                &CoordOut::Verify {
                    source: "kitchen-a".to_string(),
                    conversation_id: conversation.clone(),
                    transcript: "luna what's the weather".to_string(),
                    wake_initiated: true,
                }
            ),
            "the live turn's transcript reaches the verify gate: {outs:?}"
        );

        // 1. A transcript for a turn that has ended opens no verify gate.
        c.release("kitchen");
        let outs = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what's the weather".to_string(),
        });
        assert!(
            outs.is_empty(),
            "a finished turn's transcript reopens nothing: {outs:?}"
        );
        assert_eq!(c.session("kitchen"), &Session::Idle);

        // 2. A verified transcript for that turn does not enter HA.
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "what's the weather".to_string(),
        });
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::RunPipeline { .. })),
            "the assistant is not asked to answer a request nobody is waiting for: {outs:?}"
        );

        // 3. A rejection addressed to the dead turn must not end the *new* one.
        let (mut c2, clock2) = coordinator();
        let dead = drive_to_processing(&mut c2, "kitchen-a");
        c2.release("kitchen");
        let _ = c2.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock2.now())));
        assert!(matches!(c2.session("kitchen"), Session::Capturing { .. }));
        let outs = c2.handle(CoordIn::VerifyRejected {
            source: "kitchen-a".to_string(),
            conversation_id: dead,
            transcript: "turn on the television".to_string(),
        });
        assert!(
            matches!(c2.session("kitchen"), Session::Capturing { .. }),
            "a rejection addressed to a predecessor cannot abort the live turn: {outs:?}"
        );
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::FalseAcceptClip { .. })),
            "and no clip is written for audio that was never this turn's: {outs:?}"
        );

        // 4. A pipeline result from the dead turn must not resurrect the group as Speaking.
        let (mut c3, _clock3) = coordinator();
        let dead = drive_to_processing(&mut c3, "kitchen-a");
        c3.release("kitchen");
        let outs = c3.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: dead,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/1.mp3".to_string()),
            mime_type: None,
        });
        assert_eq!(
            c3.session("kitchen"),
            &Session::Idle,
            "an Idle group is not put into Speaking by an answer that arrived late: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::PlayMedia { .. })),
            "nothing is played for a conversation nobody is waiting for: {outs:?}"
        );
        assert!(
            c3.suppressed().is_empty(),
            "and the group is not left deaf by it: {outs:?}"
        );

        // 5. The case the four above cannot reach, because they all put the group in a state
        // with no turn in flight: here the group *is* in `Processing`, so a guard that only
        // looked at the session's variant would let every one of these through. A first turn
        // runs to playback, its follow-up window opens, and a second turn starts — the live
        // `Processing` now carries a different conversation, and the first turn's completions
        // are still arriving.
        let (mut c4, clock4) = coordinator();
        let _ = drive_to_speaking(&mut c4, "kitchen-a");
        let _ = c4.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        let first = conversation_of(&c4, "kitchen");
        let _ = c4.handle(CoordIn::Wake(wake("kitchen-a", 50_000, clock4.now())));
        let _ = c4.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 50_000,
            endpoint: 80_000,
        });
        let second = conversation_of(&c4, "kitchen");
        assert_ne!(
            first, second,
            "the group is now serving its second turn, so the first turn's id names nobody"
        );

        // The predecessor's transcript, arriving late: no verify gate, in a group that is
        // very much in the state where a transcript gets one.
        let outs = c4.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: first.clone(),
            text: "answered a request that was already over".to_string(),
        });
        assert!(
            outs.is_empty(),
            "the earlier turn's transcript opens no verify gate in the live turn: {outs:?}"
        );

        // And the right id from a source that did not win the group: the winner is the one
        // whose turn this is.
        let outs = c4.handle(CoordIn::Transcript {
            source: "kitchen-b".to_string(),
            conversation_id: second.clone(),
            text: "not mine to answer".to_string(),
        });
        assert!(
            outs.is_empty(),
            "a source that lost arbitration speaks for no turn: {outs:?}"
        );

        // Control, in this same state: the live turn's own completion is believed, so the two
        // drops above are about identity and not about the state.
        let outs = c4.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: second.clone(),
            text: "what's the weather".to_string(),
        });
        assert!(
            has(
                &outs,
                &CoordOut::Verify {
                    source: "kitchen-a".to_string(),
                    conversation_id: second,
                    transcript: "what's the weather".to_string(),
                    wake_initiated: true,
                }
            ),
            "the live turn's transcript still reaches the verify gate: {outs:?}"
        );
    }

    /// A winner that disconnects **while nothing is owed to it** loses its session: the
    /// satellite that reports `playback_done` or speaks in the follow-up window is gone, so
    /// the state has no subject. (A disconnect with work still in flight is the other test,
    /// `a_source_that_goes_away_mid_capture_still_ends_its_utterance`: the audio it already
    /// delivered is still owed its transcript, so that turn stands.)
    #[test]
    fn a_winner_that_disconnects_ends_its_session() {
        let (mut c, _clock) = coordinator();
        let _ = drive_to_speaking(&mut c, "kitchen-a");
        assert!(matches!(c.session("kitchen"), Session::Speaking { .. }));
        let outs = c.handle(CoordIn::Ingest(IngestEvent::SourceGone {
            source: "kitchen-a".to_string(),
        }));
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(has(&outs, &led("kitchen-a", LedState::Idle)));

        // The same for the follow-up window: a window whose speaker has walked away is not
        // a window.
        let (mut c3, _clock3) = coordinator();
        let _ = drive_to_speaking(&mut c3, "kitchen-a");
        let _ = c3.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(matches!(c3.session("kitchen"), Session::FollowUp { .. }));
        let outs = c3.handle(CoordIn::Ingest(IngestEvent::SourceGone {
            source: "kitchen-a".to_string(),
        }));
        assert_eq!(c3.session("kitchen"), &Session::Idle, "{outs:?}");

        // A lost candidate must not stay suppressed forever.
        let (mut c2, clock2) = coordinator();
        let _ = c2.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock2.now())));
        let _ = c2.handle(CoordIn::Wake(wake("kitchen-b", 8200, clock2.now())));
        assert!(c2.is_suppressed("kitchen-b"));
        let outs = c2.handle(CoordIn::Ingest(IngestEvent::SourceGone {
            source: "kitchen-b".to_string(),
        }));
        assert!(!c2.is_suppressed("kitchen-b"), "{outs:?}");
    }

    #[test]
    fn events_map_to_their_actions_and_unknown_ones_do_nothing() {
        let (mut c, clock) = coordinator();
        let _ = clock;
        assert_eq!(
            c.handle(CoordIn::Ingest(IngestEvent::Event {
                source: "kitchen-a".to_string(),
                event: proto::Event::SaveBuffer
            })),
            vec![CoordOut::MissedWakeClip {
                source: "kitchen-a".to_string()
            }],
            "the for-you button writes the last 10 s (S8.2)"
        );
        assert!(
            c.handle(CoordIn::Ingest(IngestEvent::Event {
                source: "kitchen-a".to_string(),
                event: proto::Event::Rssi { dbm: -60.0 }
            }))
            .is_empty()
        );
        assert!(
            c.handle(CoordIn::Ingest(IngestEvent::Event {
                source: "kitchen-a".to_string(),
                event: proto::Event::Button {
                    name: "night_mode".to_string()
                }
            }))
            .is_empty()
        );
        // playback_done outside Speaking changes nothing.
        assert!(
            c.handle(CoordIn::Ingest(IngestEvent::Event {
                source: "kitchen-a".to_string(),
                event: proto::Event::PlaybackDone
            }))
            .is_empty()
        );
    }

    #[test]
    fn release_returns_a_group_to_idle_and_clears_its_suppression() {
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        assert!(!c.suppressed().is_empty());
        c.release("kitchen");
        assert_eq!(c.session("kitchen"), &Session::Idle);
        assert!(c.suppressed().is_empty());
        // Idle is idempotent.
        c.release("kitchen");
        c.release("never-declared");
    }

    /// C-2: the expiry for a winner that is still **connected**. The ASR/HA error paths
    /// in `main.rs` log and send no `CoordIn`, so without this bound one failed call
    /// held the group `Processing` — every later wake dropped as busy, the LED up —
    /// until the satellite disconnected. The bound sits above a legitimate turn (10 s
    /// ASR + 60 s pipeline), so the first half of the test pins that a slow answer is
    /// not cut off.
    #[test]
    fn a_connected_processing_session_expires_rather_than_wedging_the_room() {
        let (mut c, clock) = coordinator();
        let _ = drive_to_processing(&mut c, "kitchen-a");
        assert!(matches!(c.session("kitchen"), Session::Processing { .. }));

        // Just inside the bound: a slow but valid turn is still in flight.
        clock.advance(PROCESSING_TIMEOUT - Duration::from_secs(1));
        let outs = c.handle(CoordIn::Tick);
        assert!(
            matches!(c.session("kitchen"), Session::Processing { .. }),
            "a turn still inside ASR + pipeline budget is not discarded as stale: {outs:?}"
        );

        clock.advance(Duration::from_secs(1));
        let outs = c.handle(CoordIn::Tick);
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "an answer that never came cannot hold the group forever: {outs:?}"
        );
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "the stale LED comes down through the one end path: {outs:?}"
        );
        assert!(
            c.suppressed().is_empty(),
            "and the room is not left deaf: {outs:?}"
        );

        // The room can be woken again: the expiry really ended the session.
        let outs = c.handle(CoordIn::Wake(wake("kitchen-a", 60_000, clock.now())));
        assert!(
            matches!(
                c.session("kitchen"),
                Session::Capturing { winner, .. } if winner == "kitchen-a"
            ),
            "a wake after the expiry opens a new session: {outs:?}"
        );
    }

    /// C-3: mute is operator intent, not session suppression. `suppress_group` used to
    /// copy `muted` members into `suppressed` and `unsuppress_group` refused to remove
    /// them, so the ordinary sequence mute → turn → unmute left the satellite deaf for
    /// good: with no session open, nothing ever cleared the entry.
    #[test]
    fn unmuting_after_a_session_does_not_leave_the_source_deaf() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("living-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: true,
        });
        // The turn in flight continues through the mute.
        let _ = c.handle(CoordIn::EndpointReached {
            source: "living-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        let conversation = conversation_of(&c, "living");
        let _ = c.handle(CoordIn::Transcript {
            source: "living-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what's the weather".to_string(),
        });
        let _ = c.handle(CoordIn::Verified {
            source: "living-a".to_string(),
            conversation_id: conversation.clone(),
            text: "what's the weather".to_string(),
        });
        let _ = c.handle(CoordIn::PipelineFinished {
            source: "living-a".to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/1.mp3".to_string()),
            mime_type: None,
        });
        assert!(matches!(c.session("living"), Session::Speaking { .. }));

        // The answer finishes and the session ends while the source is still muted.
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "living-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(!matches!(c.session("living"), Session::Speaking { .. }));

        // Unmute: `is_suppressed` must be false and `suppressed` empty — the session
        // that could clear a leaked entry is gone.
        let outs = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: false,
        });
        assert!(!c.is_suppressed("living-a"), "{outs:?}");
        assert!(c.suppressed().is_empty(), "{outs:?}");
        let outs = c.handle(CoordIn::Wake(wake("living-a", 50_000, clock.now())));
        assert!(
            matches!(
                c.session("living"),
                Session::Capturing { winner, .. } if winner == "living-a"
            ),
            "the unmuted satellite hears the next wake: {outs:?}"
        );
    }

    /// C-3's other half: a source unmuted *while its group is playing TTS* stays
    /// suppressed until the session ends — its mic is back within earshot of that
    /// speaker, and S5.2 exists for exactly that self-trigger. (A fix that only made
    /// `unsuppress_group` unconditional would redden this: `suppress_group` skipped
    /// the muted member, so the re-insert on unmute is what holds here.)
    #[test]
    fn unmuting_mid_answer_keeps_the_source_suppressed_until_the_session_ends() {
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("living-a", 8000, clock.now())));
        let _ = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: true,
        });
        let _ = c.handle(CoordIn::EndpointReached {
            source: "living-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        let conversation = conversation_of(&c, "living");
        let _ = c.handle(CoordIn::Transcript {
            source: "living-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what's the weather".to_string(),
        });
        let _ = c.handle(CoordIn::Verified {
            source: "living-a".to_string(),
            conversation_id: conversation.clone(),
            text: "what's the weather".to_string(),
        });
        let _ = c.handle(CoordIn::PipelineFinished {
            source: "living-a".to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/1.mp3".to_string()),
            mime_type: None,
        });
        assert!(matches!(c.session("living"), Session::Speaking { .. }));

        let outs = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_living".to_string(),
            muted: false,
        });
        assert!(
            c.is_suppressed("living-a"),
            "the mic is back but the speaker is still talking: {outs:?}"
        );
        assert_eq!(c.suppressed(), vec!["living-a".to_string()]);
        assert!(
            c.handle(CoordIn::Wake(wake("living-a", 50_000, clock.now())))
                .is_empty(),
            "its own TTS is not a wake"
        );

        // The session's end takes it down with the rest of the group.
        let outs = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "living-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(
            !c.is_suppressed("living-a"),
            "nothing outlives the session: {outs:?}"
        );
        assert!(c.suppressed().is_empty(), "{outs:?}");
    }

    /// C-5 (defense in depth, no seam change): `EndpointReached` carries no
    /// conversation id, so the endpoint a capture that already timed out delivers late
    /// is attributed to the turn that replaced it. Its own arithmetic identifies it —
    /// an endpoint at or before the live capture's start is a reversed or empty range,
    /// which would send zero samples to ASR and strand the live turn in `Processing`.
    #[test]
    fn a_stale_endpoint_from_a_dead_capture_leaves_the_live_one_capturing() {
        let (mut c, clock) = coordinator();
        // Turn A wakes and never endpoints; the hard cap closes it (B4-1).
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        clock.advance(Duration::from_secs(c.cfg.capture.hard_cap_s) + Duration::from_secs(1));
        let _ = c.handle(CoordIn::Tick);
        assert_eq!(c.session("kitchen"), &Session::Idle);

        // Turn B opens on the same source, further along the sample domain.
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 50_000, clock.now())));
        assert!(matches!(c.session("kitchen"), Session::Capturing { .. }));

        // A's backstop endpoint finally lands. Its range predates B's capture.
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        assert!(
            matches!(
                c.session("kitchen"),
                Session::Capturing { winner, .. } if winner == "kitchen-a"
            ),
            "the live capture is not completed by a dead turn's endpoint: {outs:?}"
        );
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::Transcribe { .. })),
            "and zero samples are never handed to ASR: {outs:?}"
        );

        // The live turn's own endpoint, after its start, still completes it.
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 50_000,
            endpoint: 80_000,
        });
        let conversation = conversation_of(&c, "kitchen");
        assert!(
            has(
                &outs,
                &CoordOut::Transcribe {
                    source: "kitchen-a".to_string(),
                    conversation_id: conversation,
                    utterance_start: 50_000,
                    endpoint: 80_000,
                }
            ),
            "a valid endpoint is still accepted: {outs:?}"
        );
    }

    #[test]
    fn an_unknown_group_reads_idle_and_an_unconfigured_source_is_its_own_group() {
        let (mut c, clock) = coordinator();
        assert_eq!(c.session("no-such-group"), &Session::Idle);
        // S7.1: an ad-hoc source still gets exactly one session, of its own.
        let outs = c.handle(CoordIn::Wake(wake("attic", 8000, clock.now())));
        assert!(has(&outs, &led("attic", LedState::Listening)));
        assert!(matches!(
            c.session("attic"),
            Session::Capturing { winner, .. } if winner == "attic"
        ));
    }

    // ── S5.1: the SNR rule and the voiced denominator ──────────────────────────

    fn tone(amplitude: i16, samples: usize) -> Vec<i16> {
        vec![amplitude; samples]
    }

    #[test]
    fn snr_is_a_finite_ratio_and_never_panics_on_degenerate_input() {
        let span = 3000u64..8000u64;
        let padding = ms_to_samples(300) as usize;
        let span_len = (span.end - span.start) as usize;
        let loud = tone(20_000, span_len + 2 * padding);
        let quiet = tone(100, 4000);
        let ratio = snr(&loud, &span, &quiet);
        assert!(ratio > 1.0, "loud keyword over quiet room is a clear win");
        assert!(!ratio.is_nan() && ratio.is_finite());

        // A silent denominator is a large finite number, not NaN (contract choice 12).
        let silence = snr(&loud, &span, &[]);
        assert!(silence > 1e6 && silence.is_finite(), "{silence}");
        let zeros = snr(&loud, &span, &tone(0, 4000));
        assert!(zeros.is_finite() && zeros > 1e6);

        // A degenerate span never wins and never panics.
        assert_eq!(snr(&loud, &(8000..8000), &quiet), 0.0);
        // False positive: the reversed span is the input under test — `snr` must score a
        // start >= end span 0.0 rather than panic (contract choice 12).
        #[allow(clippy::reversed_empty_ranges)]
        {
            assert_eq!(snr(&loud, &(9000..8000), &quiet), 0.0);
        }
        assert_eq!(snr(&[], &span, &quiet), 0.0);

        // The closer satellite — the same keyword at a higher level — scores higher.
        let far = tone(2_000, span_len + 2 * padding);
        assert!(snr(&loud, &span, &quiet) > snr(&far, &span, &quiet));
    }

    #[test]
    fn the_noise_window_shrinks_backwards_until_it_is_unvoiced() {
        let window = 0u64..32_000; // 2 s at 16 kHz
        let floor = ms_to_samples(250); // 4000 samples
        // The two chunks nearest the span are speech; before them is room tone.
        let mut vad = ScriptedVad {
            verdicts: [true, true, false].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let (used, choice) = shrink_noise_window_choice(window.clone(), &mut vad, floor);
        assert_eq!(used, 0..24_000, "two 250 ms chunks were dropped");
        assert_eq!(choice, WindowChoice::Shrunk);
        assert_eq!(vad.resets, 1, "S5.1 asks for a fresh pass");
        assert_eq!(vad.calls, 3);

        // Already unvoiced: the whole window stands.
        let mut quiet = ScriptedVad {
            verdicts: [false].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let (used, choice) = shrink_noise_window_choice(window.clone(), &mut quiet, floor);
        assert_eq!(used, window);
        assert_eq!(choice, WindowChoice::Full);

        // Nothing in the window is unvoiced: the floor fallback, the EARLIEST 250 ms,
        // i.e. the part farthest from the span.
        let mut all_voiced = ScriptedVad {
            verdicts: [true; 40].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let (used, choice) = shrink_noise_window_choice(window.clone(), &mut all_voiced, floor);
        assert_eq!(used, 0..floor);
        assert_eq!(choice, WindowChoice::FloorFallback);

        // A window already at or below the floor is returned whole, and never empty.
        let tiny = 1000u64..1500;
        let (used, _) =
            shrink_noise_window_choice(tiny.clone(), &mut ScriptedVad::default(), floor);
        assert_eq!(used, tiny);
        let (used, _) = shrink_noise_window_choice(1000..1000, &mut ScriptedVad::default(), floor);
        assert_eq!(used, 1000..1000);
    }

    #[test]
    fn the_public_shrink_returns_a_non_empty_sub_range_of_the_window() {
        let window = 0u64..32_000;
        let mut vad = ScriptedVad {
            verdicts: [true, false].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let used = shrink_noise_window(window.clone(), &mut vad, ms_to_samples(250));
        assert!(used.start >= window.start && used.end <= window.end);
        assert!(used.end > used.start);
    }

    #[test]
    fn a_voiced_denominator_still_elects_the_near_satellite_after_the_shrink() {
        // AC1's hardest case: someone is speaking in front of the far satellite's 2 s
        // noise window… without the shrink both ratios collapse toward 1 and arbitration
        // elects the wrong room (S5.1).
        let span = 40_000u64..48_000u64;
        let span_len = (span.end - span.start) as usize;
        let padding = ms_to_samples(300) as usize;
        let window_len = 32_000usize; // capture.noise_window_s = 2 s
        let floor = ms_to_samples(250);

        let near_keyword = tone(20_000, span_len + 2 * padding);
        let far_keyword = tone(3_000, span_len + 2 * padding);
        // The near satellite's window has 500 ms of speech right before the keyword.
        let mut near_noise = tone(100, window_len);
        for sample in near_noise.iter_mut().skip(window_len - 8_000) {
            *sample = 30_000;
        }
        let far_noise = tone(100, window_len);

        // Without the shrink: the near satellite's denominator is the speech itself, and
        // the far satellite wins. That is the bug B2-1 exists for.
        let unshrunk_near = snr(&near_keyword, &span, &near_noise);
        let unshrunk_far = snr(&far_keyword, &span, &far_noise);
        assert!(
            unshrunk_far > unshrunk_near,
            "the unshrunk rule elects the wrong room: near {unshrunk_near}, far {unshrunk_far}"
        );

        // With it: the near satellite's speech is walked out of the denominator and the
        // near satellite wins, which is the plan's max-ratio rule.
        let mut near_vad = ScriptedVad {
            verdicts: [true, true, false].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let (near_window, choice) =
            shrink_noise_window_choice(0..window_len as u64, &mut near_vad, floor);
        assert_eq!(choice, WindowChoice::Shrunk);
        let mut far_vad = ScriptedVad {
            verdicts: [false].into_iter().collect(),
            resets: 0,
            calls: 0,
        };
        let (far_window, _) = shrink_noise_window_choice(0..window_len as u64, &mut far_vad, floor);

        let near = snr(
            &near_keyword,
            &span,
            &near_noise[near_window.start as usize..near_window.end as usize],
        );
        let far = snr(
            &far_keyword,
            &span,
            &far_noise[far_window.start as usize..far_window.end as usize],
        );
        assert!(near > far, "the near satellite must win: {near} vs {far}");

        // The winner is by max ratio, and the loser is the one the coordinator suppresses.
        let (mut c, clock) = coordinator();
        let candidates = [
            ("kitchen-b", far),
            ("kitchen-a", near), // later arrival, higher ratio
        ];
        let winner = candidates
            .iter()
            .max_by(|(_, a), (_, b)| a.total_cmp(b))
            .map(|(source, _)| *source)
            .expect("two candidates");
        let _ = c.handle(CoordIn::Wake(wake("kitchen-b", 40_000, clock.now())));
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 40_000, clock.now())));
        assert!(
            matches!(c.session("kitchen"), Session::Capturing { winner, .. } if *winner == "kitchen-b"),
            "the first waker holds the session; the SNR winner is the one that must be \
             reported, and `CoordIn::Wake` carries no audio to rank on"
        );
        assert_eq!(winner, "kitchen-a", "max ratio elects the near satellite");
    }

    // ── stop (stop-command plan AC1–AC5) ─────────────────────────────────────────

    /// A stop detection for `source`, shaped as the KWS thread's stop lane sends it
    /// (plan 4.4).
    fn stop(source: &str) -> CoordIn {
        CoordIn::Stop(crate::kws::StopEvent {
            source: source.to_string(),
            arrived: Instant::now(),
        })
    }

    /// The default config with `kitchen-b` mute-able. The built-in one only mutes a
    /// single-member group, and the privacy tests need a mute inside a busy group.
    fn pair_config() -> Config {
        let mut cfg = config();
        cfg.sources
            .iter_mut()
            .find(|s| s.id == "kitchen-b")
            .unwrap()
            .mute_entity = Some("input_boolean.mute_kitchen_b".to_string());
        cfg
    }

    /// A coordinator over [`pair_config`].
    fn pair_coordinator() -> (Coordinator, FakeClock) {
        let clock = FakeClock::new();
        let cfg = pair_config();
        (
            Coordinator::new(&cfg, Box::new(clock.clone())),
            clock.clone(),
        )
    }

    /// Every path that ends a busy session must publish the derived stop-listen set —
    /// exactly what the coordinator holds now that the session is gone (plan 4.3(iv),
    /// AC5c).
    fn assert_end_publishes(c: &Coordinator, outs: &[CoordOut], path: &str) {
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "{path} did not end the session"
        );
        assert!(
            has(
                outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "{path} ended a busy session without publishing the derived set: {outs:?}"
        );
    }

    /// AC1: a stop in `Processing` ends the session at once — Idle, LED idle on every
    /// non-muted member, group unsuppressed, the stop-listen set published empty — and
    /// emits no `StopPlayback`, because nothing is playing.
    #[test]
    fn a_stop_in_processing_ends_the_session_without_touching_audio() {
        let (mut c, _clock) = coordinator();
        let _ = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(stop("kitchen-a"));
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "the conversation is over the moment the stop is heard: {outs:?}"
        );
        assert!(
            has(&outs, &led("kitchen-a", LedState::Idle)),
            "the winner's LED goes idle: {outs:?}"
        );
        assert!(
            has(&outs, &led("kitchen-b", LedState::Idle)),
            "every member's does: {outs:?}"
        );
        assert!(!c.is_suppressed("kitchen-a"));
        assert!(!c.is_suppressed("kitchen-b"));
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "the emptied set is published: {outs:?}"
        );
        assert!(c.stop_listen().is_empty());
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::StopPlayback { .. })),
            "nothing was playing, so there is nothing to stop: {outs:?}"
        );
    }

    /// AC1: a stop in `Speaking` first tells the winner's media_player to stop —
    /// addressed to the winner with the same resolved entity `PlayMedia` used, whatever
    /// member spoke the stop — and only then ends the session. A muted member keeps its
    /// Muted LED (`end_session`'s existing rule).
    #[test]
    fn a_stop_in_speaking_stops_the_winners_audio_before_ending_the_session() {
        let (mut c, _clock) = pair_coordinator();
        let _ = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_kitchen_b".to_string(),
            muted: true,
        });
        drive_to_speaking(&mut c, "kitchen-a");
        // The stop is heard by kitchen-b; kitchen-a is the one playing.
        let outs = c.handle(stop("kitchen-b"));
        match outs.first() {
            Some(CoordOut::StopPlayback { source, entity }) => {
                assert_eq!(
                    source, "kitchen-a",
                    "the audio to stop is the winner's, not the caller's"
                );
                assert_eq!(
                    entity, "media_player.kitchen",
                    "addressed with the resolved entity"
                );
            }
            other => panic!("the audio stop must come first: {other:?}"),
        }
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "and the session ends with it: {outs:?}"
        );
        assert!(has(&outs, &led("kitchen-a", LedState::Idle)));
        assert!(
            !has(&outs, &led("kitchen-b", LedState::Idle)),
            "a muted member was never lit for this session and keeps its Muted LED: {outs:?}"
        );
        assert!(!c.is_suppressed("kitchen-a"));
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "the emptied set is published: {outs:?}"
        );
    }

    /// AC2 / BC2: outside `Processing`/`Speaking` the audio stop is not a feature — in
    /// `Idle`, `Capturing` and `FollowUp` it emits nothing and leaves the session
    /// untouched, and a source outside the busy group changes nothing.
    #[test]
    fn a_stop_outside_a_busy_session_changes_nothing() {
        // Idle.
        let (mut c, _clock) = coordinator();
        let outs = c.handle(stop("kitchen-a"));
        assert!(outs.is_empty(), "a stop in an idle group is nothing: {outs:?}");
        assert_eq!(c.session("kitchen"), &Session::Idle);

        // Capturing.
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let outs = c.handle(stop("kitchen-a"));
        assert!(outs.is_empty(), "the capture is not interrupted by sound: {outs:?}");
        assert!(
            matches!(c.session("kitchen"), Session::Capturing { .. }),
            "the turn stands"
        );

        // FollowUp (BC2: the transcript path is the stop path there).
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(matches!(c.session("kitchen"), Session::FollowUp { .. }));
        let outs = c.handle(stop("kitchen-a"));
        assert!(
            outs.is_empty(),
            "the follow-up window is not a busy session (BC2): {outs:?}"
        );
        assert!(
            matches!(c.session("kitchen"), Session::FollowUp { .. }),
            "the window stays open"
        );

        // A source outside the busy group.
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let outs = c.handle(stop("living-a"));
        assert!(
            outs.is_empty(),
            "another group's stop does not touch this one: {outs:?}"
        );
        assert!(matches!(c.session("kitchen"), Session::Speaking { .. }));
        assert_eq!(c.session("living"), &Session::Idle);
    }

    /// AC3: the kill mechanisms the plan relies on (2.5/2.6). A stopped session must not
    /// resurrect through the late pipeline answer, and the `media_stop` idle edge must
    /// not open a follow-up window.
    #[test]
    fn a_late_completion_after_a_stop_neither_plays_nor_opens_the_follow_up() {
        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let _ = c.handle(stop("kitchen-a"));
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/late.mp3".to_string()),
            mime_type: None,
        });
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::PlayMedia { .. })),
            "the stopped conversation's late answer never plays: {outs:?}"
        );
        assert_eq!(c.session("kitchen"), &Session::Idle);
        let outs = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(
            outs.is_empty(),
            "a playback_done after a stop (the media_stop idle edge, 2.6) opens no window: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::Endpoint { kw_span: None, .. })),
            "no listener starts on an idle group: {outs:?}"
        );
        assert_eq!(c.session("kitchen"), &Session::Idle);
    }

    /// AC4 (R2): the turn text being exactly a stop phrase ends the session without
    /// entering the pipeline — as a follow-up turn (bare "stop") and as a wake turn
    /// ("luna stop", whose keyword the verify gate has already stripped). No
    /// `RunPipeline`, no `StopPlayback` (nothing plays in those states).
    #[test]
    fn a_spoken_stop_phrase_ends_the_session_without_entering_the_pipeline() {
        // R2b: "luna stop" as a wake turn.
        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            text: "stop".to_string(),
        });
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "the wake-turn stop ends the session: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::RunPipeline { .. })),
            "a spoken stop is not a request for the pipeline: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::StopPlayback { .. })),
            "nothing was playing: {outs:?}"
        );
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "ending the Processing session publishes the set (AC5c): {outs:?}"
        );

        // R2a: bare "stop" as a follow-up turn.
        let (mut c, _clock) = coordinator();
        drive_to_speaking(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        let conversation = conversation_of(&c, "kitchen");
        let _ = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 50_000,
            endpoint: 70_000,
        });
        let _ = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "stop".to_string(),
        });
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            text: "stop".to_string(),
        });
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "the follow-up-turn stop ends the conversation: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::RunPipeline { .. })),
            "and never reaches Home Assistant (BC1): {outs:?}"
        );
    }

    /// AC4: matching is exact after `config::normalise_phrase` and uses the configured
    /// list — punctuation and case survive, supersets and substring-hits stay commands,
    /// and an operator's extra phrase works.
    #[test]
    fn stop_phrase_matching_is_exact_after_normalisation() {
        for (text, must_stop) in [
            ("stop.", true),
            (" Stop ", true),
            ("stop the music in the kitchen", false),
            ("stopwatch", false),
        ] {
            let (mut c, _clock) = coordinator();
            let conversation = drive_to_processing(&mut c, "kitchen-a");
            let outs = c.handle(CoordIn::Verified {
                source: "kitchen-a".to_string(),
                conversation_id: conversation,
                text: text.to_string(),
            });
            if must_stop {
                assert_eq!(
                    c.session("kitchen"),
                    &Session::Idle,
                    "{text:?} normalises to a configured stop and must end the session"
                );
                assert!(
                    !outs.iter().any(|o| matches!(o, CoordOut::RunPipeline { .. })),
                    "{text:?} must not reach the pipeline: {outs:?}"
                );
            } else {
                assert!(
                    outs.iter().any(
                        |o| matches!(o, CoordOut::RunPipeline { text: t, .. } if t == text)
                    ),
                    "{text:?} is a command, not a stop: {outs:?}"
                );
            }
        }

        // An operator-configured extra phrase matches through the same list.
        let mut cfg = config();
        cfg.capture.stop_phrases.push("shut up".to_string());
        let clock = FakeClock::new();
        let mut c = Coordinator::new(&cfg, Box::new(clock.clone()));
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            text: "shut up".to_string(),
        });
        assert_eq!(
            c.session("kitchen"),
            &Session::Idle,
            "the configured extra phrase ends the session: {outs:?}"
        );
        assert!(
            !outs.iter().any(|o| matches!(o, CoordOut::RunPipeline { .. })),
            "without entering the pipeline: {outs:?}"
        );
    }

    /// AC5a (privacy, R4): a muted source is never offered to the stop engine in any
    /// state, at either publication point.
    #[test]
    fn a_muted_member_never_reaches_the_stop_engine() {
        let (mut c, clock) = pair_coordinator();
        let _ = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_kitchen_b".to_string(),
            muted: true,
        });
        let muted = "kitchen-b".to_string();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: vec!["kitchen-a".to_string()]
                }
            ),
            "the busy group's unmuted member is offered; the muted one is not: {outs:?}"
        );
        assert!(
            outs.iter()
                .all(|o| !matches!(o, CoordOut::StopListenChanged { sources } if sources.contains(&muted))),
            "a muted source is starved of every spotter, including the stop engine (I4/R4): {outs:?}"
        );

        // And it stays starved across the transition into Speaking.
        let conversation = conversation_of(&c, "kitchen");
        let _ = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what time is it".to_string(),
        });
        let _ = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "what time is it".to_string(),
        });
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/2.mp3".to_string()),
            mime_type: None,
        });
        assert!(
            outs.iter()
                .all(|o| !matches!(o, CoordOut::StopListenChanged { sources } if sources.contains(&muted))),
            "nor on the way into Speaking: {outs:?}"
        );
    }

    /// AC5b (privacy, mid-session) + AC5c (ordering): muting a member of a Speaking
    /// group removes it from the next publication, and `StopListenChanged` precedes
    /// `SuppressionChanged` (F3: the stop engine is starved before the suppression watch
    /// moves). Unmuting while the group is still busy re-adds it.
    #[test]
    fn muting_a_speaking_member_shrinks_the_stop_set_before_the_suppression_watch() {
        let (mut c, _clock) = pair_coordinator();
        let outs = drive_to_speaking(&mut c, "kitchen-a");
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: vec!["kitchen-a".to_string(), "kitchen-b".to_string()]
                }
            ),
            "while the group is Speaking both members are offered: {outs:?}"
        );
        let (sl, sp) = (
            outs.iter()
                .position(|o| matches!(o, CoordOut::StopListenChanged { .. })),
            outs
                .iter()
                .position(|o| matches!(o, CoordOut::SuppressionChanged { .. })),
        );
        assert!(
            matches!((sl, sp), (Some(i), Some(j)) if i < j),
            "the stop-listen set is published before the suppression set: {outs:?}"
        );

        let outs = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_kitchen_b".to_string(),
            muted: true,
        });
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: vec!["kitchen-a".to_string()]
                }
            ),
            "the just-muted member leaves the set: {outs:?}"
        );
        let (sl, sp) = (
            outs.iter()
                .position(|o| matches!(o, CoordOut::StopListenChanged { .. })),
            outs
                .iter()
                .position(|o| matches!(o, CoordOut::SuppressionChanged { .. })),
        );
        assert!(
            matches!((sl, sp), (Some(i), Some(j)) if i < j),
            "still ahead of the suppression watch: {outs:?}"
        );

        let outs = c.handle(CoordIn::MuteChanged {
            entity: "input_boolean.mute_kitchen_b".to_string(),
            muted: false,
        });
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: vec!["kitchen-a".to_string(), "kitchen-b".to_string()]
                }
            ),
            "unmuting while the reply still plays re-adds it: {outs:?}"
        );
        assert!(matches!(c.session("kitchen"), Session::Speaking { .. }));
    }

    /// AC5c: the set is published on every transition that changes it, and every
    /// publication equals `stop_listen()`. The `end_session` cases include the one the
    /// old code got wrong: a Processing->Idle end with **no** suppressed loser publishes
    /// anyway, because the stop-listen push does not ride the suppression guard.
    #[test]
    fn every_transition_that_changes_the_stop_set_publishes_it() {
        // Capturing -> Processing: the set grows and is published non-empty (4.3(i)).
        let (mut c, clock) = coordinator();
        let _ = c.handle(CoordIn::Wake(wake("kitchen-a", 8000, clock.now())));
        let outs = c.handle(CoordIn::EndpointReached {
            source: "kitchen-a".to_string(),
            utterance_start: 8000,
            endpoint: 40_000,
        });
        assert!(
            !c.stop_listen().is_empty(),
            "a Processing group offers its whole group"
        );
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "entering Processing publishes the non-empty set: {outs:?}"
        );

        // Processing -> Speaking publishes via the ordered pair (4.3(ii)).
        let conversation = conversation_of(&c, "kitchen");
        let _ = c.handle(CoordIn::Transcript {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "luna what time is it".to_string(),
        });
        let _ = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation.clone(),
            text: "what time is it".to_string(),
        });
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/2.mp3".to_string()),
            mime_type: None,
        });
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "the transition into Speaking publishes via set_outs: {outs:?}"
        );

        // Speaking -> FollowUp (on_playback_done) empties it (4.3(iii)).
        let outs = c.handle(CoordIn::Ingest(IngestEvent::Event {
            source: "kitchen-a".to_string(),
            event: proto::Event::PlaybackDone,
        }));
        assert!(
            has(
                &outs,
                &CoordOut::StopListenChanged {
                    sources: c.stop_listen()
                }
            ),
            "the window opening takes the group out of the set: {outs:?}"
        );
        assert!(c.stop_listen().is_empty(), "FollowUp contributes nothing");

        // end_session from Processing, every path. These turns have no arbitration loser,
        // so `suppressed` never changes and the existing suppression guard stays false —
        // the stop-listen publication must not be tied to it (4.3(iv)).
        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::VerifyRejected {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            transcript: "turn on the television".to_string(),
        });
        assert_end_publishes(&c, &outs, "verify rejection");

        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::Verified {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            text: "   ".to_string(),
        });
        assert_end_publishes(&c, &outs, "empty text");
        assert!(
            !outs
                .iter()
                .any(|o| matches!(o, CoordOut::SuppressionChanged { .. })),
            "no suppression changed on this path, yet the stop-listen set was still \
             published above: {outs:?}"
        );

        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            tts_url: None,
            mime_type: None,
        });
        assert_end_publishes(&c, &outs, "no-TTS");

        let (mut c, _clock) = coordinator();
        let conversation = drive_to_processing(&mut c, "kitchen-a");
        let _ = c.handle(CoordIn::Ingest(IngestEvent::SourceGone {
            source: "kitchen-a".to_string(),
        }));
        let outs = c.handle(CoordIn::PipelineFinished {
            source: "kitchen-a".to_string(),
            conversation_id: conversation,
            tts_url: Some("http://ha.example.lan:8123/api/tts_proxy/2.mp3".to_string()),
            mime_type: None,
        });
        assert_end_publishes(&c, &outs, "gone winner");

        let (mut c, clock) = coordinator();
        let _ = drive_to_processing(&mut c, "kitchen-a");
        clock.advance(PROCESSING_TIMEOUT + Duration::from_secs(1));
        let outs = c.handle(CoordIn::Tick);
        assert_end_publishes(&c, &outs, "processing timeout");
    }
}
