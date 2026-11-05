//! Home Assistant WebSocket client: the only path to an intent (plan S4.3, S4.5, R3).
//!
//! Enters the **existing** Assist pipeline at the intent stage — no change to the HA
//! stack (plan ADR). Websocket auth with the long-lived token, `assist_pipeline/run` with
//! `start_stage: "intent"`, `end_stage: "tts"`, `input.text`, `pipeline`, `device_id`,
//! `conversation_id = session`; parse `intent-end` (log) and `tts-end` (url + mime_type);
//! then `media_player.play_media` on the winning satellite's entity with `announce: true`
//! (plan O2, HA-first). Mute entities are subscribed as state changes and forwarded to
//! the coordinator (S4.5).
//!
//! **Tolerant reader** (plan R3): every HA field is read through accessors that degrade a
//! missing or renamed field to `None` plus a warning, never a panic. U2 — the real
//! `assist_pipeline/run` event envelope — is pinned by dumping one live run as this
//! module's first task; the stage-name constants below are the frozen parts the plan
//! states, the JSON paths are U2's to pin. Reconnect + resubscribe with backoff (plan
//! invariant: backend restart is safe, HA stays healthy).
//!
//! LANE-OWNED SEAM (Lane A, S4.1/S4.3/S4.5 / U2). Public signatures are frozen —
//! implement the bodies; add only private items. Report a mismatch instead of editing.

use std::collections::HashSet;
use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

/// `assist_pipeline/run` `start_stage` (plan S4.3): enter at the intent stage, after STT,
/// because transcription is ours.
pub const START_STAGE_INTENT: &str = "intent";
/// `assist_pipeline/run` `end_stage` (plan S4.3): run through TTS.
pub const END_STAGE_TTS: &str = "tts";
/// `media_player.play_media` announce flag (plan O2): duck/override like an announcement.
pub const PLAY_ANNOUNCE: bool = true;
/// Reconnect backoff floor for the WebSocket and resubscribes (plan R3, S4.3).
pub const RECONNECT_BACKOFF_MIN_MS: u64 = 250;
/// Reconnect backoff ceiling.
pub const RECONNECT_BACKOFF_MAX_MS: u64 = 30_000;

/// HA's WebSocket endpoint, appended when `ha.url` carries no path.
const WS_PATH: &str = "/api/websocket";
/// HA's default port, used when `ha.url` omits one.
const DEFAULT_PORT: u16 = 8123;
/// Fallback `media_content_type` for a TTS audio URL. HA requires the key; when
/// `tts-end` carries no `mime_type` (R-C) the decoder-agnostic value is used (HA
/// transcodes for the announcement path — plan O2/S1.5).
const MEDIA_CONTENT_TYPE: &str = "music";
/// Upper bound on one command's round trip, so a half-dead socket cannot wedge a run.
///
/// This is an *inter-frame* bound: every received frame — including a WebSocket ping —
/// restarts it ([`Session::next_json`]). It is defence in depth, not the request bound;
/// the request bounds are [`AUTH_DEADLINE`], [`COMMAND_DEADLINE`] and [`RUN_DEADLINE`].
const READ_TIMEOUT: Duration = Duration::from_secs(30);
/// Upper bound on frames consumed for one pipeline run before giving up (a run has tens).
const MAX_RUN_EVENTS: usize = 512;
/// Overall deadline for the whole auth stage: TCP connect, websocket handshake and the
/// `auth_required` → `auth` → `auth_ok` exchange. No stream of control frames can extend
/// it — a peer that completes the handshake and then only pings fails here, not after 30 s
/// of self-renewing per-frame timeouts (finding HA-1).
pub(crate) const AUTH_DEADLINE: Duration = Duration::from_secs(10);
/// Overall deadline for one plain command round trip — the mute subscription's
/// `subscribe_events` (with its `get_states` snapshot request) and `play_media`'s
/// `call_service` — connect and auth included.
pub(crate) const COMMAND_DEADLINE: Duration = Duration::from_secs(15);
/// Overall deadline for one `assist_pipeline/run`, auth and event stream included.
/// The coordinator bounds a whole turn at 90 s and relies on this one staying inside it:
/// **do not raise this above 60 s.**
pub(crate) const RUN_DEADLINE: Duration = Duration::from_secs(60);
/// A websocket write that never completes (dead peer, full socket buffer) must not wedge
/// the stage that issued it; a frame's real wire time is microseconds.
const WRITE_TIMEOUT: Duration = Duration::from_secs(10);
/// How long a mute subscription must have been established before losing it counts as
/// "a healthy connection died" — which resets the reconnect backoff (finding HA-3) —
/// rather than as another failed attempt, which grows it. HA restarts are rare; a flap
/// dies in well under a second, so this separates the two.
const BACKOFF_RESET_AFTER: Duration = Duration::from_secs(30);
/// Mute entities are switches (`switch.kitchen_mute`), so "on" *is* muted; a
/// `media_player` mute entity carries `is_volume_muted` instead and is read first.
const MUTE_ON: [&str; 3] = ["on", "true", "muted"];
const MUTE_OFF: [&str; 3] = ["off", "false", "unmuted"];

/// Tolerant reader paths for the run's intent summary, in priority order (U2: the
/// envelope is pinned from a live dump; the rest stay as fallbacks).
const INTENT_PATHS: [&str; 4] = [
    "data/intent_output/response/speech/plain/speech",
    "data/intent_output/response/speech/speech",
    "data/intent_output/intent/name",
    "intent_output/intent/name",
];
/// `tts-end` url paths, in priority order.
const TTS_URL_PATHS: [&str; 4] = [
    "data/tts_output/url",
    "data/url",
    "data/tts/url",
    "tts_output/url",
];
/// `tts-end` mime type paths, in priority order.
const TTS_MIME_PATHS: [&str; 4] = [
    "data/tts_output/mime_type",
    "data/mime_type",
    "data/tts/mime_type",
    "tts_output/mime_type",
];

/// A request to run the pipeline for one verified utterance (plan S4.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PipelineRequest {
    /// Verified, keyword-stripped text (`verify::verify_utterance`'s output).
    pub text: String,
    /// Winner's HA `device_id`, so the pipeline sees the right devices/rooms. `None` for
    /// a source with no HA device (S7.1 uses `fallback_device_id`).
    pub device_id: Option<String>,
    /// Session id, reused across follow-up turns (AC7).
    pub conversation_id: Option<String>,
}

/// What a pipeline run produced (plan S4.3, R3: missing fields are `None` + a log).
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PipelineOutcome {
    /// A human-readable summary of `intent-end`, for the structured log.
    pub intent_summary: Option<String>,
    /// `tts-end` url — fetchable by the satellite (plan assumption, design-verified).
    pub tts_url: Option<String>,
    /// `tts-end` mime_type. The satellite's decoder set is pinned to this once U2 is
    /// dumped — frequently MP3, not only FLAC (plan S1.5, C4).
    pub mime_type: Option<String>,
}

/// A connected HA control plane (plan `[ha]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HaClient {
    /// `ha.url`, e.g. `ws://ha.example.lan:8123`.
    pub url: String,
    /// `ha.token`, a long-lived access token.
    pub token: String,
    /// `ha.pipeline_id` — the Assist pipeline `assist_pipeline/run` must name. A required,
    /// validated config key (`Config::validate`), carried here so every run sends it
    /// instead of `null` (R-C).
    pub pipeline_id: String,
}

impl HaClient {
    /// A client for the configured HA (no connection is made here).
    pub fn new(
        url: impl Into<String>,
        token: impl Into<String>,
        pipeline_id: impl Into<String>,
    ) -> Self {
        Self {
            url: url.into(),
            token: token.into(),
            pipeline_id: pipeline_id.into(),
        }
    }

    /// Run `assist_pipeline/run` at the intent stage and return what it produced
    /// (plan S4.3).
    ///
    /// Websocket auth with `self.token`, then the run with
    /// `start_stage = `[`START_STAGE_INTENT`]`, `end_stage = `[`END_STAGE_TTS`]`,
    /// `input.text`, `pipeline`, `device_id`, `conversation_id`. Parse `intent-end` into
    /// `intent_summary` (logged) and `tts-end` into `tts_url`/`mime_type`. A missing field
    /// degrades to `None` with a warning and TTS is still attempted (plan R3).
    pub async fn run_pipeline(&self, req: &PipelineRequest) -> anyhow::Result<PipelineOutcome> {
        self.run_pipeline_deadlined(req, RUN_DEADLINE).await
    }

    /// [`HaClient::run_pipeline`] under an explicit overall budget, so a test can prove
    /// the deadline bites without waiting a minute (finding HA-1).
    async fn run_pipeline_deadlined(
        &self,
        req: &PipelineRequest,
        budget: Duration,
    ) -> anyhow::Result<PipelineOutcome> {
        deadlined("the pipeline run", budget, self.run_pipeline_body(req)).await
    }

    async fn run_pipeline_body(&self, req: &PipelineRequest) -> anyhow::Result<PipelineOutcome> {
        let mut session = Session::connect(self, AUTH_DEADLINE).await?;
        // `ha.pipeline_id` is a required, validated config key and [`HaClient`] carries it
        // (R-C), so a run always names the configured pipeline rather than `null`.
        let run = run_command(req, Some(&self.pipeline_id));
        let id = session.send_command(run).await?;
        info!(
            conversation_id = req.conversation_id.as_deref().unwrap_or(""),
            device_id = req.device_id.as_deref().unwrap_or(""),
            "ha: pipeline run requested at the intent stage"
        );

        let mut out = PipelineOutcome::default();
        // A TTS url carried by an event that does **not name the tts stage**. Home Assistant
        // 2026.9.2 puts a `tts_output` in `run-start` itself (measured, pinned at
        // `tests/fixtures/ha-run-2026-11-02.json`); taking the url there ended the run before
        // `intent-end` was ever read, so the agent's answer never reached the log. Held aside
        // for the degraded paths below, never preferred over the stage-named event.
        let mut unconfirmed: Option<(String, Option<String>)> = None;
        let mut seen = 0usize;
        loop {
            if seen >= MAX_RUN_EVENTS {
                warn!(
                    events = seen,
                    "ha: pipeline run stream did not end; using what arrived"
                );
                settle_fallback(&mut out, unconfirmed.take());
                return Ok(out);
            }
            let Some(msg) = session.next_json().await? else {
                warn!("ha: socket closed before the pipeline run finished");
                settle_fallback(&mut out, unconfirmed.take());
                return Ok(out);
            };
            seen += 1;
            if !id_matches(&msg, id) {
                debug!("ha: ignoring a message for another id on the pipeline socket");
                continue;
            }
            match msg.get("type").and_then(Value::as_str) {
                Some("result") => {
                    // HA acknowledges the run immediately and then streams events.
                    check_result(&msg)?;
                }
                Some("event") => {
                    let Some(event) = msg.get("event") else {
                        warn!("ha: run event message has no `event` envelope; skipping it");
                        continue;
                    };
                    if !event.is_object() {
                        warn!(event = %event, "ha: run `event` is not an object; skipping it");
                        continue;
                    }
                    if event.get("type").and_then(Value::as_str) == Some("run-end") {
                        // Nothing better ever arrived: the held url, if there is one, is it.
                        settle_fallback(&mut out, unconfirmed.take());
                        return Ok(out);
                    }
                    if let Some(summary) = dig_str(event, &INTENT_PATHS) {
                        if out.intent_summary.is_none() {
                            info!(intent = summary, "ha: intent-end");
                            out.intent_summary = Some(summary.to_string());
                        }
                    }
                    match dig_str(event, &TTS_URL_PATHS) {
                        // The event names the tts stage: authoritative, and the run is over.
                        Some(url) if tts_named(event) => {
                            let mime = dig_str(event, &TTS_MIME_PATHS);
                            if mime.is_none() {
                                warn!(
                                    tried = TTS_MIME_PATHS.join(" | "),
                                    "ha: tts-end carries no mime_type"
                                );
                            }
                            info!(url, mime = mime.unwrap_or(""), "ha: tts-end");
                            out.tts_url = Some(url.to_string());
                            out.mime_type = mime.map(str::to_string);
                            // The pipeline's last useful stage; the caller plays it now.
                            return Ok(out);
                        }
                        // A url from an event that does not name the stage (`run-start`):
                        // hold it and keep reading, so intent-end still gets its turn.
                        Some(url) => {
                            if unconfirmed.is_none() {
                                unconfirmed = Some((
                                    url.to_string(),
                                    dig_str(event, &TTS_MIME_PATHS).map(str::to_string),
                                ));
                            }
                        }
                        None if tts_named(event) => warn!(
                            tried = TTS_URL_PATHS.join(" | "),
                            "ha: tts-end carries no url; TTS is skipped for this run"
                        ),
                        None => {}
                    }
                }
                other => debug!("ha: ignoring {other:?} on the pipeline socket"),
            }
        }
    }

    /// Play `url` on `entity_id` with `announce: `[`PLAY_ANNOUNCE`]` (plan S4.3, O2).
    ///
    /// Called with the winner's resolved `media_player` entity (its own `ha_entity`, else
    /// the group output fallback — S7.1). `mime` is the `tts-end` `mime_type`
    /// ([`PipelineOutcome::mime_type`], R-C); `None` falls back to
    /// [`MEDIA_CONTENT_TYPE`], which is what the frozen signature could only ever send.
    pub async fn play_media(
        &self,
        entity_id: &str,
        url: &str,
        mime: Option<&str>,
    ) -> anyhow::Result<()> {
        self.play_media_deadlined(entity_id, url, mime, COMMAND_DEADLINE)
            .await
    }

    /// [`HaClient::play_media`] under an explicit overall budget (finding HA-1): the
    /// connect, auth, command write and result read together may not exceed it, whatever
    /// frames the peer streams meanwhile.
    async fn play_media_deadlined(
        &self,
        entity_id: &str,
        url: &str,
        mime: Option<&str>,
        budget: Duration,
    ) -> anyhow::Result<()> {
        deadlined(
            "play_media",
            budget,
            self.play_media_body(entity_id, url, mime),
        )
        .await
    }

    async fn play_media_body(
        &self,
        entity_id: &str,
        url: &str,
        mime: Option<&str>,
    ) -> anyhow::Result<()> {
        let mut session = Session::connect(self, AUTH_DEADLINE).await?;
        let cmd = json!({
            "type": "call_service",
            "domain": "media_player",
            "service": "play_media",
            "service_data": {
                "entity_id": entity_id,
                "media_content_id": url,
                "media_content_type": mime.unwrap_or(MEDIA_CONTENT_TYPE),
                "announce": PLAY_ANNOUNCE,
            },
        });
        let id = session.send_command(cmd).await?;
        session.await_result(id).await?;
        info!(entity_id, url, "ha: play_media accepted");
        Ok(())
    }

    /// Stop current playback on `entity_id` (plan step 6, stop command).
    ///
    /// Mirrors [`HaClient::play_media`] minus the media content and `announce`
    /// fields: same connect/auth/command flow, deadline and error mapping.
    pub async fn media_stop(&self, entity_id: &str) -> anyhow::Result<()> {
        self.media_stop_deadlined(entity_id, COMMAND_DEADLINE)
            .await
    }

    /// [`HaClient::media_stop`] under an explicit overall budget (finding HA-1).
    async fn media_stop_deadlined(
        &self,
        entity_id: &str,
        budget: Duration,
    ) -> anyhow::Result<()> {
        deadlined("media_stop", budget, self.media_stop_body(entity_id)).await
    }

    async fn media_stop_body(&self, entity_id: &str) -> anyhow::Result<()> {
        let mut session = Session::connect(self, AUTH_DEADLINE).await?;
        let cmd = json!({
            "type": "call_service",
            "domain": "media_player",
            "service": "media_stop",
            "service_data": {
                "entity_id": entity_id,
            },
        });
        let id = session.send_command(cmd).await?;
        session.await_result(id).await?;
        info!(entity_id, "ha: media_stop accepted");
        Ok(())
    }

    /// Subscribe to state changes for `entities` and forward `(entity, muted)` to the
    /// coordinator (plan S4.5).
    ///
    /// Keeps the subscription alive across reconnects (R3), and on every (re)connect
    /// first syncs the entities' **current** state — `subscribe_events` alone cannot
    /// report a switch that flipped while we were down (finding HA-2). A source whose
    /// mute entity is on is suppressed by the coordinator, not here.
    pub async fn subscribe_mute_states(
        &self,
        entities: &[String],
        tx: mpsc::Sender<(String, bool)>,
    ) -> anyhow::Result<()> {
        self.subscribe_until_cancelled(entities, tx, BACKOFF_RESET_AFTER)
            .await
    }

    /// The reconnect loop behind [`HaClient::subscribe_mute_states`]. `stable_after` is
    /// the seam the HA-3 test drives: how long a connection must have been established
    /// before losing it resets the backoff instead of growing it.
    async fn subscribe_until_cancelled(
        &self,
        entities: &[String],
        tx: mpsc::Sender<(String, bool)>,
        stable_after: Duration,
    ) -> anyhow::Result<()> {
        let wanted: HashSet<&str> = entities.iter().map(String::as_str).collect();
        if wanted.is_empty() {
            // `subscribe_events` with no entity in mind asks HA for every state change in
            // the house. With nothing to watch that is traffic, a connection and a
            // reconnect loop bought for nothing, so hold the task until it is cancelled.
            info!("ha: no mute entities configured; not subscribing");
            tx.closed().await;
            return Ok(());
        }
        let mut attempt = 0u32;
        loop {
            let established = match self.subscribe_once(&wanted, &tx, stable_after).await {
                Stop::ReceiverGone => {
                    info!("ha: mute subscription ended (its receiver was dropped)");
                    return Ok(());
                }
                Stop::Disconnected { established } => {
                    warn!(attempt, established, "ha: mute subscription lost; reconnecting");
                    established
                }
                Stop::Failed { established, error } => {
                    warn!(error = %error, attempt, established, "ha: mute subscription failed; reconnecting");
                    established
                }
            };
            // Backoff exists to throttle *consecutive* failures. A connection that stayed
            // healthy and then died is not failure pressure (finding HA-3): without the
            // reset, a normally operating service accumulates the full 30 s backoff
            // purely from age and the occasional HA restart, and mute state stays
            // unavailable for half a minute after an ordinary blip.
            if established {
                attempt = 0;
            }
            let delay = backoff_delay(attempt);
            attempt = attempt.saturating_add(1);
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = tx.closed() => {
                    info!("ha: mute subscription ended (its receiver was dropped)");
                    return Ok(());
                }
            }
        }
    }

    /// One connection's worth of subscribing + streaming (plan S4.5).
    ///
    /// Every failure path reports whether the connection had been *established* (auth +
    /// the `subscribe_events` result) and then stayed up for `stable_after` before
    /// dying — that is what lets the caller's backoff reset rather than ratchet
    /// (finding HA-3). The setup round trip is bounded by [`COMMAND_DEADLINE`]; the
    /// event stream keeps only the per-frame bound, because a quiet house is healthy.
    async fn subscribe_once(
        &self,
        wanted: &HashSet<&str>,
        tx: &mpsc::Sender<(String, bool)>,
        stable_after: Duration,
    ) -> Stop {
        let setup = async {
            let mut session = Session::connect(self, AUTH_DEADLINE).await?;
            let id = session
                .send_command(json!({"type": "subscribe_events", "event_type": "state_changed"}))
                .await?;
            session.await_result(id).await?;
            // A change feed cannot report what changed before it existed — at startup, or
            // during every disconnect and backoff cycle since (finding HA-2). Ask for the
            // current states too. Subscribe first, snapshot second, and read the result
            // inside the loop below rather than blocking on it here: a change racing the
            // snapshot is then ordered after it (one socket, in order), and a peer that
            // never answers `get_states` costs the snapshot but never the change feed.
            // A failed snapshot request must never cost the change feed: an older HA, or
            // a peer that closes the socket in the moment between the ack and this write
            // (with our events already queued behind it), degrades to "no snapshot, sync
            // on the next change" — the behaviour this module had before HA-2.
            let snapshot_id = session
                .send_command(json!({"type": "get_states"}))
                .await
                .map_err(|err| {
                    debug!(error = %err, "ha: could not request the current states; mute state syncs on the next change");
                })
                .ok();
            anyhow::Ok((session, snapshot_id))
        };
        let (mut session, snapshot_id) =
            match deadlined("subscribing to state changes", COMMAND_DEADLINE, setup).await {
                Ok(ready) => ready,
                Err(error) => {
                    return Stop::Failed {
                        established: false,
                        error,
                    }
                }
            };
        info!(entities = wanted.len(), "ha: subscribing to state changes");
        let established_at = Instant::now();
        // At an exit point after this: the connection demonstrably worked, so the caller
        // starts the next backoff from the floor.
        let established = || established_at.elapsed() >= stable_after;
        loop {
            let msg = tokio::select! {
                _ = tx.closed() => return Stop::ReceiverGone,
                msg = session.next_json() => match msg {
                    Ok(msg) => msg,
                    Err(error) => {
                        return Stop::Failed { established: established(), error };
                    }
                },
            };
            let Some(msg) = msg else {
                return Stop::Disconnected { established: established() };
            };
            match msg.get("type").and_then(Value::as_str) {
                Some("result") if snapshot_id.is_some_and(|sid| id_matches(&msg, sid)) => {
                    if let Err(err) = check_result(&msg) {
                        warn!(error = %err, "ha: get_states was rejected; mute state syncs on the next change");
                        continue;
                    }
                    let Some(states) = msg.get("result").and_then(Value::as_array) else {
                        warn!("ha: get_states carried no state list; mute state syncs on the next change");
                        continue;
                    };
                    let mut synced = 0usize;
                    for state in states {
                        if let Some((entity, muted)) = snapshot_change(state, wanted) {
                            synced += 1;
                            if tx.send((entity, muted)).await.is_err() {
                                return Stop::ReceiverGone;
                            }
                        }
                    }
                    info!(entities = synced, "ha: mute states synced from the current state");
                }
                Some("event") => {
                    let Some((entity, muted)) = mute_change(&msg, wanted) else {
                        continue;
                    };
                    if tx.send((entity, muted)).await.is_err() {
                        return Stop::ReceiverGone;
                    }
                }
                other => debug!("ha: ignoring {other:?} on the state subscription"),
            }
        }
    }
}

/// Why a subscription attempt ended, and whether the connection had been *established*
/// (authenticated, `subscribe_events` accepted) and stayed up long enough to count as
/// healthy: the caller resets its reconnect backoff on a healthy loss and grows it on
/// every other kind (finding HA-3).
#[derive(Debug)]
enum Stop {
    /// The consumer dropped its receiver: shut down gracefully.
    ReceiverGone,
    /// The socket went away: reconnect after a backoff.
    Disconnected { established: bool },
    /// The attempt ended with an error: reconnect after a backoff.
    Failed { established: bool, error: anyhow::Error },
}

/// Enforce `stage`'s overall deadline around `work` (finding HA-1). The per-frame
/// [`READ_TIMEOUT`] restarts on every received frame — including a WebSocket ping — so a
/// peer that acknowledges a command and then only pings holds the call open forever;
/// nothing can extend this bound. The error names the stage and its budget so the log
/// line says which stage to go look at.
async fn deadlined<T>(
    stage: &str,
    budget: Duration,
    work: impl Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(budget, work).await.unwrap_or_else(|_| {
        Err(anyhow!(
            "ha: {stage} exceeded its overall {} ms deadline",
            budget.as_millis()
        ))
    })
}

/// A live, authenticated HA WebSocket connection plus the request-id counter.
struct Session {
    ws: Ws,
    next_id: u64,
}

type Ws = tokio_tungstenite::WebSocketStream<TcpStream>;

impl Session {
    /// Connect, complete the auth handshake, and hand back a ready session — the whole
    /// stage bounded by `auth_budget` (finding HA-1), so a peer that finishes the TCP and
    /// websocket handshakes and then only pings still fails at the deadline. The
    /// per-frame [`READ_TIMEOUT`] bounds inside stay as defence in depth: they are the
    /// inner bound and produce the more specific message when they fire first.
    async fn connect(client: &HaClient, auth_budget: Duration) -> Result<Self> {
        deadlined(
            "authenticating with Home Assistant",
            auth_budget,
            async {
                let (tcp_addr, ws_url) = socket_parts(&client.url)?;
                let tcp = tokio::time::timeout(READ_TIMEOUT, TcpStream::connect(tcp_addr.as_str()))
                    .await
                    .with_context(|| format!("ha: connecting to {tcp_addr} timed out"))?
                    .with_context(|| format!("ha: connecting to {tcp_addr} failed"))?;
                let handshake = tokio::time::timeout(
                    READ_TIMEOUT,
                    tokio_tungstenite::client_async(ws_url.as_str(), tcp),
                )
                .await
                .with_context(|| format!("ha: websocket handshake with {ws_url} timed out"))?;
                let (ws, _response) = handshake
                    .with_context(|| format!("ha: websocket handshake with {ws_url} failed"))?;
                let mut session = Session { ws, next_id: 0 };
                session.authenticate(&client.token).await?;
                Ok(session)
            },
        )
        .await
    }

    /// `auth_required` → `auth` → `auth_ok`, per HA's WebSocket API.
    async fn authenticate(&mut self, token: &str) -> Result<()> {
        loop {
            let Some(msg) = self.next_json().await? else {
                bail!("ha: connection closed before auth_required");
            };
            match msg.get("type").and_then(Value::as_str) {
                Some("auth_required") => {
                    // The greeting also carries `ha_version`, which this client does not
                    // gate on: the tolerant reader is the compatibility story.
                    self.send_value(&json!({"type": "auth", "access_token": token}))
                        .await?;
                }
                Some("auth_ok") => {
                    debug!("ha: authenticated");
                    return Ok(());
                }
                Some("auth_invalid") => {
                    let message = msg.get("message").and_then(Value::as_str).unwrap_or("");
                    bail!("ha: auth rejected by Home Assistant: {message}");
                }
                other => warn!(r#type = ?other, "ha: ignoring message during auth handshake"),
            }
        }
    }

    /// Send a bare message (no `id`), e.g. the `auth` frame. The write is bounded too
    /// (finding HA-1): a peer that has stopped reading must not wedge the stage that
    /// issued the frame — a write is the half of the round trip no read timeout covers.
    async fn send_value(&mut self, value: &Value) -> Result<()> {
        let frame = Message::Text(value.to_string());
        let sent = tokio::time::timeout(WRITE_TIMEOUT, self.ws.send(frame))
            .await
            .map_err(|_| {
                anyhow!(
                    "ha: sending a websocket frame exceeded the {} ms write bound",
                    WRITE_TIMEOUT.as_millis()
                )
            })?;
        sent.context("ha: sending a websocket frame failed")
    }

    /// Send a command, stamping it with the next request id.
    async fn send_command(&mut self, mut cmd: Value) -> Result<u64> {
        self.next_id = self.next_id.saturating_add(1);
        let id = self.next_id;
        match &mut cmd {
            Value::Object(map) => {
                map.insert("id".to_string(), json!(id));
            }
            _ => bail!("ha: command must be a JSON object, got {cmd}"),
        }
        self.send_value(&cmd).await?;
        Ok(id)
    }

    /// Pump until the `result` for `id` arrives, ignoring streamed events.
    async fn await_result(&mut self, id: u64) -> Result<()> {
        loop {
            let Some(msg) = self.next_json().await? else {
                bail!("ha: connection closed before the result for command {id}");
            };
            if !id_matches(&msg, id) {
                debug!("ha: ignoring a message for another id");
                continue;
            }
            match msg.get("type").and_then(Value::as_str) {
                Some("result") => return check_result(&msg),
                Some("event") | Some("pong") => {
                    debug!("ha: ignoring an event while awaiting a result")
                }
                other => warn!("ha: ignoring {other:?} while awaiting a result"),
            }
        }
    }

    /// The next JSON text frame, or `None` once the server closed cleanly.
    ///
    /// Non-JSON text, binary, ping/pong and `Frame` payloads are skipped with a log: the
    /// tolerant reader never turns a stray frame into an error (plan R3).
    async fn next_json(&mut self) -> Result<Option<Value>> {
        loop {
            let frame = match tokio::time::timeout(READ_TIMEOUT, self.ws.next()).await {
                Err(_) => bail!(
                    "ha: timed out after {}s waiting for a message",
                    READ_TIMEOUT.as_secs()
                ),
                Ok(None) => return Ok(None),
                Ok(Some(Err(err))) => return Err(anyhow!("ha: websocket error: {err}")),
                Ok(Some(Ok(frame))) => frame,
            };
            match frame {
                Message::Text(text) => match serde_json::from_str::<Value>(&text) {
                    Ok(value) => return Ok(Some(value)),
                    Err(err) => warn!(error = %err, "ha: ignoring a non-JSON text frame"),
                },
                Message::Close(_) => return Ok(None),
                other => debug!(frame = ?other.is_text(), "ha: ignoring a non-text frame"),
            }
        }
    }
}

/// Build the `assist_pipeline/run` command (the `id` is stamped by [`Session`]).
fn run_command(req: &PipelineRequest, pipeline: Option<&str>) -> Value {
    json!({
        "type": "assist_pipeline/run",
        "start_stage": START_STAGE_INTENT,
        "end_stage": END_STAGE_TTS,
        "input": { "text": req.text },
        "pipeline": pipeline,
        "device_id": req.device_id,
        "conversation_id": req.conversation_id,
    })
}

/// Does this message belong to request `id`?
fn id_matches(msg: &Value, id: u64) -> bool {
    msg.get("id").and_then(Value::as_u64) == Some(id)
}

/// HA answers `{"type":"result","success":bool,"error":{…}}`.
fn check_result(msg: &Value) -> Result<()> {
    match msg.get("success").and_then(Value::as_bool) {
        Some(false) => {
            let code = msg
                .get("error")
                .and_then(|e| e.get("code"))
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let message = msg
                .get("error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("");
            Err(anyhow!("ha: command rejected ({code}): {message}"))
        }
        _ => Ok(()),
    }
}

/// Walk a `/`-separated path through nested objects, no indexing panics.
fn dig<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cursor = value;
    for key in path.split('/') {
        cursor = cursor.get(key)?;
    }
    Some(cursor)
}

/// The first non-empty string among `paths`, in priority order.
fn dig_str<'a>(value: &'a Value, paths: &[&'static str]) -> Option<&'a str> {
    paths.iter().find_map(|path| {
        dig(value, path)
            .and_then(Value::as_str)
            .filter(|found| !found.is_empty())
    })
}

/// Does this event **name** the TTS stage — the only thing that makes a url authoritative
/// and the run finished?
///
/// Deliberately *not* "does it contain a `tts_output`": Home Assistant 2026.9.2 carries one
/// in `run-start` (pinned in `tests/fixtures/ha-run-2026-11-02.json`), and treating that as
/// the trigger ended the run on the first event, so `intent-end` was never read and the
/// agent's answer never appeared in the log. The two shapes that name the stage — the real
/// `type: "tts-end"` and the older `data.stage: "tts"` — are what qualify.
fn tts_named(event: &Value) -> bool {
    if event.get("type").and_then(Value::as_str) == Some("tts-end") {
        return true;
    }
    event
        .get("data")
        .and_then(|data| data.get("stage"))
        .and_then(Value::as_str)
        == Some("tts")
}

/// Adopt a TTS url that came from an event which does not name the stage, on the paths
/// where no `tts-end` ever arrived (stream cap, socket closed, run-end without one).
/// Logged as a warning because it is the degraded reading of R3, not the normal one.
fn settle_fallback(out: &mut PipelineOutcome, unconfirmed: Option<(String, Option<String>)>) {
    if out.tts_url.is_some() {
        return;
    }
    let Some((url, mime)) = unconfirmed else {
        return;
    };
    warn!(url = %url, "ha: no event named the tts stage; using the tts_output the run announced");
    out.tts_url = Some(url);
    out.mime_type = mime;
}

/// A `state_changed` event for a **subscribed** entity, as `(entity, muted)`.
///
/// Membership is checked before the state is parsed. HA streams every state change in the
/// house, and most of them (a climate's `heat`, a person's `away`, a sensor's `20.3`) are
/// not on/off — parsing first turns the whole household into a WARN stream and buries the
/// warning that is actually worth seeing, which is a *subscribed* mute entity reporting
/// something unintelligible.
fn mute_change(msg: &Value, wanted: &HashSet<&str>) -> Option<(String, bool)> {
    let event = msg.get("event")?;
    // HA puts `event_type` at the top of the event; tolerate it under `data` too.
    let event_type = event.get("event_type").and_then(Value::as_str).or_else(|| {
        event
            .get("data")
            .and_then(|data| data.get("event_type"))
            .and_then(Value::as_str)
    });
    if event_type != Some("state_changed") {
        return None;
    }
    let data = event.get("data")?;
    let entity = data.get("entity_id").and_then(Value::as_str)?;
    if entity.is_empty() {
        warn!("ha: state_changed without an entity_id; skipping it");
        return None;
    }
    if !wanted.contains(entity) {
        return None;
    }
    let muted = data.get("new_state").and_then(muted_from_state)?;
    Some((entity.to_string(), muted))
}

/// One `get_states` entry for a **subscribed** entity, as `(entity, muted)` — the
/// snapshot twin of [`mute_change`]. The shapes differ: `get_states` hands back the
/// state object itself (`{"entity_id": …, "state": …, "attributes": …}`) where a
/// `state_changed` event wraps it under `event.data.new_state`, so only the mute
/// reading is shared. Membership is filtered first, for the same reason as
/// [`mute_change`]: a house-wide snapshot must not turn unrelated entities into WARNs.
fn snapshot_change(state: &Value, wanted: &HashSet<&str>) -> Option<(String, bool)> {
    let entity = state.get("entity_id").and_then(Value::as_str)?;
    if entity.is_empty() || !wanted.contains(entity) {
        return None;
    }
    let muted = muted_from_state(state)?;
    Some((entity.to_string(), muted))
}

/// Mute state from a `new_state` object: `media_player` mute attribute first, then the
/// plain on/off state a `switch` mute entity reports (S4.5's config uses switches).
fn muted_from_state(new_state: &Value) -> Option<bool> {
    if let Some(muted) = new_state
        .get("attributes")
        .and_then(|attributes| attributes.get("is_volume_muted"))
        .and_then(Value::as_bool)
    {
        return Some(muted);
    }
    match new_state.get("state") {
        Some(Value::Bool(muted)) => Some(*muted),
        Some(Value::String(state)) => {
            let state = state.as_str();
            if MUTE_ON.contains(&state) {
                Some(true)
            } else if MUTE_OFF.contains(&state) {
                Some(false)
            } else {
                warn!(
                    state,
                    "ha: mute entity state is neither on nor off; skipping it"
                );
                None
            }
        }
        _ => {
            warn!(
                tried = "new_state.attributes.is_volume_muted | new_state.state",
                "ha: state_changed without a readable mute state"
            );
            None
        }
    }
}

/// Exponential backoff with a floor and a cap (plan R3).
fn backoff_delay(attempt: u32) -> Duration {
    // 250 ms · 2^attempt, capped; the shift is clamped so it can never overflow.
    let factor = 1u64.checked_shl(attempt.min(16)).unwrap_or(u64::MAX);
    let ms = RECONNECT_BACKOFF_MIN_MS
        .saturating_mul(factor)
        .min(RECONNECT_BACKOFF_MAX_MS);
    Duration::from_millis(ms)
}

/// `ha.url` → `(tcp authority for connect, ws url to handshake)`.
///
/// `http(s)://` is accepted as an alias for `ws(s)://` (operators copy the HA URL); the
/// default port and `/api/websocket` path are filled in. TLS is not supported: the
/// dependency has no TLS feature enabled, so `wss`/`https` is reported, not silently
/// downgraded.
fn socket_parts(url: &str) -> Result<(String, String)> {
    let url = url.trim();
    let (scheme, rest) = url
        .split_once("://")
        .ok_or_else(|| anyhow!("ha: url {url:?} must start with ws://, http:// or https://"))?;
    let rest = rest.trim_end_matches('/');
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, WS_PATH.to_string()),
    };
    if authority.is_empty() {
        bail!("ha: url {url:?} has no host");
    }
    let ws_scheme = match scheme.to_ascii_lowercase().as_str() {
        "ws" | "http" => "ws",
        "wss" | "https" => bail!(
            "ha: url {url:?} needs TLS, which this build does not enable; use ws:// on the LAN"
        ),
        other => bail!("ha: url {url:?} has unsupported scheme {other:?}"),
    };
    let tcp_addr = if authority.contains(':') {
        authority.to_string()
    } else {
        format!("{authority}:{DEFAULT_PORT}")
    };
    Ok((tcp_addr, format!("{ws_scheme}://{authority}{path}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use futures_util::stream::SplitStream;
    use tokio::net::TcpListener;
    use tokio_tungstenite::accept_async;
    use tracing_subscriber::layer::SubscriberExt;

    // ---- mock Home Assistant ------------------------------------------------------

    /// One scripted step on a mock HA connection.
    #[derive(Clone, Debug)]
    enum Step {
        /// Send this frame to the client.
        Send(Value),
        /// Wait for one client text frame and record it.
        Await,
        /// Close the socket (simulates an HA restart).
        Close,
        /// Keep recording client frames until the client goes away.
        Hold,
        /// Ping forever, faster than any read timeout, and never send a JSON frame: the
        /// wedged-but-chatty peer HA-1 is about.
        Pings,
        /// Hold the socket open and silent for this long.
        Sleep(Duration),
    }

    type ServerStream = SplitStream<Ws>;

    struct MockHa {
        url: String,
        seen: Arc<Mutex<Vec<Vec<Value>>>>,
        _accept: tokio::task::JoinHandle<()>,
    }

    impl MockHa {
        fn frames(&self, connection: usize) -> Vec<Value> {
            self.seen
                .lock()
                .unwrap()
                .get(connection)
                .cloned()
                .unwrap_or_default()
        }
    }

    /// Serve `scripts[connection]` on connection *n*, on an ephemeral loopback port.
    async fn mock_ha(scripts: Vec<Vec<Step>>) -> MockHa {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock ha");
        let addr = listener.local_addr().expect("mock ha addr");
        let seen = Arc::new(Mutex::new(vec![Vec::new(); scripts.len().max(1)]));
        let scripts = Arc::new(scripts);
        let accept = tokio::spawn({
            let seen = Arc::clone(&seen);
            async move {
                let mut connection = 0usize;
                while let Ok((tcp, _peer)) = listener.accept().await {
                    {
                        let mut seen = seen.lock().unwrap();
                        while seen.len() <= connection {
                            seen.push(Vec::new());
                        }
                    }
                    let script = scripts.get(connection).cloned().unwrap_or_default();
                    tokio::spawn(drive(tcp, script, Arc::clone(&seen), connection));
                    connection += 1;
                }
            }
        });
        MockHa {
            url: format!("ws://{addr}"),
            seen,
            _accept: accept,
        }
    }

    async fn drive(
        tcp: TcpStream,
        script: Vec<Step>,
        seen: Arc<Mutex<Vec<Vec<Value>>>>,
        idx: usize,
    ) {
        let Ok(ws) = accept_async(tcp).await else {
            return;
        };
        let (mut sink, mut stream) = ws.split();
        for step in script {
            match step {
                Step::Send(value) => {
                    if sink.send(Message::Text(value.to_string())).await.is_err() {
                        return;
                    }
                }
                Step::Await => {
                    if !await_frame(&mut stream, &seen, idx).await {
                        return;
                    }
                }
                Step::Close => {
                    let _ = sink.close().await;
                    return;
                }
                Step::Hold => {
                    while await_frame(&mut stream, &seen, idx).await {}
                    return;
                }
                Step::Pings => loop {
                    if sink.send(Message::Ping(Vec::new())).await.is_err() {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                },
                Step::Sleep(hold) => {
                    tokio::time::sleep(hold).await;
                }
            }
        }
    }

    /// Record the client's next text frame; `false` once the peer is gone.
    async fn await_frame(
        stream: &mut ServerStream,
        seen: &Arc<Mutex<Vec<Vec<Value>>>>,
        idx: usize,
    ) -> bool {
        while let Some(Ok(frame)) = stream.next().await {
            if let Message::Text(text) = frame {
                let value = serde_json::from_str(&text).unwrap_or(Value::String(text.clone()));
                seen.lock().unwrap()[idx].push(value);
                return true;
            }
        }
        false
    }

    // ---- helpers ------------------------------------------------------------------

    /// The pipeline every test client names: `ha.pipeline_id` is a required, validated
    /// key and [`HaClient`] carries it to `assist_pipeline/run` (R-C).
    const PIPELINE: &str = "01J9ZEXAMPLE";

    fn ha(url: &str) -> HaClient {
        HaClient::new(url, "tok", PIPELINE)
    }

    /// Fail instead of hanging if a seam deadlocks.
    async fn within<F: Future>(label: &str, future: F) -> F::Output {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .unwrap_or_else(|_| panic!("{label} timed out"))
    }

    fn request(text: &str) -> PipelineRequest {
        PipelineRequest {
            text: text.to_string(),
            device_id: Some("aa:bb:cc:dd:ee:ff".to_string()),
            conversation_id: Some("conv-1".to_string()),
        }
    }

    /// `auth_required` → (client `auth`) → `auth_ok`.
    fn auth_steps() -> Vec<Step> {
        vec![
            Step::Send(json!({"type": "auth_required", "ha_version": "2024.6.0"})),
            Step::Await,
            Step::Send(json!({"type": "auth_ok", "ha_version": "2024.6.0"})),
        ]
    }

    fn run_ack(id: u64) -> Step {
        Step::Send(
            json!({"id": id, "type": "result", "success": true, "result": {"response_type": "result"}}),
        )
    }

    fn run_event(id: u64, event: Value) -> Step {
        Step::Send(json!({"id": id, "type": "event", "event": event}))
    }

    fn run_end(id: u64) -> Step {
        run_event(id, json!({"type": "run-end", "data": {}}))
    }

    fn state_changed(entity: &str, state: &str) -> Step {
        Step::Send(state_changed_msg(entity, state))
    }

    /// Collect `WARN` events emitted on this thread.
    struct WarnCount(Arc<AtomicUsize>);

    impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for WarnCount {
        fn on_event(
            &self,
            event: &tracing::Event<'_>,
            _ctx: tracing_subscriber::layer::Context<'_, S>,
        ) {
            if *event.metadata().level() == tracing::Level::WARN {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    fn capture_warnings() -> (Arc<AtomicUsize>, tracing::subscriber::DefaultGuard) {
        let count = Arc::new(AtomicUsize::new(0));
        let subscriber = tracing_subscriber::registry().with(WarnCount(Arc::clone(&count)));
        (count, tracing::subscriber::set_default(subscriber))
    }

    // ---- auth ---------------------------------------------------------------------

    #[tokio::test]
    async fn auth_frame_is_exact() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![Step::Await, run_ack(1), run_end(1), Step::Close],
        ]
        .concat()])
        .await;
        let outcome = within(
            "run_pipeline",
            ha(&mock.url).run_pipeline(&request("hello")),
        )
        .await
        .expect("auth must succeed");
        assert_eq!(outcome, PipelineOutcome::default());

        let frames = mock.frames(0);
        assert!(frames.len() >= 2, "expected auth + run, got {frames:?}");
        assert_eq!(frames[0], json!({"type": "auth", "access_token": "tok"}));
        assert_eq!(frames[1]["type"], json!("assist_pipeline/run"));
    }

    #[tokio::test]
    async fn auth_rejected_is_error() {
        let mock = mock_ha(vec![vec![
            Step::Send(json!({"type": "auth_required", "ha_version": "2024.6.0"})),
            Step::Await,
            Step::Send(json!({"type": "auth_invalid", "message": "Invalid access token"})),
            Step::Close,
        ]])
        .await;
        let err = within("run_pipeline", ha(&mock.url).run_pipeline(&request("hi")))
            .await
            .expect_err("rejected auth must be an error");
        let text = err.to_string();
        assert!(text.contains("auth"), "{text}");
        assert!(text.contains("Invalid access token"), "{text}");
        assert_eq!(
            mock.frames(0)[0],
            json!({"type": "auth", "access_token": "tok"})
        );
    }

    // ---- assist_pipeline/run ------------------------------------------------------

    #[tokio::test]
    async fn run_pipeline_sends_full_command() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![Step::Await, run_ack(1), run_end(1), Step::Close],
        ]
        .concat()])
        .await;
        within(
            "run_pipeline",
            ha(&mock.url).run_pipeline(&request("what's the weather")),
        )
        .await
        .expect("run");

        let frames = mock.frames(0);
        // Exact object equality: every required key, and nothing else.
        assert_eq!(
            frames[1],
            json!({
                "id": 1,
                "type": "assist_pipeline/run",
                "start_stage": "intent",
                "end_stage": "tts",
                "input": {"text": "what's the weather"},
                "pipeline": PIPELINE,
                "device_id": "aa:bb:cc:dd:ee:ff",
                "conversation_id": "conv-1",
            })
        );
        assert_eq!(frames[1]["start_stage"], json!(START_STAGE_INTENT));
        assert_eq!(frames[1]["end_stage"], json!(END_STAGE_TTS));
    }

    #[tokio::test]
    async fn run_pipeline_extracts_tts_end_media() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                run_ack(1),
                run_event(1, json!({"type": "run-start", "data": {"run_id": "run-1"}})),
                run_event(
                    1,
                    json!({
                        "type": "stage",
                        "data": {
                            "stage": "intent",
                            "intent_output": {
                                "response": {"speech": {"plain": {"speech": "It is 21 degrees."}}},
                                "intent": {"name": "HassGetWeather"},
                            },
                        },
                    }),
                ),
                run_event(
                    1,
                    json!({
                        "type": "stage",
                        "data": {
                            "stage": "tts",
                            "tts_output": {
                                "url": "/api/tts_proxy/abc_123.flac",
                                "mime_type": "audio/flac",
                            },
                        },
                    }),
                ),
                run_end(1),
            ],
        ]
        .concat()])
        .await;
        let outcome = within(
            "run_pipeline",
            ha(&mock.url).run_pipeline(&request("what's the weather")),
        )
        .await
        .expect("run");
        assert_eq!(outcome.intent_summary.as_deref(), Some("It is 21 degrees."));
        assert_eq!(
            outcome.tts_url.as_deref(),
            Some("/api/tts_proxy/abc_123.flac")
        );
        assert_eq!(outcome.mime_type.as_deref(), Some("audio/flac"));
    }

    /// The same bytes, one layer down: does the tolerant reader's own accessor find the
    /// agent's speech in the captured event? Split out from the run test so a failure there
    /// says "the paths" and a failure here says "the loop".
    #[test]
    fn accessor_finds_the_captured_intent_speech() {
        let captured: Value =
            serde_json::from_str(include_str!("../tests/fixtures/ha-run-2026-11-02.json"))
                .expect("the captured run is valid JSON");
        let events = captured["events"].as_array().expect("an events array");
        let intent = events
            .iter()
            .find(|e| e["type"] == json!("intent-end"))
            .expect("the capture contains an intent-end event");
        let speech = intent["data"]["intent_output"]["response"]["speech"]["plain"]["speech"]
            .as_str()
            .expect("the capture carries the agent's speech as a string");
        assert!(!speech.is_empty(), "and it is not empty");
        assert_eq!(
            dig_str(intent, &INTENT_PATHS),
            Some(speech),
            "the accessor must agree with the captured structure"
        );
    }

    /// U2, closed by capture rather than by inference. Every event below is verbatim from
    /// Home Assistant 2026.9.2's reply to `assist_pipeline/run` with
    /// `start_stage: "intent"`, dumped from this household and stored at
    /// `tests/fixtures/ha-run-2026-11-02.json` (provenance and the command that produced
    /// it: `docs/design/TEST-NOTES.md`, U2).
    ///
    /// This test exists because every other run test in this file feeds the
    /// *spec-derived* shape the mock here invented — `{"type": "stage", "data": {"stage":
    /// "tts", …}}` — while the real envelope names the stage in the event's own `type`
    /// and carries the media under `data.tts_output` plus fields (`media_id`, `token`)
    /// the mock never had. The tolerant reader survives both today, but only this case
    /// pins the one shape that actually arrives in the house: narrowing it to the
    /// invented form would keep every other test here green while breaking production.
    #[tokio::test]
    async fn run_pipeline_parses_the_captured_live_envelope() {
        let captured: Value =
            serde_json::from_str(include_str!("../tests/fixtures/ha-run-2026-11-02.json"))
                .expect("the captured run is valid JSON");
        let events = captured["events"]
            .as_array()
            .expect("the captured run carries an events array");
        assert_eq!(captured["ha_version"], json!("2026.9.2"), "pinned capture");

        let mut steps = vec![auth_steps(), vec![Step::Await, run_ack(1)]];
        for event in events {
            // Verbatim: no synthesized run-end, no re-shaped stage events.
            steps.push(vec![run_event(1, event.clone())]);
        }
        steps.push(vec![Step::Close]);
        let mock = mock_ha(vec![steps.concat()]).await;

        let outcome = within(
            "run_pipeline",
            ha(&mock.url).run_pipeline(&request("what is the weather")),
        )
        .await
        .expect("the captured envelope must parse");
        assert!(
            outcome.tts_url.is_some() && outcome.intent_summary.is_some(),
            "the captured run must yield both the agent's speech and the TTS url: {outcome:?}"
        );
        assert!(
            outcome
                .intent_summary
                .as_deref()
                .unwrap()
                .starts_with("It's clear and calm out"),
            "the agent's speech is at data/intent_output/response/speech/plain/speech"
        );
        assert_eq!(
            outcome.tts_url.as_deref(),
            Some("/api/tts_proxy/0W0_xKCRLu3ZcEM0qDSrNg.mp3")
        );
        // R-C, measured rather than assumed: this household's TTS answers in MP3.
        assert_eq!(outcome.mime_type.as_deref(), Some("audio/mpeg"));
    }

    #[tokio::test]
    async fn run_pipeline_missing_tts_url_is_none_and_warns() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                run_ack(1),
                // A `tts` stage with no url and a mistyped mime_type: warn, don't fail.
                run_event(
                    1,
                    json!({"type": "stage", "data": {"stage": "tts", "tts_output": {"mime_type": 42}}}),
                ),
                run_end(1),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        let (warnings, _guard) = capture_warnings();
        let outcome = within("run_pipeline", ha(&mock.url).run_pipeline(&request("hi")))
            .await
            .expect("a partial tts-end must not fail the run");
        assert_eq!(outcome.tts_url, None);
        assert_eq!(outcome.mime_type, None);
        assert_eq!(outcome.intent_summary, None);
        assert!(
            warnings.load(Ordering::Relaxed) >= 1,
            "a tts-end without a url must warn"
        );
    }

    #[tokio::test]
    async fn run_pipeline_malformed_event_is_none_not_error() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                run_ack(1),
                run_event(1, json!("not an object")),
                Step::Send(json!({"id": 1, "type": "event"})),
                run_event(1, json!({"type": "stage", "data": null})),
                run_end(1),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        let (warnings, _guard) = capture_warnings();
        let outcome = within("run_pipeline", ha(&mock.url).run_pipeline(&request("hi")))
            .await
            .expect("malformed events must not fail the run");
        assert_eq!(outcome, PipelineOutcome::default());
        assert!(
            warnings.load(Ordering::Relaxed) >= 2,
            "each malformed run event must warn"
        );
    }

    #[tokio::test]
    async fn run_pipeline_result_error_is_error() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({
                    "id": 1,
                    "type": "result",
                    "success": false,
                    "error": {"code": "unknown_pipeline", "message": "Pipeline not found"},
                })),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        let err = within("run_pipeline", ha(&mock.url).run_pipeline(&request("hi")))
            .await
            .expect_err("a rejected run must be an error");
        assert!(err.to_string().contains("unknown_pipeline"), "{err}");
    }

    // ---- play_media ---------------------------------------------------------------

    #[tokio::test]
    async fn play_media_sends_call_service_with_announce() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        within(
            "play_media",
            ha(&mock.url).play_media(
                "media_player.kitchen_satellite",
                "/api/tts_proxy/abc.flac",
                None,
            ),
        )
        .await
        .expect("play_media");

        assert_eq!(
            mock.frames(0)[1],
            json!({
                "id": 1,
                "type": "call_service",
                "domain": "media_player",
                "service": "play_media",
                "service_data": {
                    "entity_id": "media_player.kitchen_satellite",
                    "media_content_id": "/api/tts_proxy/abc.flac",
                    "media_content_type": MEDIA_CONTENT_TYPE,
                    "announce": true,
                },
            })
        );
        assert_eq!(
            mock.frames(0)[1]["service_data"]["announce"],
            json!(PLAY_ANNOUNCE)
        );
    }

    #[tokio::test]
    async fn media_stop_sends_call_service_without_announce() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        within(
            "media_stop",
            ha(&mock.url).media_stop("media_player.kitchen_satellite"),
        )
        .await
        .expect("media_stop");

        assert_eq!(
            mock.frames(0)[1],
            json!({
                "id": 1,
                "type": "call_service",
                "domain": "media_player",
                "service": "media_stop",
                "service_data": {
                    "entity_id": "media_player.kitchen_satellite",
                },
            })
        );
        let frames = mock.frames(0);
        let service_data = &frames[1]["service_data"];
        assert!(
            service_data.get("media_content_type").is_none(),
            "media_stop must not send media content type: {service_data}"
        );
        assert!(
            service_data.get("announce").is_none(),
            "media_stop must not send announce: {service_data}"
        );
        assert!(
            service_data.get("media_content_id").is_none(),
            "media_stop must not send media content: {service_data}"
        );
    }

    /// R-C: `tts-end`'s `mime_type` must reach `media_content_type`; the frozen
    /// two-argument form could only ever send [`MEDIA_CONTENT_TYPE`].
    #[tokio::test]
    async fn play_media_carries_the_tts_mime_type() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        within(
            "play_media",
            ha(&mock.url).play_media(
                "media_player.kitchen_satellite",
                "/api/tts_proxy/abc.mp3",
                Some("audio/mpeg"),
            ),
        )
        .await
        .expect("play_media");

        assert_eq!(
            mock.frames(0)[1]["service_data"]["media_content_type"],
            json!("audio/mpeg")
        );
    }

    #[tokio::test]
    async fn play_media_error_result_is_error() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({
                    "id": 1,
                    "type": "result",
                    "success": false,
                    "error": {"code": "not_found", "message": "Entity not found"},
                })),
                Step::Close,
            ],
        ]
        .concat()])
        .await;
        let err = within(
            "play_media",
            ha(&mock.url).play_media("media_player.nope", "/x.flac", None),
        )
        .await
        .expect_err("a rejected play_media must be an error");
        assert!(err.to_string().contains("not_found"), "{err}");
    }

    // ---- mute subscription --------------------------------------------------------

    #[tokio::test]
    async fn subscribe_pushes_only_requested_entities() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                state_changed("switch.kitchen_mute", "on"),
                state_changed("switch.hallway_mute", "on"),
                Step::Send(json!({
                    "id": 1,
                    "type": "event",
                    "event": {"event_type": "automation_triggered", "data": {"entity_id": "switch.kitchen_mute"}},
                })),
                Step::Hold,
            ],
        ]
        .concat()])
        .await;
        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn({
            let client = ha(&mock.url);
            async move { client.subscribe_mute_states(&entities, tx).await }
        });

        let first = within("mute push", rx.recv()).await.expect("a mute change");
        assert_eq!(first, ("switch.kitchen_mute".to_string(), true));
        // The hallway entity and the non-state event must not be forwarded.
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "only subscribed entities are forwarded"
        );

        assert_eq!(
            mock.frames(0)[1],
            json!({"id": 1, "type": "subscribe_events", "event_type": "state_changed"})
        );
        drop(rx);
        let ended = within("subscription shutdown", task).await.expect("join");
        assert!(ended.is_ok(), "graceful shutdown returns Ok: {ended:?}");
    }

    #[tokio::test]
    async fn subscribe_reconnects_and_resubscribes() {
        let subscribe_conn = |state: &str, tail: Vec<Step>| {
            let mut steps = auth_steps();
            steps.extend(vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                state_changed("switch.kitchen_mute", state),
            ]);
            steps.extend(tail);
            steps
        };
        let mock = mock_ha(vec![
            subscribe_conn("off", vec![Step::Close]),
            subscribe_conn("on", vec![Step::Hold]),
        ])
        .await;

        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn({
            let client = ha(&mock.url);
            async move { client.subscribe_mute_states(&entities, tx).await }
        });

        assert_eq!(
            within("first mute push", rx.recv()).await.expect("first"),
            ("switch.kitchen_mute".to_string(), false)
        );
        // After HA drops the socket the client must reconnect and resubscribe.
        let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("reconnect within the backoff")
            .expect("second mute push");
        assert_eq!(second, ("switch.kitchen_mute".to_string(), true));

        let expected_subscribe =
            json!({"id": 1, "type": "subscribe_events", "event_type": "state_changed"});
        for connection in 0..2 {
            let frames = mock.frames(connection);
            assert!(frames.len() >= 2, "connection {connection}: {frames:?}");
            assert_eq!(frames[0], json!({"type": "auth", "access_token": "tok"}));
            assert_eq!(
                frames[1], expected_subscribe,
                "connection {connection} must resubscribe"
            );
        }

        drop(rx);
        let ended = within("subscription shutdown", task).await.expect("join");
        assert!(ended.is_ok(), "graceful shutdown returns Ok: {ended:?}");
    }

    #[tokio::test]
    async fn subscribe_stops_on_dropped_receiver() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Hold,
            ],
        ]
        .concat()])
        .await;
        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, rx) = mpsc::channel(8);
        drop(rx);
        let client = ha(&mock.url);
        within(
            "subscribe with a dead receiver",
            client.subscribe_mute_states(&entities, tx),
        )
        .await
        .expect("a dropped receiver is a graceful shutdown, not an error");
        // The auth frame is deterministic here (the client consumed `auth_ok`, which the
        // mock only sends after recording it). The `subscribe_events` frame is not: the
        // client returns as soon as the receiver is gone, without waiting for the mock to
        // read it — its exact JSON is asserted in the two streaming tests instead.
        assert_eq!(
            mock.frames(0)[0],
            json!({"type": "auth", "access_token": "tok"})
        );
    }

    // ---- overall deadlines (HA-1) ---------------------------------------------------
    //
    // The per-frame read timeout restarts on every received frame, and a WebSocket ping
    // is a frame. These feed the client a peer that keeps the socket alive and says
    // nothing: every one of these calls used to wait forever (30 s at a time, forever).

    #[tokio::test]
    async fn a_peer_that_only_pings_cannot_wedge_the_auth_stage() {
        let mock = mock_ha(vec![vec![
            Step::Send(json!({"type": "auth_required", "ha_version": "2024.6.0"})),
            Step::Await,
            Step::Pings,
        ]])
        .await;
        let err = within(
            "auth under a ping flood",
            Session::connect(&ha(&mock.url), Duration::from_millis(400)),
        )
        .await
        // `err()` rather than `expect_err()`: `Session` is deliberately not `Debug` (it
        // holds the socket and the token), and only the error is of interest here.
        .err()
        .expect("pings must not hold the auth stage open past its deadline");
        let text = err.to_string();
        assert!(text.contains("authenticating"), "must name the stage: {text}");
        assert!(text.contains("deadline"), "must name the deadline: {text}");
        assert!(!text.contains("tok"), "the token never reaches an error: {text}");
    }

    #[tokio::test]
    async fn a_peer_that_only_pings_after_the_run_ack_cannot_wedge_a_pipeline_run() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await, // the run command
                run_ack(1),  // HA acknowledges the run...
                Step::Pings, // ...and then every "frame" is a ping: MAX_RUN_EVENTS counts
                             // JSON, the read timeout restarts per frame, so only an
                             // overall deadline ends this.
            ],
        ]
        .concat()])
        .await;
        let started = Instant::now();
        let err = within(
            "run under a ping flood",
            ha(&mock.url).run_pipeline_deadlined(&request("hello"), Duration::from_millis(500)),
        )
        .await
        .expect_err("a heartbeating, silent peer must end at the run deadline");
        let text = err.to_string();
        assert!(text.contains("pipeline run"), "must name the stage: {text}");
        assert!(text.contains("deadline"), "must name the deadline: {text}");
        assert!(
            started.elapsed() >= Duration::from_millis(400),
            "it must have used its budget rather than erroring early: {:?}",
            started.elapsed()
        );
    }

    #[tokio::test]
    async fn a_peer_that_only_pings_after_a_command_cannot_wedge_play_media() {
        let mock = mock_ha(vec![[auth_steps(), vec![Step::Await, Step::Pings]].concat()]).await;
        let err = within(
            "play_media under a ping flood",
            ha(&mock.url).play_media_deadlined(
                "media_player.kitchen_satellite",
                "/api/tts_proxy/abc.mp3",
                None,
                Duration::from_millis(400),
            ),
        )
        .await
        .expect_err("awaiting a result under a ping flood must hit the deadline");
        let text = err.to_string();
        assert!(text.contains("play_media"), "must name the stage: {text}");
        assert!(text.contains("deadline"), "must name the deadline: {text}");
    }

    // ---- current-state sync (HA-2) ---------------------------------------------------

    /// `subscribe_events` is a change feed: a mute switch that was already on when
    /// listenrr started must reach the coordinator without waiting for someone to flip
    /// it. The client asks for the current states right after (re)subscribing and
    /// forwards them through the same path as a live change.
    #[tokio::test]
    async fn subscribe_forwards_the_current_state_before_any_change() {
        let mock = mock_ha(vec![[
            auth_steps(),
            vec![
                Step::Await, // subscribe_events
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Await, // get_states — only ever sent after the subscription landed
                Step::Send(json!({
                    "id": 2, "type": "result", "success": true,
                    "result": [
                        {"entity_id": "switch.kitchen_mute", "state": "on", "attributes": {}},
                        {"entity_id": "switch.hallway_mute", "state": "on", "attributes": {}},
                        {"entity_id": "climate.landing", "state": "heat", "attributes": {}},
                    ],
                })),
                state_changed("switch.kitchen_mute", "off"),
                Step::Hold,
            ],
        ]
        .concat()])
        .await;
        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn({
            let client = ha(&mock.url);
            async move { client.subscribe_mute_states(&entities, tx).await }
        });

        let snapshot = within("the current state", rx.recv())
            .await
            .expect("an already-on switch is forwarded without waiting for a change");
        assert_eq!(snapshot, ("switch.kitchen_mute".to_string(), true));
        // The change feed still works afterwards, and neither the unsubscribed hallway
        // switch nor the climate was forwarded — one entity, both times.
        let change = within("the live change", rx.recv())
            .await
            .expect("live changes keep flowing after the snapshot");
        assert_eq!(change, ("switch.kitchen_mute".to_string(), false));
        assert!(
            tokio::time::timeout(Duration::from_millis(300), rx.recv())
                .await
                .is_err(),
            "nothing outside the mute set is forwarded"
        );

        let frames = mock.frames(0);
        assert_eq!(
            frames[1],
            json!({"id": 1, "type": "subscribe_events", "event_type": "state_changed"}),
            "subscribe first, so no change can be missed"
        );
        assert_eq!(
            frames[2],
            json!({"id": 2, "type": "get_states"}),
            "snapshot second, so a racing change orders after it"
        );

        drop(rx);
        let ended = within("subscription shutdown", task).await.expect("join");
        assert!(ended.is_ok(), "graceful shutdown returns Ok: {ended:?}");
    }

    /// The other half of HA-2: the switch changed while the subscription was **down**.
    /// The new connection never carries a `state_changed` for it — only re-reading the
    /// current state can know.
    #[tokio::test]
    async fn subscribe_resnapshots_the_current_state_after_a_reconnect() {
        let conn = |state: &str, tail: Vec<Step>| {
            let mut steps = auth_steps();
            steps.extend(vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Await,
                Step::Send(json!({
                    "id": 2, "type": "result", "success": true,
                    "result": [
                        {"entity_id": "switch.kitchen_mute", "state": state, "attributes": {}},
                    ],
                })),
            ]);
            steps.extend(tail);
            steps
        };
        let mock = mock_ha(vec![
            conn("off", vec![Step::Close]),
            // No state_changed step on the second connection: the flip happened while
            // we were disconnected, and a change feed cannot deliver it.
            conn("on", vec![Step::Hold]),
        ])
        .await;

        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn({
            let client = ha(&mock.url);
            async move { client.subscribe_mute_states(&entities, tx).await }
        });

        assert_eq!(
            within("first snapshot", rx.recv()).await.expect("first state"),
            ("switch.kitchen_mute".to_string(), false)
        );
        let second = tokio::time::timeout(Duration::from_secs(3), rx.recv())
            .await
            .expect("reconnect within the backoff")
            .expect("the state that changed while disconnected");
        assert_eq!(second, ("switch.kitchen_mute".to_string(), true));

        let frames = mock.frames(1);
        assert_eq!(
            frames[2],
            json!({"id": 2, "type": "get_states"}),
            "the reconnected subscription re-reads the current state"
        );

        drop(rx);
        let ended = within("subscription shutdown", task).await.expect("join");
        assert!(ended.is_ok(), "graceful shutdown returns Ok: {ended:?}");
    }

    // ---- backoff reset (HA-3) --------------------------------------------------------

    /// Backoff throttles *consecutive* failures. Three failed attempts ratchet the
    /// delay; then one connection stays up past the stability threshold and dies — a
    /// healthy loss, which must put the next reconnect back at the floor. Scripted:
    /// 250+500+1000 ms of backoff, a 300 ms healthy connection, then **250 ms** to the
    /// fifth connection. Without the reset the wait after the healthy connection is
    /// `backoff_delay(3)` = 2000 ms, so the last state lands at ~4.05 s, not ~2.3 s.
    #[tokio::test]
    async fn a_healthy_connection_resets_the_reconnect_backoff() {
        let rejected = || {
            vec![
                Step::Send(json!({"type": "auth_required", "ha_version": "2024.6.0"})),
                Step::Await,
                Step::Send(json!({"type": "auth_invalid", "message": "try again"})),
                Step::Close,
            ]
        };
        let healthy = || {
            let mut steps = auth_steps();
            steps.extend(vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                // Established, and clearly longer than the 100 ms stability threshold
                // passed below — this is the "it was working, then HA restarted" case.
                Step::Sleep(Duration::from_millis(300)),
                Step::Close,
            ]);
            steps
        };
        let serving = || {
            let mut steps = auth_steps();
            steps.extend(vec![
                Step::Await,
                Step::Send(json!({"id": 1, "type": "result", "success": true})),
                Step::Await,
                Step::Send(json!({
                    "id": 2, "type": "result", "success": true,
                    "result": [
                        {"entity_id": "switch.kitchen_mute", "state": "on", "attributes": {}},
                    ],
                })),
                Step::Hold,
            ]);
            steps
        };
        let mock = mock_ha(vec![
            rejected(),
            rejected(),
            rejected(),
            healthy(),
            serving(),
        ])
        .await;

        let entities = vec!["switch.kitchen_mute".to_string()];
        let (tx, mut rx) = mpsc::channel(8);
        let task = tokio::spawn({
            let client = ha(&mock.url);
            async move {
                client
                    .subscribe_until_cancelled(&entities, tx, Duration::from_millis(100))
                    .await
            }
        });

        let event = tokio::time::timeout(Duration::from_millis(3_500), rx.recv())
            .await
            .expect(
                "the reconnect after a healthy connection must start from the floor \
                 (250 ms), not continue the ratchet (2000 ms)",
            )
            .expect("the serving subscription's state");
        assert_eq!(event, ("switch.kitchen_mute".to_string(), true));

        drop(rx);
        let ended = within("subscription shutdown", task).await.expect("join");
        assert!(ended.is_ok(), "graceful shutdown returns Ok: {ended:?}");
    }

    // ---- pure helpers -------------------------------------------------------------

    #[test]
    fn backoff_delay_is_monotonic_and_capped() {
        assert_eq!(
            backoff_delay(0),
            Duration::from_millis(RECONNECT_BACKOFF_MIN_MS)
        );
        assert_eq!(
            backoff_delay(1),
            Duration::from_millis(RECONNECT_BACKOFF_MIN_MS * 2)
        );
        assert_eq!(
            backoff_delay(2),
            Duration::from_millis(RECONNECT_BACKOFF_MIN_MS * 4)
        );
        assert_eq!(
            backoff_delay(7),
            Duration::from_millis(RECONNECT_BACKOFF_MAX_MS)
        );
        assert_eq!(
            backoff_delay(40),
            Duration::from_millis(RECONNECT_BACKOFF_MAX_MS),
            "the shift is clamped, so no overflow"
        );
        for attempt in 1..40 {
            assert!(backoff_delay(attempt) >= backoff_delay(attempt - 1));
        }
    }

    #[test]
    fn socket_parts_fills_path_and_default_port() {
        assert_eq!(
            socket_parts("ws://ha.example.lan:8123").unwrap(),
            (
                "ha.example.lan:8123".to_string(),
                "ws://ha.example.lan:8123/api/websocket".to_string()
            )
        );
        assert_eq!(
            socket_parts("http://192.168.1.20:8123/").unwrap(),
            (
                "192.168.1.20:8123".to_string(),
                "ws://192.168.1.20:8123/api/websocket".to_string()
            )
        );
        assert_eq!(
            socket_parts(" ws://ha.example.lan ").unwrap(),
            (
                "ha.example.lan:8123".to_string(),
                "ws://ha.example.lan/api/websocket".to_string()
            )
        );
        assert_eq!(
            socket_parts("ws://ha.example.lan:9/custom/socket")
                .unwrap()
                .1,
            "ws://ha.example.lan:9/custom/socket"
        );
        assert!(socket_parts("https://ha.example.lan")
            .unwrap_err()
            .to_string()
            .contains("TLS"));
        assert!(socket_parts("ha.example.lan:8123").is_err());
        assert!(socket_parts("ftp://ha.example.lan").is_err());
    }

    #[test]
    fn run_command_carries_pipeline_id_when_known() {
        let req = request("hi");
        let without = run_command(&req, None);
        assert!(without.get("pipeline").is_some(), "the key is always sent");
        assert_eq!(without["pipeline"], Value::Null);
        let with = run_command(&req, Some("01J9ZEXAMPLE"));
        assert_eq!(with["pipeline"], json!("01J9ZEXAMPLE"));
        assert_eq!(with["input"], json!({"text": "hi"}));
        assert_eq!(with["type"], json!("assist_pipeline/run"));
    }

    #[test]
    fn muted_from_state_reads_attribute_then_switch_state() {
        // A `media_player` mute entity.
        assert_eq!(
            muted_from_state(&json!({"state": "playing", "attributes": {"is_volume_muted": true}})),
            Some(true)
        );
        // The configured mute entities are switches (S4.5).
        assert_eq!(muted_from_state(&json!({"state": "on"})), Some(true));
        assert_eq!(muted_from_state(&json!({"state": "off"})), Some(false));
        // Unknown/unavailable is skipped, not guessed.
        assert_eq!(muted_from_state(&json!({"state": "unavailable"})), None);
        assert_eq!(muted_from_state(&json!({})), None);
        assert_eq!(muted_from_state(&Value::Null), None);
    }

    /// The payload `state_changed` wraps, for direct calls into `mute_change`.
    fn state_changed_msg(entity: &str, state: &str) -> Value {
        json!({
            "type": "event", "id": 1,
            "event": {
                "event_type": "state_changed",
                "data": {"entity_id": entity,
                          "new_state": {"entity_id": entity, "state": state, "attributes": {}}},
            },
        })
    }

    /// The property that was broken in service on 2026-11-02: HA streams every state change
    /// in the house, and a climate's `heat`, a person's `away` or a sensor's `20.3` is not
    /// an on/off value. Parsing before the membership check turned a whole household into a
    /// WARN stream while the service watched no entities at all. Swap the two steps in
    /// `mute_change` and this reddens; the second half keeps the useful warning useful.
    #[test]
    fn state_changes_outside_the_mute_set_produce_no_warnings() {
        use Ordering::Relaxed;
        let wanted: HashSet<&str> = ["switch.landing_mute"].into_iter().collect();

        let (count, _guard) = capture_warnings();
        for (entity, state) in [
            ("climate.landing", "heat"),
            ("person.alex", "away"),
            ("sensor.boiler", "20.3"),
        ] {
            assert_eq!(
                mute_change(&state_changed_msg(entity, state), &wanted),
                None
            );
        }
        assert_eq!(
            count.load(Relaxed),
            0,
            "house-wide state changes were reported as mute faults"
        );

        // A *subscribed* entity reporting something unintelligible is still worth a line.
        let (count, _guard) = capture_warnings();
        assert_eq!(
            mute_change(
                &state_changed_msg("switch.landing_mute", "unavailable"),
                &wanted
            ),
            None
        );
        assert_eq!(
            count.load(Relaxed),
            1,
            "a subscribed mute entity was dropped without a word"
        );
    }
}
