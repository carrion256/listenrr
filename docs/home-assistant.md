# Home Assistant setup

listenrr does not replace Home Assistant's voice stack. It replaces the part in front of it:
wake word, capture and speech to text. From the text onward it uses your existing Assist pipeline,
so whatever conversation agent and TTS voice you already have keep working.

## What happens to one request

```mermaid
sequenceDiagram
    participant S as Satellite
    participant L as listenrr
    participant A as ASR (Wyoming)
    participant H as Home Assistant
    S->>L: audio frames (TCP)
    L->>L: wake word, arbitration, capture
    L->>A: utterance audio
    A-->>L: transcript
    L->>L: verify gate, strip wake word
    L->>H: assist_pipeline/run (start_stage intent, end_stage tts)
    H-->>L: intent-end (agent reply), tts-end (audio URL)
    alt Home Assistant plays it
        L->>H: media_player.play_media (announce) on the satellite's entity
        H->>S: fetch audio from the tts_proxy URL
    else the source plays it
        L->>S: play control with the URL
    end
    S-->>L: playback_done
```

- **Intent stage, not STT.** listenrr starts the pipeline with `start_stage: "intent"` and
  `end_stage: "tts"`, and sends the transcript as `input.text`. Home Assistant's own speech to
  text is never used, so the Assist pipeline's STT setting does not matter to listenrr.
- **The conversation agent is yours.** Whatever the pipeline names as its conversation agent
  answers: Home Assistant's built-in intents, an LLM integration such as OpenAI, or a local model.
  The conversation id is the listenrr session id, so a follow-up turn continues the same
  conversation without the wake word.
- **The TTS engine is yours.** The pipeline's TTS stage produces the audio, and Home Assistant
  returns a `/api/tts_proxy/...` URL for it. Any TTS integration works. The run this project was
  developed against used an ElevenLabs-backed TTS entity, and its audio came back as MP3
  (`audio/mpeg`). listenrr takes the format from the `mime_type` in `tts-end` and understands MP3 and
  FLAC. See [`tests/fixtures/ha-run-2026-11-02.json`](../tests/fixtures/ha-run-2026-11-02.json) for
  the exact events Home Assistant 2026.9.2 sent.
- **Which device is talking.** The run carries the source's Home Assistant `device_id`, so the
  conversation agent can resolve "the lights in here" to the right area. Set `device_id` on the
  `[[sources]]` entry (or `fallback_device_id` for a source with no device of its own).

## Using Hermes as the conversation agent

This is how the project's own house runs. [Hermes Agent](https://github.com/NousResearch/hermes-agent) exposes an
[OpenAI-compatible API server](https://hermes-agent.nousresearch.com/docs/user-guide/features/api-server), and the
pipeline's conversation agent is a Home Assistant agent pointed at that endpoint.

1. Run Hermes with its API server enabled. It requires an API key and refuses to start the
   endpoint without one. Keep the endpoint on your LAN or behind a reverse proxy that limits who
   can reach it.
2. In Home Assistant, add an OpenAI-compatible conversation agent and give it Hermes's base URL
   (the `/v1` root) and that key. Use an integration that accepts a custom base URL.
3. In **Settings, Voice assistants**, set that agent as the conversation agent of the pipeline you
   give listenrr in `ha.pipeline_id`. Set the pipeline's TTS to the voice you want (the author uses
   an ElevenLabs entity).

From then on, `assist_pipeline/run` from listenrr reaches Hermes with the transcript as the user
message, and Hermes's reply is spoken. Because the agent keeps its own memory and tools, it can do
more than Home Assistant's intents: answer questions, run tools and keep context between turns.
The model Hermes uses is Hermes's own configuration and is invisible to listenrr.

Two practical notes:

- Voice replies should be short. Put that in the agent's persona or system prompt, because a long
  answer is a long wait before the TTS audio exists.
- An empty request is never sent. A bare wake word strips to nothing, and listenrr drops it before
  the pipeline so the agent never sees an empty message.

## Steps

### 1. Pick or create the pipeline

In Home Assistant, open **Settings, Voice assistants**, and choose the pipeline whose conversation
agent and TTS voice you want. You need its id. Current Home Assistant does not serve the pipeline
registry over REST (`/api/assist/pipeline/` answers 404), so read it with the
`assist_pipeline/pipeline/list` WebSocket command.

The STT and wake word fields on that pipeline are ignored. Only the conversation agent, language
and TTS settings take effect.

### 2. Create an access token

Open your user profile, **Security**, **Long-lived access tokens**, and create one. Scope it by
using a dedicated user if you want to limit what a leaked token can do: listenrr needs to run
Assist pipelines, call `media_player` services and read the state of any mute entities.

### 3. Configure `[ha]`

```toml
[ha]
url = "ws://homeassistant.local:8123"     # WebSocket base; REST is derived from it
token = "<long-lived token>"
pipeline_id = "<pipeline id from step 1>"
internal_url = "http://192.168.1.20:8123" # address the satellites use to fetch the audio
```

`url` may be `ws://`, `wss://`, `http://` or `https://`. listenrr maps it to the right scheme for
the WebSocket (`/api/websocket`) and for fetching replies. `internal_url` is only needed when the
satellites cannot reach Home Assistant at `url`, for example when `url` is an external hostname and
the satellites live on the LAN. Leave it empty otherwise.

The token sits in `listenrr.toml`. Keep that file `0640` and out of git.

### 4. Tell listenrr where each reply plays

Each source in `[[sources]]` has one of two playback routes.

**Home Assistant plays it (default).** Home Assistant has to see the satellite as a
`media_player`. listenrr calls `media_player.play_media` on that entity with `announce: true`, so
music ducks while the assistant speaks. Set the entity on the source:

```toml
[[sources]]
id = "kitchen-sat"
room = "kitchen"
group = "kitchen"
device_id = "<ha device id>"
ha_entity = "media_player.kitchen_satellite"
```

For a board running the `luna_stream` firmware component, the entity is the `speaker` media player
defined in [`config/satellite.example.yaml`](../config/satellite.example.yaml), which appears in
Home Assistant once the ESPHome device is added.

**The source plays it.** The Android tablet app has a speaker but no `media_player`. Set
`playback = "source"` and listenrr sends it a `play` control with the audio URL. This route is for
the tablet app only; a board running the firmware must stay on the default.

A source with no speaker (a microphone only) uses its group's output instead:

```toml
[[groups]]
name = "living"
output_entity = "media_player.living_room_speaker"
output_device_id = "<ha device id>"
```

### 5. Optional: mute from Home Assistant

Point `mute_entity` at any entity (usually a `switch`) and turning it on mutes that source. It keeps
streaming audio but its wake words are ignored. listenrr only reads the entity. If no source sets
one, listenrr does not subscribe to Home Assistant state changes at all.

### 6. Check it

```sh
listenrr health --config /etc/listenrr/listenrr.toml   # config only, contacts nothing
```

Then watch the service log while you speak. Logs are JSON on stderr, one line per event, so a turn can be followed from the wake to the playback.

## Stop and interrupt

Saying only "stop" ends the conversation: it is matched on the transcript and never sent to Home
Assistant. To also stop a reply that is already playing, listenrr calls `media_player.media_stop` on
the satellite's entity. The tablet app is told through the wire protocol instead.

## Things that go wrong

- **Handshake 404.** The WebSocket is at `/api/websocket`. `/api` alone is the REST root.
- **Every token rejected.** Home Assistant sends `auth_required` before it checks the token. Only
  `auth_ok` is success.
- **"Sorry, I had a problem talking to ..." on a bare wake word.** An empty request reaches an LLM
  agent as an empty message. listenrr drops these before the pipeline, so you will not see it.
- **Reply never plays, run succeeded.** Check that `internal_url` is reachable from the satellite
  and that the entity accepts the audio. Some devices advertise only the announcement form and
  reject plain `play_media`. [`deployment.md`](deployment.md) has the details.
- **No audio from a source that has `playback = "source"`.** It must be the tablet app. A board
  running the ESP firmware refuses that control and drops the connection.

See [`deployment.md`](deployment.md) for running the service and the ASR, and
[`protocol.md`](protocol.md) for the wire format.
