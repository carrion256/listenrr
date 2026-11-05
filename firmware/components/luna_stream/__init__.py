"""luna_stream — ESPHome external component that streams a satellite's
microphone to the `listenrr` backend over the frozen TCP protocol, drives
the status LED / earcon from the backend's control messages, and reports an
established connection through `on_connected`.

Wire contract: `src/proto.rs` (§4 of the original design plan, not published). The
byte-level code lives in luna_stream.h and is host-tested with g++; this file
only describes configuration and code generation.

Example usage (config/satellite.example.yaml):

    luna_stream:
      backend_host: 192.168.1.30
      backend_port: 10700
      source_id: kitchen
      room: kitchen
      token: !secret luna_token        # never a literal here
      has_speaker: true
      has_aec: false
      microphone: mic_i2s
      media_player: media_out
      on_led:
        - lambda: '...'
      on_earcon:
        - lambda: '...'
      on_connected:
        - lambda: '...'   # fired from loop() when a connection is established
"""

import esphome.codegen as cg
import esphome.config_validation as cv
from esphome import automation
from esphome.components import media_player, microphone
from esphome.const import CONF_ID

DEPENDENCIES = ["esp32", "wifi"]
AUTO_LOAD = ["microphone", "media_player"]

# Keys are literals rather than esphome.const imports: none of these are
# upstream constants, and inventing a const name would put a private key in
# ESPHome's namespace.
CONF_BACKEND_HOST = "backend_host"
CONF_BACKEND_PORT = "backend_port"
CONF_SOURCE_ID = "source_id"
CONF_ROOM = "room"
CONF_TOKEN = "token"
CONF_HAS_SPEAKER = "has_speaker"
CONF_HAS_AEC = "has_aec"
CONF_FIRMWARE_VERSION = "firmware_version"
CONF_MICROPHONE = "microphone"
CONF_MEDIA_PLAYER = "media_player"
CONF_FRAME_BUFFER_FRAMES = "frame_buffer_frames"
CONF_RX_BUFFER_BYTES = "rx_buffer_bytes"
CONF_ON_LED = "on_led"
CONF_ON_EARCON = "on_earcon"
CONF_ON_CONNECTED = "on_connected"

# The backend's documented default port: TCP listen 0.0.0.0:10700.
DEFAULT_BACKEND_PORT = 10700
# Matches the hello line's `fw` example in the frozen contract.
DEFAULT_FIRMWARE_VERSION = "0.1.0"
# 16 frames is ~1 s of audio (64 ms each); enough to ride out a short stall
# without buffering minutes of speech on a satellite.
DEFAULT_FRAME_BUFFER_FRAMES = 16
DEFAULT_RX_BUFFER_BYTES = 512

luna_stream_ns = cg.esphome_ns.namespace("luna_stream")
LunaStream = luna_stream_ns.class_("LunaStream", cg.Component)


def _as_list(value):
    """Accept either shape validate_automation(single=True) may return."""
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    return [value]


def _validate_token(token):
    """A token is required, and it is meant to come from secrets.yaml.

    ESPHome substitutes `!secret` long before validation runs, so a schema here
    physically cannot tell a secret reference from a typed literal — it sees a
    string either way. What can be checked is caught: empty, and the placeholder
    values that end up shipped by accident. The C++ side logs a warning at
    startup for a short token, which is the remaining "looks hand-typed" signal.
    """
    lowered = token.strip().lower()
    if lowered in ("", "change_me", "change-me", "changeme", "secret", "token", "luna_token"):
        raise cv.Invalid(
            "token looks like a placeholder; set the shared secret in secrets.yaml and "
            "reference it as token: !secret luna_token"
        )
    return token


# The firmware builds the hello line into a fixed 512-byte stack buffer
# (on_connected_ in luna_stream.cpp), and its JsonWriter refuses the moment the
# line would need the last byte for the NUL: 511 bytes is the longest line that
# fits. Nothing else told the operator — build_hello returned 0 at runtime and
# the satellite sat in a reconnect loop (FW-5). Reproduce the exact wire length
# here so an over-long field is a config-time error instead.
FIRMWARE_HELLO_BUFFER = 512
MAX_HELLO_LINE_BYTES = FIRMWARE_HELLO_BUFFER - 1


def _json_escaped_len(value):
    """Wire bytes `value` occupies inside a JSON string literal.

    Mirrors JsonWriter::str in luna_stream.h (which mirrors serde_json): '"'
    and '\\' become two bytes, \\b \\f \\n \\r \\t become two, every other
    character below 0x20 becomes the six-byte \\u00XX, and anything else is
    written as its own UTF-8 bytes. json.dumps cannot stand in for this: with
    ensure_ascii=True it escapes non-ASCII, which the firmware does not.
    """
    total = 0
    for ch in value:
        if ch in '"\\':
            total += 2
        elif ch in "\b\f\n\r\t":
            total += 2
        elif ord(ch) < 0x20:
            total += 6
        else:
            total += len(ch.encode("utf-8"))
    return total


def _hello_wire_len(source_id, room, token, fw, has_speaker, has_aec):
    """Exact byte count of the hello line build_hello() will emit, including
    the trailing '\\n'. Field-for-field mirror of luna_stream_codec::build_hello;
    the key order and literal text are the frozen contract (docs/protocol.md
    §9.4: that example is 120 bytes of JSON + '\\n' = 121)."""
    fixed = (
        '{"v":1'
        ',"source_id":'
        ',"room":'
        ',"token":'
        ',"caps":{"has_speaker":' + ("true" if has_speaker else "false")
        + ',"has_aec":' + ("true" if has_aec else "false")
        + '},"fw":'
        '}\n'
    )
    total = len(fixed)
    # The component passes token=nullptr exactly when the config token is empty,
    # and build_hello then writes the four bytes `null` with no quotes.
    if token == "":
        total += 4
    else:
        total += 2 + _json_escaped_len(token)
    for value in (source_id, room, fw):
        total += 2 + _json_escaped_len(value)  # the surrounding quotes too
    return total


def _validate_hello_fits(config):
    total = _hello_wire_len(
        config[CONF_SOURCE_ID],
        config[CONF_ROOM],
        config[CONF_TOKEN],
        config[CONF_FIRMWARE_VERSION],
        config[CONF_HAS_SPEAKER],
        config[CONF_HAS_AEC],
    )
    if total > MAX_HELLO_LINE_BYTES:
        raise cv.Invalid(
            f"the hello line would be {total} bytes, but the firmware builds it into a "
            f"{FIRMWARE_HELLO_BUFFER}-byte buffer: at most {MAX_HELLO_LINE_BYTES} bytes fit "
            f"(source_id {_json_escaped_len(config[CONF_SOURCE_ID])}, room "
            f"{_json_escaped_len(config[CONF_ROOM])}, token "
            f"{_json_escaped_len(config[CONF_TOKEN])}, firmware_version "
            f"{_json_escaped_len(config[CONF_FIRMWARE_VERSION])} wire bytes). Shorten the "
            "longest field — usually the token belongs in a shorter secret."
        )
    return config


CONFIG_SCHEMA = cv.All(
    cv.Schema(
        {
            cv.GenerateID(): cv.declare_id(LunaStream),
            cv.Required(CONF_BACKEND_HOST): cv.string_strict,
            cv.Optional(CONF_BACKEND_PORT, default=DEFAULT_BACKEND_PORT): cv.port,
            cv.Required(CONF_SOURCE_ID): cv.string_strict,
            cv.Required(CONF_ROOM): cv.string_strict,
            cv.Required(CONF_TOKEN): cv.All(cv.string_strict, _validate_token),
            # Advertised in hello `caps`; both default false so a satellite with
            # no speaker/AEC stays a valid minimal client (src/proto.rs Caps).
            cv.Optional(CONF_HAS_SPEAKER, default=False): cv.boolean,
            cv.Optional(CONF_HAS_AEC, default=False): cv.boolean,
            cv.Optional(CONF_FIRMWARE_VERSION, default=DEFAULT_FIRMWARE_VERSION): cv.string_strict,
            # The contract is fixed at 16 kHz mono 16-bit signed LE, so this
            # component asks for exactly that through ESPHome's own
            # MicrophoneSource schema rather than touching the Microphone
            # directly: the source converts bits-per-sample and channel layout
            # for us, and shares the physical mic with micro_wake_word or
            # voice_assistant. FINAL_VALIDATE_SCHEMA below checks the sample
            # rate against the mic at config time.
            cv.Required(CONF_MICROPHONE): microphone.microphone_source_schema(
                min_bits_per_sample=16,
                max_bits_per_sample=16,
                min_channels=1,
                max_channels=1,
            ),
            # Optional: when present, reaching idle emits the `playback_done`
            # event that moves the backend Speaking -> FollowUp.
            cv.Optional(CONF_MEDIA_PLAYER): cv.use_id(media_player.MediaPlayer),
            # Sizing for the two buffers that face the network.
            cv.Optional(
                CONF_FRAME_BUFFER_FRAMES, default=DEFAULT_FRAME_BUFFER_FRAMES
            ): cv.int_range(min=2, max=128),
            # The firmware's receive buffer is a fixed 512 bytes (rx_buf_ in
            # luna_stream.h); poll_downlink_() took min(configured, 512), so a
            # larger setting was a silent no-op while dump_config printed the
            # requested number (FW-6). The schema now matches the real maximum.
            cv.Optional(CONF_RX_BUFFER_BYTES, default=DEFAULT_RX_BUFFER_BYTES): cv.int_range(
                min=64, max=512
            ),
            # Downlink control triggers. `on_led` receives the state string
            # ("idle" | "listening" | "speaking" | "muted"); `on_earcon`
            # receives the earcon name. Unknown states are never passed on:
            # the decoder drops them, so a handler only sees the four above.
            cv.Optional(CONF_ON_LED): automation.validate_automation(single=True),
            cv.Optional(CONF_ON_EARCON): automation.validate_automation(single=True),
            # No payload: entering the Connected state is itself the event.
            # Fired from loop() (the main loop), once per established
            # connection — AC2's post-reconnect I2C report is the intended
            # consumer, and it must not run on the capture task.
            cv.Optional(CONF_ON_CONNECTED): automation.validate_automation(single=True),
        }
    ).extend(cv.COMPONENT_SCHEMA),
    cv.only_on_esp32,
    _validate_hello_fits,
)

# The wire format cannot negotiate: a 2053-byte record is 1024 int16 samples at
# 16 kHz mono. Check that against the configured microphone during validation
# instead of discovering it at runtime, the way micro_wake_word does.
FINAL_VALIDATE_SCHEMA = cv.Schema(
    {
        cv.Required(CONF_MICROPHONE): microphone.final_validate_microphone_source_schema(
            "luna_stream", sample_rate=16000
        ),
    },
    extra=cv.ALLOW_EXTRA,
)


async def to_code(config):
    var = cg.new_Pvariable(config[CONF_ID])
    await cg.register_component(var, config)

    cg.add(var.set_backend(config[CONF_BACKEND_HOST], config[CONF_BACKEND_PORT]))
    cg.add(var.set_source_id(config[CONF_SOURCE_ID]))
    cg.add(var.set_room(config[CONF_ROOM]))
    cg.add(var.set_token(config[CONF_TOKEN]))
    cg.add(var.set_firmware_version(config[CONF_FIRMWARE_VERSION]))
    cg.add(var.set_has_speaker(config[CONF_HAS_SPEAKER]))
    cg.add(var.set_has_aec(config[CONF_HAS_AEC]))
    cg.add(var.set_frame_buffer_frames(config[CONF_FRAME_BUFFER_FRAMES]))
    cg.add(var.set_rx_buffer_bytes(config[CONF_RX_BUFFER_BYTES]))

    # passive=False: a satellite streams continuously, so this component starts
    # and stops the microphone itself. Other consumers (micro_wake_word,
    # voice_assistant) can still attach their own MicrophoneSource to the same
    # physical Microphone.
    mic_source = await microphone.microphone_source_to_code(config[CONF_MICROPHONE], passive=False)
    cg.add(var.set_mic_source(mic_source))

    if CONF_MEDIA_PLAYER in config:
        player = await cg.get_variable(config[CONF_MEDIA_PLAYER])
        cg.add(var.set_media_player(player))

    for conf in _as_list(config.get(CONF_ON_LED)):
        await automation.build_automation(
            var.get_led_trigger(), [(cg.std_string, "state")], conf
        )
    for conf in _as_list(config.get(CONF_ON_EARCON)):
        await automation.build_automation(
            var.get_earcon_trigger(), [(cg.std_string, "name")], conf
        )
    for conf in _as_list(config.get(CONF_ON_CONNECTED)):
        await automation.build_automation(var.get_connected_trigger(), [], conf)
