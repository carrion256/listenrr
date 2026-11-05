#pragma once
// luna_stream — ESPHome external component that streams microphone audio to the
// `listenrr` backend over its frozen TCP wire protocol (plan §4, src/proto.rs).
//
// This header has two halves:
//   1. `luna_stream_codec` — the complete wire codec. Pure C++17, no ESPHome
//      headers, so it is host-testable with g++ (see the LUNA_STREAM_HOST_TEST
//      block at the bottom of this file).
//   2. `esphome::luna_stream::LunaStream` — the component itself, compiled only
//      for ESPHome builds (everything ESPHome-specific lives here; the codec
//      above only moves bytes).
//
// Byte contract, transcribed from src/proto.rs (do not "improve" it):
//   hello   : one '\n'-terminated JSON line, no tag
//             {"v":1,"source_id":..,"room":..,"token":..,"caps":{...},"fw":..}
//   audio   : TAG_AUDIO(0x01) | seq u32 LE | 2048 B PCM  = 2053 B per record
//             (the frame alone, seq+PCM, is 2052 B = AUDIO_FRAME_LEN)
//   event   : TAG_EVENT(0x02) | len u16 LE | JSON
//   control : TAG_CONTROL(0x03) | len u16 LE | JSON   (downlink only)

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace luna_stream_codec {

// ---------------------------------------------------------------------------
// Frozen contract constants (src/proto.rs)
// ---------------------------------------------------------------------------
static constexpr uint32_t PROTOCOL_VERSION = 1;

static constexpr uint8_t TAG_AUDIO = 0x01;
static constexpr uint8_t TAG_EVENT = 0x02;
static constexpr uint8_t TAG_CONTROL = 0x03;

static constexpr size_t SAMPLE_RATE_HZ = 16000;
static constexpr size_t FRAME_SAMPLES = 1024;
static constexpr size_t FRAME_PCM_BYTES = FRAME_SAMPLES * 2;      // 2048
static constexpr size_t AUDIO_FRAME_LEN = 4 + FRAME_PCM_BYTES;    // 2052
static constexpr size_t AUDIO_RECORD_LEN = 1 + AUDIO_FRAME_LEN;   // 2053
static constexpr size_t TAGGED_HEADER_LEN = 3;                    // tag + u16 len
static constexpr size_t MAX_JSON_PAYLOAD = 65535;
static constexpr size_t MAX_HELLO_LINE = 4096;

// Firmware-side bound on a downlink control payload. The contract's length
// field is u16, but a control is a few dozen bytes; 1 KiB keeps the decoder's
// buffer in internal RAM while leaving room for a `play` URL.
static constexpr size_t MAX_CONTROL_JSON = 1024;

// Firmware-side caps on the string fields a control carries. The backend imposes
// no length limit (`Control::Earcon{name: String}`), so these are the firmware's:
// generous enough for a real Home Assistant TTS-proxy URL and a spoken earcon
// name, and a control past them is refused — never truncated into a
// different-but-valid control that would look like success.
static constexpr size_t MAX_CONTROL_NAME = 128;
static constexpr size_t MAX_CONTROL_URL = 512;

// Firmware-side bound on one event JSON body. The contract allows a payload up to
// MAX_JSON_PAYLOAD (65535), but the body is built on the stack of the code that
// emits the event; a button name past this is refused (and logged by the
// component) rather than truncated.
static constexpr size_t MAX_EVENT_JSON = 512;

// Reconnect backoff: 1 s doubling to a 30 s ceiling.
static constexpr uint32_t RECONNECT_MIN_MS = 1000;
static constexpr uint32_t RECONNECT_MAX_MS = 30000;

static constexpr uint8_t DECODER_ERR_BAD_TAG = 1;
static constexpr uint8_t DECODER_ERR_PAYLOAD_TOO_LONG = 2;
static constexpr uint8_t DECODER_ERR_BAD_JSON = 3;

// ---------------------------------------------------------------------------
// Small bounds-checked JSON writer
// ---------------------------------------------------------------------------
class JsonWriter {
 public:
  JsonWriter(char *out, size_t cap) : out_(out), cap_(cap) {}

  bool raw(const char *s) { return this->raw(s, std::strlen(s)); }

  bool raw(const char *s, size_t len) {
    if (!this->ok_ || this->len_ + len >= this->cap_) {
      this->ok_ = false;
      return false;
    }
    std::memcpy(this->out_ + this->len_, s, len);
    this->len_ += len;
    this->out_[this->len_] = '\0';
    return true;
  }

  // Writes a JSON string literal, including the surrounding quotes. Escaping
  // matches serde_json: '"', '\\', \b \f \n \r \t and other < 0x20 as \u00XX.
  bool str(const char *s) {
    if (!this->raw("\"")) {
      return false;
    }
    for (const char *p = s; *p != '\0'; p++) {
      const unsigned char c = static_cast<unsigned char>(*p);
      char esc[8];
      size_t esc_len = 0;
      switch (c) {
        case '"':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = '"';
          break;
        case '\\':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = '\\';
          break;
        case '\b':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = 'b';
          break;
        case '\f':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = 'f';
          break;
        case '\n':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = 'n';
          break;
        case '\r':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = 'r';
          break;
        case '\t':
          esc_len = 2;
          esc[0] = '\\';
          esc[1] = 't';
          break;
        default:
          if (c < 0x20) {
            static const char HEX[] = "0123456789abcdef";
            esc_len = 6;
            esc[0] = '\\';
            esc[1] = 'u';
            esc[2] = '0';
            esc[3] = '0';
            esc[4] = HEX[(c >> 4) & 0x0F];
            esc[5] = HEX[c & 0x0F];
          } else {
            esc_len = 1;
            esc[0] = static_cast<char>(c);
          }
          break;
      }
      if (!this->raw(esc, esc_len)) {
        return false;
      }
    }
    return this->raw("\"");
  }

  bool u32(uint32_t v) {
    char buf[16];
    return this->raw(buf, format_u64(buf, sizeof(buf), static_cast<uint64_t>(v)));
  }

  bool boolean(bool v) { return this->raw(v ? "true" : "false"); }

  // Fixed-point number with one decimal, e.g. tenths = -625 -> "-62.5".
  // Written without printf float support (ESP-IDF's %f depends on newlib
  // configuration); this is also what makes the rssi event byte-exact.
  bool tenths(int value) {
    const bool neg = value < 0;
    const long v = neg ? -static_cast<long>(value) : static_cast<long>(value);
    char buf[24];
    int n = 0;
    if (neg) {
      buf[n++] = '-';
    }
    n += static_cast<int>(format_u64(buf + n, sizeof(buf) - n, static_cast<uint64_t>(v / 10)));
    buf[n++] = '.';
    buf[n++] = static_cast<char>('0' + (v % 10));
    return this->raw(buf, static_cast<size_t>(n));
  }

  bool ok() const { return this->ok_; }
  size_t size() const { return this->len_; }
  const char *data() const { return this->out_; }

 private:
  // Returns the number of bytes written (without the terminator).
  static size_t format_u64(char *buf, size_t cap, uint64_t v) {
    char tmp[24];
    size_t n = 0;
    do {
      tmp[n++] = static_cast<char>('0' + (v % 10));
      v /= 10;
    } while (v != 0 && n < sizeof(tmp));
    if (n + 1 > cap) {
      return 0;
    }
    for (size_t i = 0; i < n; i++) {
      buf[i] = tmp[n - 1 - i];
    }
    buf[n] = '\0';
    return n;
  }

  char *out_;
  size_t cap_;
  size_t len_{0};
  bool ok_{true};
};

// ---------------------------------------------------------------------------
// hello
// ---------------------------------------------------------------------------
struct HelloFields {
  const char *source_id;
  const char *room;
  const char *token;  // nullptr encodes JSON null (backend's Option<String>)
  const char *fw;
  bool has_speaker;
  bool has_aec;
};

// Writes the hello line *including* its trailing '\n'. Returns the byte count,
// or 0 when the buffer is too small (or a field is missing).
inline size_t build_hello(char *out, size_t cap, const HelloFields &h) {
  if (out == nullptr || cap == 0 || h.source_id == nullptr || h.room == nullptr || h.fw == nullptr) {
    return 0;
  }
  JsonWriter w(out, cap);
  w.raw("{\"v\":");
  w.u32(PROTOCOL_VERSION);
  w.raw(",\"source_id\":");
  w.str(h.source_id);
  w.raw(",\"room\":");
  w.str(h.room);
  w.raw(",\"token\":");
  if (h.token == nullptr) {
    w.raw("null");
  } else {
    w.str(h.token);
  }
  w.raw(",\"caps\":{\"has_speaker\":");
  w.boolean(h.has_speaker);
  w.raw(",\"has_aec\":");
  w.boolean(h.has_aec);
  w.raw("},\"fw\":");
  w.str(h.fw);
  w.raw("}\n");
  if (!w.ok()) {
    return 0;
  }
  return w.size();
}

// ---------------------------------------------------------------------------
// uplink: audio
// ---------------------------------------------------------------------------
// TAG_AUDIO | seq u32 LE | FRAME_PCM_BYTES of little-endian i16 PCM.
// `pcm_len` must be exactly FRAME_PCM_BYTES: the contract refuses to pad or
// truncate a frame, so this returns 0 instead.
inline size_t build_audio_record(uint8_t *out, size_t cap, uint32_t seq, const uint8_t *pcm, size_t pcm_len) {
  if (out == nullptr || pcm == nullptr || pcm_len != FRAME_PCM_BYTES || cap < AUDIO_RECORD_LEN) {
    return 0;
  }
  out[0] = TAG_AUDIO;
  out[1] = static_cast<uint8_t>(seq & 0xFF);
  out[2] = static_cast<uint8_t>((seq >> 8) & 0xFF);
  out[3] = static_cast<uint8_t>((seq >> 16) & 0xFF);
  out[4] = static_cast<uint8_t>((seq >> 24) & 0xFF);
  std::memcpy(out + 5, pcm, FRAME_PCM_BYTES);
  return AUDIO_RECORD_LEN;
}

// ---------------------------------------------------------------------------
// uplink: events
// ---------------------------------------------------------------------------
inline size_t build_tagged(uint8_t *out, size_t cap, uint8_t tag, const char *json, size_t json_len) {
  if (out == nullptr || json == nullptr || json_len > MAX_JSON_PAYLOAD) {
    return 0;
  }
  if (cap < TAGGED_HEADER_LEN + json_len) {
    return 0;
  }
  out[0] = tag;
  out[1] = static_cast<uint8_t>(json_len & 0xFF);
  out[2] = static_cast<uint8_t>((json_len >> 8) & 0xFF);
  std::memcpy(out + TAGGED_HEADER_LEN, json, json_len);
  return TAGGED_HEADER_LEN + json_len;
}

inline size_t build_event(uint8_t *out, size_t cap, const char *json, size_t json_len) {
  return build_tagged(out, cap, TAG_EVENT, json, json_len);
}

// {"type":"playback_done"}
inline size_t build_event_playback_done(uint8_t *out, size_t cap) {
  static const char JSON[] = "{\"type\":\"playback_done\"}";
  return build_event(out, cap, JSON, sizeof(JSON) - 1);
}

// {"type":"save_buffer"} — the satellite's "for you" button. Never `button`.
inline size_t build_event_save_buffer(uint8_t *out, size_t cap) {
  static const char JSON[] = "{\"type\":\"save_buffer\"}";
  return build_event(out, cap, JSON, sizeof(JSON) - 1);
}

// {"type":"button","name":"night_mode"}
inline size_t build_event_button(uint8_t *out, size_t cap, const char *name) {
  if (name == nullptr || cap < TAGGED_HEADER_LEN) {
    return 0;
  }
  // MAX_EVENT_JSON, not the old 256: a 300-character button name used to overflow this
  // body and the whole event was dropped on the way out. A name past the cap is refused
  // (build returns 0, the component logs it) — never truncated into a shorter name that
  // would look like the button the user actually pressed.
  char json[MAX_EVENT_JSON];
  JsonWriter w(json, sizeof(json));
  w.raw("{\"type\":\"button\",\"name\":");
  w.str(name);
  w.raw("}");
  if (!w.ok()) {
    return 0;
  }
  return build_event(out, cap, w.data(), w.size());
}

// {"type":"rssi","dbm":-62.5} — dbm_tenths is dbm * 10 so the float text is
// stable without printf float support. 64 bytes is sufficient by construction: an `int`
// is at most "-214748364.8", so the body cannot exceed 35 bytes.
inline size_t build_event_rssi(uint8_t *out, size_t cap, int dbm_tenths) {
  char json[64];
  JsonWriter w(json, sizeof(json));
  w.raw("{\"type\":\"rssi\",\"dbm\":");
  w.tenths(dbm_tenths);
  w.raw("}");
  if (!w.ok()) {
    return 0;
  }
  return build_event(out, cap, w.data(), w.size());
}

// ---------------------------------------------------------------------------
// downlink: control decoding
// ---------------------------------------------------------------------------
enum class LedState : uint8_t { Idle, Listening, Speaking, Muted, Unknown };

enum class ControlType : uint8_t { Led, Earcon, Play, Unknown };

struct Control {
  ControlType type{ControlType::Unknown};
  LedState led{LedState::Unknown};
  // MAX_CONTROL_NAME / MAX_CONTROL_URL, not the old 48/320: the backend's contract has no
  // length limit, and a 60-character name or a 344-character HA TTS-proxy URL is a
  // legitimate control. Past these caps the payload is refused (and logged), never cut.
  char name[MAX_CONTROL_NAME]{0};
  char url[MAX_CONTROL_URL]{0};
  char format[16]{0};
};

inline const char *led_state_name(LedState state) {
  switch (state) {
    case LedState::Idle:
      return "idle";
    case LedState::Listening:
      return "listening";
    case LedState::Speaking:
      return "speaking";
    case LedState::Muted:
      return "muted";
    case LedState::Unknown:
    default:
      return "unknown";
  }
}

inline LedState led_state_from_name(const char *s, size_t len) {
  if (s == nullptr) {
    return LedState::Unknown;
  }
  if (len == 4 && std::memcmp(s, "idle", 4) == 0) {
    return LedState::Idle;
  }
  if (len == 9 && std::memcmp(s, "listening", 9) == 0) {
    return LedState::Listening;
  }
  if (len == 8 && std::memcmp(s, "speaking", 8) == 0) {
    return LedState::Speaking;
  }
  if (len == 5 && std::memcmp(s, "muted", 5) == 0) {
    return LedState::Muted;
  }
  return LedState::Unknown;
}

namespace detail {

inline bool is_ws(char c) { return c == ' ' || c == '\t' || c == '\r' || c == '\n'; }

// Structural gate the key scanner alone cannot provide: the payload must be exactly one
// balanced JSON object, with nothing but whitespace after it. serde_json rejects
// `{"a":1` (unterminated) and `{"a":1} junk` / two objects on one payload ("trailing
// characters"), so accepting either here would silently disagree with the backend about
// what a valid control is (plan R10 wants drift caught).
inline bool json_object_well_formed(const char *json, size_t len) {
  size_t i = 0;
  while (i < len && is_ws(json[i])) {
    i++;
  }
  if (i >= len || json[i] != '{') {
    return false;
  }
  int depth = 0;
  bool in_string = false;
  bool escaped = false;
  size_t end = 0;  // one past the closing brace of the top-level value
  for (; i < len; i++) {
    const char c = json[i];
    if (in_string) {
      if (escaped) {
        escaped = false;
      } else if (c == '\\') {
        escaped = true;
      } else if (c == '"') {
        in_string = false;
      }
      continue;
    }
    if (c == '"') {
      in_string = true;
    } else if (c == '{' || c == '[') {
      depth++;
    } else if (c == '}' || c == ']') {
      depth--;
      if (depth < 0) {
        return false;
      }
      if (depth == 0) {
        end = i + 1;
        break;
      }
    }
  }
  if (in_string || depth != 0 || end == 0) {
    return false;
  }
  // A second top-level value (`{"type":"led","state":"idle"} {"type":…}`) is not one JSON
  // document; serde fails it after the first value and so does this.
  for (size_t k = end; k < len; k++) {
    if (!is_ws(json[k])) {
      return false;
    }
  }
  return true;
}

// Finds top-level key `key` in the payload's own object and returns a pointer to the first
// byte of its value, or nullptr.
//
// Only depth 1 (the payload object itself) is searched, and only in key position: a key
// nested inside a value is not a top-level key, so `{"type":"earcon","meta":{"name":"ghost"}}`
// has no `name` — which is exactly how serde_json reads it ("missing field `name`"). A
// *repeated* top-level key sets `*duplicate`, because serde rejects a repeated field
// ("duplicate field `state`") instead of silently picking the first or the last.
inline const char *find_key(const char *json, size_t len, const char *key, bool *duplicate) {
  const size_t key_len = std::strlen(key);
  const char *found = nullptr;
  int depth = 0;
  char last = 0;  // last significant byte outside a string: '{' or ',' opens a key slot
  size_t i = 0;
  while (i < len) {
    const char c = json[i];
    if (c == '"') {
      size_t end = i + 1;
      while (end < len && json[end] != '"') {
        end += (json[end] == '\\') ? 2 : 1;
      }
      if (end >= len) {
        break;  // unterminated string; json_object_well_formed already refused this
      }
      if (depth == 1 && (last == '{' || last == ',') && end - i - 1 == key_len &&
          std::memcmp(json + i + 1, key, key_len) == 0) {
        size_t j = end + 1;
        while (j < len && is_ws(json[j])) {
          j++;
        }
        if (j < len && json[j] == ':') {
          j++;
          while (j < len && is_ws(json[j])) {
            j++;
          }
          if (found != nullptr && duplicate != nullptr) {
            *duplicate = true;
          }
          if (j < len) {
            found = json + j;
          }
        }
      }
      last = '"';
      i = end + 1;
      continue;
    }
    if (!is_ws(c)) {
      if (c == '{' || c == '[') {
        depth++;
      } else if (c == '}' || c == ']') {
        depth--;
      }
      last = c;
    }
    i++;
  }
  return found;
}

// Copies a JSON string value into `out` (NUL-terminated). Escape support: \" \\ \/ \b \f
// \n \r \t and \uXXXX, including surrogate pairs, encoded as the same UTF-8 bytes
// serde_json produces (a raw non-ASCII character already arrives as UTF-8 and is copied
// verbatim). A repeated top-level key, a value that is not a string, or a NUL (`\u0000`,
// unrepresentable in a C string) is refused rather than guessed at.
inline bool copy_json_string(const char *json, size_t len, const char *key, char *out, size_t out_cap) {
  bool duplicate = false;
  const char *p = find_key(json, len, key, &duplicate);
  if (p == nullptr || duplicate || out_cap == 0) {
    return false;
  }
  const size_t at = static_cast<size_t>(p - json);
  if (at >= len || json[at] != '"') {
    return false;
  }
  size_t i = at + 1;
  size_t n = 0;
  while (i < len) {
    char c = json[i];
    if (c == '"') {
      if (n >= out_cap) {
        return false;
      }
      out[n] = '\0';
      return true;
    }
    if (c == '\\') {
      if (i + 1 >= len) {
        return false;
      }
      const char e = json[++i];
      switch (e) {
        case '"':
          c = '"';
          break;
        case '\\':
          c = '\\';
          break;
        case '/':
          c = '/';
          break;
        case 'b':
          c = '\b';
          break;
        case 'f':
          c = '\f';
          break;
        case 'n':
          c = '\n';
          break;
        case 'r':
          c = '\r';
          break;
        case 't':
          c = '\t';
          break;
        case 'u': {
          if (i + 4 >= len) {
            return false;
          }
          unsigned v = 0;
          for (int k = 1; k <= 4; k++) {
            const char h = json[i + k];
            v <<= 4;
            if (h >= '0' && h <= '9') {
              v |= static_cast<unsigned>(h - '0');
            } else if (h >= 'a' && h <= 'f') {
              v |= static_cast<unsigned>(h - 'a' + 10);
            } else if (h >= 'A' && h <= 'F') {
              v |= static_cast<unsigned>(h - 'A' + 10);
            } else {
              return false;
            }
          }
          i += 4;
          // JSON escapes a character by number, so the bytes that come out must be the
          // UTF-8 serde_json produces for the same character ("caf\u00e9" -> café).
          if (v >= 0xD800 && v <= 0xDBFF) {
            // High surrogate: an astral character arrives as a pair.
            if (i + 6 >= len || json[i + 1] != '\\' || json[i + 2] != 'u') {
              return false;
            }
            unsigned lo = 0;
            for (int k = 3; k <= 6; k++) {
              const char h = json[i + k];
              lo <<= 4;
              if (h >= '0' && h <= '9') {
                lo |= static_cast<unsigned>(h - '0');
              } else if (h >= 'a' && h <= 'f') {
                lo |= static_cast<unsigned>(h - 'a' + 10);
              } else if (h >= 'A' && h <= 'F') {
                lo |= static_cast<unsigned>(h - 'A' + 10);
              } else {
                return false;
              }
            }
            if (lo < 0xDC00 || lo > 0xDFFF) {
              return false;
            }
            i += 6;
            v = 0x10000u + ((v - 0xD800u) << 10) + (lo - 0xDC00u);
          } else if (v >= 0xDC00 && v <= 0xDFFF) {
            return false;  // lone low surrogate: serde_json refuses this too
          }
          if (v == 0) {
            return false;  // \u0000 cannot live in the NUL-terminated string the caller gets
          }
          unsigned char utf8[4];
          size_t nbytes = 0;
          if (v < 0x80) {
            utf8[nbytes++] = static_cast<unsigned char>(v);
          } else if (v < 0x800) {
            utf8[nbytes++] = static_cast<unsigned char>(0xC0 | (v >> 6));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | (v & 0x3F));
          } else if (v < 0x10000) {
            utf8[nbytes++] = static_cast<unsigned char>(0xE0 | (v >> 12));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | ((v >> 6) & 0x3F));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | (v & 0x3F));
          } else {
            utf8[nbytes++] = static_cast<unsigned char>(0xF0 | (v >> 18));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | ((v >> 12) & 0x3F));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | ((v >> 6) & 0x3F));
            utf8[nbytes++] = static_cast<unsigned char>(0x80 | (v & 0x3F));
          }
          if (n + nbytes >= out_cap) {
            return false;
          }
          for (size_t k = 0; k < nbytes; k++) {
            out[n++] = static_cast<char>(utf8[k]);
          }
          i++;
          continue;
        }
        default:
          return false;
      }
      i++;
    } else {
      // JSON forbids an unescaped control character (U+0000..U+001F) inside a string, and
      // serde_json rejects the payload for it. Copying the byte instead would be worse than
      // wrong for a NUL: the C string the caller gets would read as a shorter name, so the
      // satellite would play a different earcon than the backend asked for.
      if (static_cast<unsigned char>(c) < 0x20) {
        return false;
      }
      i++;
    }
    if (n + 1 >= out_cap) {
      return false;
    }
    out[n++] = c;
  }
  return false;  // unterminated string
}

}  // namespace detail

// Decodes a control JSON payload (no tag/length header). Unknown `type`, an
// unknown `led.state` value, or a malformed payload returns false: the caller
// fires nothing rather than guessing.
inline bool parse_control_json(const char *json, size_t len, Control *out) {
  if (json == nullptr || out == nullptr || len == 0) {
    return false;
  }
  if (!detail::json_object_well_formed(json, len)) {
    return false;
  }
  Control c;
  char type[16];
  if (!detail::copy_json_string(json, len, "type", type, sizeof(type))) {
    return false;
  }
  if (std::strcmp(type, "led") == 0) {
    char state[16];
    if (!detail::copy_json_string(json, len, "state", state, sizeof(state))) {
      return false;
    }
    c.led = led_state_from_name(state, std::strlen(state));
    if (c.led == LedState::Unknown) {
      return false;
    }
    c.type = ControlType::Led;
  } else if (std::strcmp(type, "earcon") == 0) {
    if (!detail::copy_json_string(json, len, "name", c.name, sizeof(c.name))) {
      return false;
    }
    c.type = ControlType::Earcon;
  } else if (std::strcmp(type, "play") == 0) {
    if (!detail::copy_json_string(json, len, "url", c.url, sizeof(c.url))) {
      return false;
    }
    if (!detail::copy_json_string(json, len, "format", c.format, sizeof(c.format))) {
      return false;
    }
    c.type = ControlType::Play;
  } else {
    return false;
  }
  *out = c;
  return true;
}

// ---------------------------------------------------------------------------
// downlink: incremental decoder
// ---------------------------------------------------------------------------
// TCP delivers arbitrary byte counts. `feed` consumes bytes until it has one
// complete control (Status::Pending, drained with `take`), runs out of input
// (Status::NeedMore) or hits a protocol error (Status::Error, latched until
// `reset`, mirroring the backend's first-error-wins FrameDecoder).
class DownlinkDecoder {
 public:
  enum class Status : uint8_t { NeedMore, Pending, Error };

  Status feed(const uint8_t *data, size_t len, size_t *consumed) {
    if (consumed != nullptr) {
      *consumed = 0;
    }
    if (this->error_ != 0) {
      return Status::Error;
    }
    size_t i = 0;
    while (i < len) {
      switch (this->state_) {
        case State::Tag:
          if (data[i] != TAG_CONTROL) {
            this->error_ = DECODER_ERR_BAD_TAG;
            i++;
            if (consumed != nullptr) {
              *consumed = i;
            }
            return Status::Error;
          }
          i++;
          this->state_ = State::LenLo;
          break;
        case State::LenLo:
          this->need_ = data[i++];
          this->state_ = State::LenHi;
          break;
        case State::LenHi:
          this->need_ = static_cast<uint16_t>(this->need_ | (static_cast<uint16_t>(data[i++]) << 8));
          if (this->need_ > MAX_CONTROL_JSON) {
            this->error_ = DECODER_ERR_PAYLOAD_TOO_LONG;
            if (consumed != nullptr) {
              *consumed = i;
            }
            return Status::Error;
          }
          this->got_ = 0;
          this->state_ = State::Payload;
          break;
        case State::Payload: {
          size_t take = this->need_ - this->got_;
          if (take > len - i) {
            take = len - i;
          }
          if (take > 0) {
            std::memcpy(this->payload_ + this->got_, data + i, take);
            this->got_ = static_cast<uint16_t>(this->got_ + take);
            i += take;
          }
          if (this->got_ < this->need_) {
            break;  // need more bytes for this payload
          }
          Control control;
          if (!parse_control_json(reinterpret_cast<const char *>(this->payload_), this->need_, &control)) {
            this->error_ = DECODER_ERR_BAD_JSON;
            if (consumed != nullptr) {
              *consumed = i;
            }
            return Status::Error;
          }
          this->pending_ = control;
          this->has_pending_ = true;
          this->state_ = State::Tag;
          this->need_ = 0;
          this->got_ = 0;
          if (consumed != nullptr) {
            *consumed = i;
          }
          return Status::Pending;
        }
      }
    }
    if (consumed != nullptr) {
      *consumed = i;
    }
    return Status::NeedMore;
  }

  bool take(Control *out) {
    if (!this->has_pending_) {
      return false;
    }
    if (out != nullptr) {
      *out = this->pending_;
    }
    this->has_pending_ = false;
    return true;
  }

  bool has_pending() const { return this->has_pending_; }
  bool has_error() const { return this->error_ != 0; }
  uint8_t error_code() const { return this->error_; }

  void reset() {
    this->state_ = State::Tag;
    this->need_ = 0;
    this->got_ = 0;
    this->has_pending_ = false;
    this->error_ = 0;
    this->pending_ = Control{};
  }

 private:
  enum class State : uint8_t { Tag, LenLo, LenHi, Payload };

  State state_{State::Tag};
  uint16_t need_{0};
  uint16_t got_{0};
  uint8_t payload_[MAX_CONTROL_JSON]{0};
  Control pending_;
  bool has_pending_{false};
  uint8_t error_{0};
};

// ---------------------------------------------------------------------------
// outbound buffer: whole records in, resumable partial writes out
// ---------------------------------------------------------------------------
// `consume(n)` is what makes a half-written record safe: the caller only ever
// removes bytes the socket actually accepted, so a frame is neither dropped nor
// duplicated across `send()` calls that accept fewer bytes than offered.
class OutboundBuffer {
 public:
  void init(uint8_t *storage, size_t capacity) {
    this->buf_ = storage;
    this->cap_ = capacity;
    this->head_ = 0;
    this->tail_ = 0;
  }

  size_t capacity() const { return this->cap_; }
  size_t pending() const { return this->tail_ - this->head_; }
  const uint8_t *head() const { return this->buf_ != nullptr ? this->buf_ + this->head_ : nullptr; }

  bool push(const uint8_t *data, size_t len) {
    if (this->buf_ == nullptr || data == nullptr || len == 0) {
      return false;
    }
    if (this->head_ > 0 && this->tail_ + len > this->cap_) {
      const size_t pending = this->pending();
      std::memmove(this->buf_, this->buf_ + this->head_, pending);
      this->head_ = 0;
      this->tail_ = pending;
    }
    if (this->tail_ + len > this->cap_) {
      return false;
    }
    std::memcpy(this->buf_ + this->tail_, data, len);
    this->tail_ += len;
    return true;
  }

  void consume(size_t n) {
    if (n == 0) {
      return;
    }
    const size_t pending = this->pending();
    this->head_ += (n >= pending) ? pending : n;
    if (this->head_ == this->tail_) {
      this->head_ = 0;
      this->tail_ = 0;
    }
  }

  void clear() {
    this->head_ = 0;
    this->tail_ = 0;
  }

 private:
  uint8_t *buf_{nullptr};
  size_t cap_{0};
  size_t head_{0};
  size_t tail_{0};
};

// 1 s, 2 s, 4 s, ... capped at 30 s (attempt 0 == first retry).
inline uint32_t reconnect_delay_ms(unsigned attempt) {
  if (attempt > 5) {
    return RECONNECT_MAX_MS;
  }
  const uint64_t ms = static_cast<uint64_t>(RECONNECT_MIN_MS) << attempt;
  return ms > RECONNECT_MAX_MS ? RECONNECT_MAX_MS : static_cast<uint32_t>(ms);
}

// A connection counts as "settled" — healthy enough to reset the reconnect
// backoff — only after it has survived this long. A backend that accepts the
// TCP connection and then rejects the hello (bad token, unknown source_id)
// closes within milliseconds, so it never settles and its retries climb the
// ladder above instead of hammering at ~1 Hz (FW-4); any real session clears
// the threshold easily and keeps fast recovery after an eventual drop.
static constexpr uint32_t RECONNECT_SETTLE_MS = 5000;

// Wrap-safe comparison on the 32-bit millis() clock: the signed-difference
// form, the same one the component's reconnect due-date uses.
inline bool reconnect_settled(uint32_t now_ms, uint32_t connected_at_ms) {
  return static_cast<int32_t>(now_ms - connected_at_ms) >= static_cast<int32_t>(RECONNECT_SETTLE_MS);
}

}  // namespace luna_stream_codec

// ===========================================================================
// Host test — compiled directly, no ESPHome toolchain:
//   g++ -std=c++17 -Wall -Wextra -Werror -Wno-pragma-once-outside-header -O2
//       -DLUNA_STREAM_HOST_TEST -x c++ luna_stream.h -o /tmp/luna_framing_test
//   /tmp/luna_framing_test
// ===========================================================================
#if defined(LUNA_STREAM_HOST_TEST)

#include <cstdio>
#include <cstdlib>
#include <string>

// LUNA_STREAM_NO_TEST_MAIN compiles the codec alone, without this self-test, so the
// wire-contract differential harness (tests/wire_diff.cpp) can link the same codec and
// bring its own main.
#if !defined(LUNA_STREAM_NO_TEST_MAIN)
namespace luna_stream_test {

using namespace luna_stream_codec;

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond, what)                                                    \
  do {                                                                       \
    g_checks++;                                                              \
    if (!(cond)) {                                                           \
      g_failures++;                                                          \
      std::printf("FAIL %s (%s:%d)\n", (what), __FILE__, __LINE__);           \
    }                                                                        \
  } while (0)

static bool bytes_equal(const uint8_t *got, size_t got_len, const char *want) {
  const size_t want_len = std::strlen(want);
  return got_len == want_len && std::memcmp(got, want, want_len) == 0;
}

static void test_constants() {
  CHECK(FRAME_SAMPLES == 1024, "1024 samples per frame");
  CHECK(FRAME_PCM_BYTES == 2048, "2048 PCM bytes per frame");
  CHECK(AUDIO_FRAME_LEN == 2052, "frame (seq+pcm) is 2052 bytes");
  CHECK(AUDIO_RECORD_LEN == 2053, "on-socket record is 2053 bytes");
  CHECK(TAG_AUDIO == 0x01 && TAG_EVENT == 0x02 && TAG_CONTROL == 0x03, "tag bytes");
  CHECK(SAMPLE_RATE_HZ == 16000, "16 kHz");
  std::printf("PASS constants\n");
}

static void test_hello() {
  char buf[512];
  HelloFields h;
  h.source_id = "kitchen";
  h.room = "kitchen";
  h.token = "s3cret";
  h.fw = "0.1.0";
  h.has_speaker = true;
  h.has_aec = false;
  const size_t n = build_hello(buf, sizeof(buf), h);
  CHECK(n > 0 && n == std::strlen(buf), "hello is NUL-terminated exactly at its returned length");
  CHECK(n < sizeof(buf), "hello fits its buffer");
  CHECK(
      bytes_equal(reinterpret_cast<const uint8_t *>(buf), n,
                   "{\"v\":1,\"source_id\":\"kitchen\",\"room\":\"kitchen\",\"token\":\"s3cret\","
                   "\"caps\":{\"has_speaker\":true,\"has_aec\":false},\"fw\":\"0.1.0\"}\n"),
      "hello line bytes match proto::encode_hello_line");

  // No trailing newline inside the buffer, exactly one at the end.
  CHECK(n >= 2 && buf[n - 1] == '\n' && buf[n - 2] == '}', "single trailing newline");

  // Optional token -> JSON null (backend's Option<String>).
  h.token = nullptr;
  const size_t n2 = build_hello(buf, sizeof(buf), h);
  std::string null_tok(buf, n2);
  CHECK(null_tok.find("\"token\":null") != std::string::npos, "absent token encodes as null");

  // Escaping + escaping-driven capacity failure.
  h.token = "a\"b\\c";
  h.room = "r\tx";
  const size_t n3 = build_hello(buf, sizeof(buf), h);
  std::string esc(buf, n3);
  CHECK(esc.find("\"token\":\"a\\\"b\\\\c\"") != std::string::npos, "token escaping");
  CHECK(esc.find("\"room\":\"r\\tx\"") != std::string::npos, "control-char escaping");
  char tiny[16];
  CHECK(build_hello(tiny, sizeof(tiny), h) == 0, "too-small hello buffer returns 0");
  std::printf("PASS hello\n");
}

static void test_audio_record() {
  static uint8_t pcm[FRAME_PCM_BYTES];
  for (size_t i = 0; i < sizeof(pcm); i++) {
    pcm[i] = static_cast<uint8_t>(i & 0xFF);
  }
  uint8_t rec[AUDIO_RECORD_LEN + 8];
  std::memset(rec, 0xAA, sizeof(rec));

  size_t n = build_audio_record(rec, sizeof(rec), 0, pcm, sizeof(pcm));
  CHECK(n == AUDIO_RECORD_LEN, "seq=0 record length is 2053");
  CHECK(rec[0] == 0x01, "audio tag byte");
  CHECK(rec[1] == 0x00 && rec[2] == 0x00 && rec[3] == 0x00 && rec[4] == 0x00, "seq 0 is four zero bytes");
  CHECK(std::memcmp(rec + 5, pcm, sizeof(pcm)) == 0, "PCM payload copied verbatim at offset 5");
  CHECK(rec[AUDIO_RECORD_LEN] == 0xAA, "record does not write past 2053 bytes");

  n = build_audio_record(rec, sizeof(rec), 0x01020304u, pcm, sizeof(pcm));
  CHECK(n == AUDIO_RECORD_LEN, "seq=0x01020304 record length");
  CHECK(rec[1] == 0x04 && rec[2] == 0x03 && rec[3] == 0x02 && rec[4] == 0x01, "seq is u32 little-endian");

  // Sequence with the top bit set (0x80000000) to prove unsigned shifting.
  build_audio_record(rec, sizeof(rec), 0x80000000u, pcm, sizeof(pcm));
  CHECK(rec[1] == 0x00 && rec[4] == 0x80, "seq high bit little-endian");

  // Wrong sample count is refused, never padded.
  CHECK(build_audio_record(rec, sizeof(rec), 7, pcm, sizeof(pcm) - 2) == 0, "short PCM refused");
  CHECK(build_audio_record(rec, AUDIO_RECORD_LEN - 1, 7, pcm, sizeof(pcm)) == 0, "small buffer refused");
  std::printf("PASS audio record\n");
}

static void test_events() {
  uint8_t rec[512];
  // Every body is compared byte-for-byte against the literal the contract
  // specifies; the u16 length is checked against strlen of that same literal,
  // so neither number is hand-counted here.
  struct Row {
    const char *json;
    const char *what;
  };

  size_t n = build_event_playback_done(rec, sizeof(rec));
  const Row playback_done = {"{\"type\":\"playback_done\"}", "playback_done body"};
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, playback_done.json), playback_done.what);
  CHECK(rec[0] == TAG_EVENT, "playback_done tag");
  CHECK(n == TAGGED_HEADER_LEN + std::strlen(playback_done.json), "playback_done length");
  CHECK(rec[1] == (uint8_t) (std::strlen(playback_done.json) & 0xFF) && rec[2] == 0,
        "playback_done len u16 LE");

  n = build_event_save_buffer(rec, sizeof(rec));
  const Row save_buffer = {"{\"type\":\"save_buffer\"}", "save_buffer body (the for-you button, never button)"};
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, save_buffer.json), save_buffer.what);
  CHECK(rec[0] == TAG_EVENT, "save_buffer tag");
  CHECK(n == TAGGED_HEADER_LEN + std::strlen(save_buffer.json), "save_buffer length");
  CHECK(std::strstr(save_buffer.json, "button") == nullptr, "save_buffer is not a button event");

  n = build_event_button(rec, sizeof(rec), "night_mode");
  const Row button = {"{\"type\":\"button\",\"name\":\"night_mode\"}", "button body"};
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, button.json), button.what);
  CHECK(rec[0] == TAG_EVENT, "button tag");
  CHECK(n == TAGGED_HEADER_LEN + std::strlen(button.json), "button record length");
  CHECK(rec[1] == (uint8_t) (std::strlen(button.json) & 0xFF) && rec[2] == 0, "button len u16 LE");

  n = build_event_rssi(rec, sizeof(rec), -625);
  const Row rssi = {"{\"type\":\"rssi\",\"dbm\":-62.5}", "rssi -62.5 body"};
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, rssi.json), rssi.what);
  CHECK(n == TAGGED_HEADER_LEN + std::strlen(rssi.json), "rssi length");
  CHECK(rec[1] == (uint8_t) (std::strlen(rssi.json) & 0xFF) && rec[2] == 0, "rssi len u16 LE");

  n = build_event_rssi(rec, sizeof(rec), -620);
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, "{\"type\":\"rssi\",\"dbm\":-62.0}"),
        "rssi -62.0 keeps one decimal like serde_json floats");
  n = build_event_rssi(rec, sizeof(rec), 0);
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, "{\"type\":\"rssi\",\"dbm\":0.0}"), "rssi 0.0");
  n = build_event_rssi(rec, sizeof(rec), -5);
  CHECK(bytes_equal(rec + TAGGED_HEADER_LEN, n - TAGGED_HEADER_LEN, "{\"type\":\"rssi\",\"dbm\":-0.5}"),
        "rssi -0.5");
  CHECK(build_event_button(rec, 20, "night_mode") == 0, "small buffer refused for events");
  std::printf("PASS events\n");
}

static void test_control_json() {
  Control c;
  // Every payload length comes from strlen of the literal, never from counting.
  auto parse = [&c](const char *json) { return parse_control_json(json, std::strlen(json), &c); };

  CHECK(parse("{\"type\":\"led\",\"state\":\"listening\"}"), "led/listening parses");
  CHECK(c.type == ControlType::Led && c.led == LedState::Listening, "led state decoded");
  CHECK(std::strcmp(led_state_name(c.led), "listening") == 0, "led name round-trips");

  CHECK(parse("{\"type\":\"led\",\"state\":\"speaking\"}") && c.led == LedState::Speaking, "led/speaking");
  CHECK(parse("{\"type\":\"led\",\"state\":\"idle\"}") && c.led == LedState::Idle, "led/idle");
  CHECK(parse("{\"type\":\"led\",\"state\":\"muted\"}") && c.led == LedState::Muted, "led/muted");

  CHECK(parse("{\"type\":\"earcon\",\"name\":\"ack\"}"), "earcon parses");
  CHECK(c.type == ControlType::Earcon && std::strcmp(c.name, "ack") == 0, "earcon name");

  CHECK(parse("{\"type\":\"play\",\"url\":\"http://ha/api/tts/1.flac\",\"format\":\"flac\"}"),
        "reserved play parses");
  CHECK(c.type == ControlType::Play && std::strcmp(c.format, "flac") == 0, "play format");
  CHECK(std::strstr(c.url, "tts/1.flac") != nullptr, "play url");

  // Extra keys and whitespace are tolerated; unknown values are not guessed.
  CHECK(parse("{ \"type\" : \"led\" , \"state\" : \"idle\" , \"extra\": 7 }") && c.led == LedState::Idle,
        "whitespace and unknown keys tolerated");
  CHECK(parse("{\"type\":\"earcon\",\"name\":\"d\\u0069ng\"}") && std::strcmp(c.name, "ding") == 0,
        "unicode escape decoded");
  CHECK(parse("{\"type\":\"earcon\",\"name\":\"a{}b\\\"c\"}") && std::strcmp(c.name, "a{}b\"c") == 0,
        "braces and escaped quotes inside a string value are not structural");
  CHECK(parse("{\"type\":\"led\",\"state\":\"idle\"},\"x\":1}") == false, "extra closing brace rejected");
  // Ends with '}' yet never closes: this is the only input that exercises the
  // depth counter independently of the trailing-brace check.
  CHECK(!parse("{\"type\":\"led\",\"state\":\"idle\",{\"a\":1}"), "unclosed nested object rejected");
  CHECK(!parse("{\"type\":\"led\",\"state\":{\"nested\":\"idle\"}}"), "nested object as a state value rejected");

  // Rejections: the firmware fires nothing rather than guessing.
  CHECK(!parse_control_json("", 0, &c), "empty payload rejected");
  CHECK(!parse("{\"type\":\"bogus\"}"), "unknown control type rejected");
  CHECK(!parse("{\"type\":\"led\",\"state\":\"blinking\"}"), "unknown led state rejected");
  CHECK(!parse("{\"type\":\"led\"}"), "led without state rejected");
  CHECK(!parse("{\"type\":\"earcon\"}"), "earcon without name rejected");
  CHECK(!parse("{\"type\":\"play\",\"url\":\"u\"}"), "play without format rejected");
  CHECK(!parse("{\"type\":\"led\",\"state\":\"idle\""), "unterminated json rejected");
  CHECK(!parse("{\"led\":\"idle\"}"), "payload without type rejected");
  std::printf("PASS control json\n");
}

// Builds `0x03 | len u16 LE | json` the way the backend's encode_control does.
static size_t make_control_record(uint8_t *out, const char *json) {
  const size_t len = std::strlen(json);
  return build_tagged(out, MAX_CONTROL_JSON + TAGGED_HEADER_LEN, TAG_CONTROL, json, len);
}

static const char *const CONTROL_LED_MUTED = "{\"type\":\"led\",\"state\":\"muted\"}";

static void test_downlink_whole_and_split() {
  uint8_t rec[MAX_CONTROL_JSON + TAGGED_HEADER_LEN];
  const size_t body_len = std::strlen(CONTROL_LED_MUTED);
  const size_t n = make_control_record(rec, CONTROL_LED_MUTED);
  CHECK(n == TAGGED_HEADER_LEN + body_len, "control record length = 3-byte header + payload");
  CHECK(rec[0] == 0x03, "control tag byte");
  CHECK(rec[1] == (uint8_t) (body_len & 0xFF) && rec[2] == (uint8_t) ((body_len >> 8) & 0xFF),
        "control header is tag + u16 LE len");

  // Whole record in one feed.
  {
    DownlinkDecoder d;
    size_t consumed = 0;
    CHECK(d.feed(rec, n, &consumed) == DownlinkDecoder::Status::Pending, "whole record -> Pending");
    CHECK(consumed == n, "whole record consumed entirely");
    Control c;
    CHECK(d.take(&c) && c.led == LedState::Muted, "whole record decodes muted");
    CHECK(!d.take(&c), "pending control is drained once");
    CHECK(!d.has_error(), "no error after a good record");
  }

  // Byte-at-a-time feeding must agree with the whole-buffer feed.
  {
    DownlinkDecoder d;
    bool got = false;
    for (size_t i = 0; i < n; i++) {
      size_t consumed = 0;
      const auto st = d.feed(rec + i, 1, &consumed);
      CHECK(consumed == 1, "one byte is consumed per feed");
      if (st == DownlinkDecoder::Status::Pending) {
        Control c;
        CHECK(d.take(&c) && c.led == LedState::Muted, "byte-at-a-time decodes muted");
        got = true;
      } else {
        CHECK(st == DownlinkDecoder::Status::NeedMore, "byte-at-a-time is NeedMore until the last byte");
      }
    }
    CHECK(got, "byte-at-a-time produced the control exactly once");
    CHECK(!d.has_pending(), "no duplicate control");
  }

  // Two records back-to-back in one buffer: first is Pending, remainder re-fed.
  {
    uint8_t two[MAX_CONTROL_JSON + TAGGED_HEADER_LEN];
    const size_t a = make_control_record(two, "{\"type\":\"earcon\",\"name\":\"ack\"}");
    const size_t b = make_control_record(two + a, "{\"type\":\"led\",\"state\":\"idle\"}");
    DownlinkDecoder d;
    size_t consumed = 0;
    CHECK(d.feed(two, a + b, &consumed) == DownlinkDecoder::Status::Pending, "first of two -> Pending");
    CHECK(consumed == a, "only the first record is consumed");
    Control c;
    CHECK(d.take(&c) && c.type == ControlType::Earcon, "first record is the earcon");
    CHECK(d.feed(two + consumed, a + b - consumed, &consumed) == DownlinkDecoder::Status::Pending,
          "second record -> Pending");
    CHECK(d.take(&c) && c.type == ControlType::Led && c.led == LedState::Idle, "second record is led/idle");
  }
  std::printf("PASS downlink decode (whole, split, back-to-back)\n");
}

static void test_downlink_truncated() {
  // Truncated body: the 3-byte header plus the first 10 payload bytes only.
  const size_t split = TAGGED_HEADER_LEN + 10;
  uint8_t rec[MAX_CONTROL_JSON + TAGGED_HEADER_LEN];
  const size_t n = make_control_record(rec, CONTROL_LED_MUTED);
  CHECK(split < n, "the split really is a truncation");
  DownlinkDecoder d;
  size_t consumed = 0;
  CHECK(d.feed(rec, split, &consumed) == DownlinkDecoder::Status::NeedMore, "truncated body -> NeedMore");
  CHECK(consumed == split, "all truncated bytes are buffered");
  CHECK(!d.has_pending(), "no control emitted from a truncated record");
  CHECK(!d.has_error(), "a truncated record is not an error");
  Control c;
  CHECK(!d.take(&c), "take on a truncated record yields nothing");

  // The rest arrives: exactly one control, then a following record still decodes.
  uint8_t both[MAX_CONTROL_JSON + TAGGED_HEADER_LEN];
  const size_t m = make_control_record(both, "{\"type\":\"earcon\",\"name\":\"ack\"}");
  uint8_t tail[64];
  std::memcpy(tail, rec + split, n - split);
  std::memcpy(tail + (n - split), both, m);
  const size_t tail_len = (n - split) + m;
  CHECK(d.feed(tail, tail_len, &consumed) == DownlinkDecoder::Status::Pending, "completed record -> Pending");
  CHECK(consumed == n - split, "consumed exactly the buffered remainder");
  CHECK(d.take(&c) && c.led == LedState::Muted, "resumed record decodes muted after truncation");
  CHECK(d.feed(tail + consumed, tail_len - consumed, &consumed) == DownlinkDecoder::Status::Pending,
        "decoder is not wedged after truncation");
  CHECK(d.take(&c) && c.type == ControlType::Earcon && std::strcmp(c.name, "ack") == 0, "next record decodes");

  // Header-only truncation (tag + one length byte).
  DownlinkDecoder d2;
  CHECK(d2.feed(rec, 2, &consumed) == DownlinkDecoder::Status::NeedMore, "tag+len_lo only -> NeedMore");
  CHECK(d2.feed(rec + 2, n - 2, &consumed) == DownlinkDecoder::Status::Pending, "len_hi + body completes it");

  // Errors are latched until reset.
  DownlinkDecoder d3;
  const uint8_t bad_tag[4] = {0x07, 0x00, 0x00, 0x00};
  CHECK(d3.feed(bad_tag, sizeof(bad_tag), &consumed) == DownlinkDecoder::Status::Error, "wrong tag -> Error");
  CHECK(d3.error_code() == DECODER_ERR_BAD_TAG, "error code BAD_TAG");
  CHECK(d3.feed(rec, n, &consumed) == DownlinkDecoder::Status::Error, "error stays latched");
  d3.reset();
  CHECK(!d3.has_error(), "reset clears the latched error");
  CHECK(d3.feed(rec, n, &consumed) == DownlinkDecoder::Status::Pending, "usable again after reset");

  // Oversize length field is refused without buffering 64 KiB.
  DownlinkDecoder d4;
  const uint8_t oversize[3] = {0x03, 0x01, 0x08};  // 0x0801 = 2049 > MAX_CONTROL_JSON
  CHECK(d4.feed(oversize, sizeof(oversize), &consumed) == DownlinkDecoder::Status::Error,
        "oversize length -> Error");
  CHECK(d4.error_code() == DECODER_ERR_PAYLOAD_TOO_LONG, "error code PAYLOAD_TOO_LONG");

  // Bad JSON in a well-framed record.
  uint8_t junk[MAX_CONTROL_JSON + TAGGED_HEADER_LEN];
  const size_t jn = make_control_record(junk, "{\"type\":\"nope\"}");
  DownlinkDecoder d5;
  CHECK(d5.feed(junk, jn, &consumed) == DownlinkDecoder::Status::Error, "unknown type -> Error");
  CHECK(d5.error_code() == DECODER_ERR_BAD_JSON, "error code BAD_JSON");
  std::printf("PASS downlink truncated/error paths\n");
}

// Fake socket that accepts at most `chunk` bytes per call, to prove a partial
// write resumes from the exact byte it stopped at.
static void test_partial_write_resume() {
  static uint8_t storage[4 * AUDIO_RECORD_LEN + 64];
  OutboundBuffer out;
  out.init(storage, sizeof(storage));

  static uint8_t pcm[FRAME_PCM_BYTES];
  for (size_t i = 0; i < sizeof(pcm); i++) {
    pcm[i] = static_cast<uint8_t>((i * 7) & 0xFF);
  }
  uint8_t expected[AUDIO_RECORD_LEN];
  const size_t frame_len = build_audio_record(expected, sizeof(expected), 0, pcm, sizeof(pcm));
  CHECK(frame_len == AUDIO_RECORD_LEN, "expected frame length");

  CHECK(out.push(expected, frame_len), "whole record fits the queue");
  CHECK(out.pending() == frame_len, "queue holds exactly one record");

  // A `send()` that accepts only 7 bytes at a time.
  uint8_t received[2 * AUDIO_RECORD_LEN];
  size_t received_len = 0;
  const size_t chunk = 7;
  int sends = 0;
  while (out.pending() > 0) {
    size_t offer = out.pending();
    if (offer > chunk) {
      offer = chunk;
    }
    std::memcpy(received + received_len, out.head(), offer);
    received_len += offer;
    out.consume(offer);
    sends++;
    CHECK(sends < 400, "partial write makes progress");
  }
  CHECK(received_len == frame_len, "no bytes lost and none duplicated across partial writes");
  CHECK(std::memcmp(received, expected, frame_len) == 0, "resumed stream is byte-identical");
  CHECK(out.pending() == 0, "queue drained");
  CHECK(sends == static_cast<int>((frame_len + chunk - 1) / chunk), "exactly ceil(2053/7) partial writes");

  // Header/frame boundary in the middle of a partial write: two records, the
  // socket stops mid-header, and the resume continues into the PCM payload.
  out.clear();
  static uint8_t pcm2[FRAME_PCM_BYTES];
  for (size_t i = 0; i < sizeof(pcm2); i++) {
    pcm2[i] = static_cast<uint8_t>((255 - i) & 0xFF);
  }
  uint8_t expected2[AUDIO_RECORD_LEN];
  build_audio_record(expected2, sizeof(expected2), 1, pcm2, sizeof(pcm2));
  CHECK(out.push(expected, frame_len) && out.push(expected2, frame_len), "queue holds two records");
  size_t total = 0;
  size_t stop_at = 2053 - 3;  // stops three bytes into the second record
  while (out.pending() > 0) {
    size_t offer = out.pending();
    if (total < stop_at && total + offer > stop_at) {
      offer = stop_at - total;
    }
    if (offer > 100) {
      offer = 100;
    }
    total += offer;
    out.consume(offer);
  }
  CHECK(total == 2 * frame_len, "two records written exactly once each");
  CHECK(out.pending() == 0, "queue empty after two records");

  // Queue overflow is reported, not silently truncated.
  static uint8_t small[2 * AUDIO_RECORD_LEN];
  OutboundBuffer tiny;
  tiny.init(small, sizeof(small));
  CHECK(tiny.push(expected, frame_len), "first record accepted");
  CHECK(tiny.push(expected2, frame_len), "second record accepted");
  CHECK(!tiny.push(expected, frame_len), "third record refused when full");
  CHECK(tiny.pending() == 2 * frame_len, "refused record does not disturb pending bytes");
  // Compaction on consume keeps the full capacity usable.
  tiny.consume(frame_len);
  CHECK(tiny.push(expected, frame_len), "capacity is reusable after a partial drain (compaction)");
  std::printf("PASS partial-write resume\n");
}

static void test_backoff() {
  CHECK(reconnect_delay_ms(0) == 1000, "attempt 0 -> 1 s");
  CHECK(reconnect_delay_ms(1) == 2000, "attempt 1 -> 2 s");
  CHECK(reconnect_delay_ms(2) == 4000, "attempt 2 -> 4 s");
  CHECK(reconnect_delay_ms(3) == 8000, "attempt 3 -> 8 s");
  CHECK(reconnect_delay_ms(4) == 16000, "attempt 4 -> 16 s");
  CHECK(reconnect_delay_ms(5) == 30000, "attempt 5 capped at 30 s");
  CHECK(reconnect_delay_ms(6) == 30000, "attempt 6 capped at 30 s");
  CHECK(reconnect_delay_ms(1000) == 30000, "huge attempt capped at 30 s");
  std::printf("PASS reconnect backoff\n");
}

// The cases a differential test against the backend (src/proto.rs + serde_json) caught:
// every payload here is one where the firmware used to disagree with serde.
static void test_wire_contract_parity() {
  Control c;

  // 1. Only the payload's own top-level keys count: `meta.name` is not a `name`
  // (serde: missing field `name`, so the control is rejected).
  const char *nested_name = "{\"type\":\"earcon\",\"meta\":{\"name\":\"ghost\"}}";
  CHECK(!parse_control_json(nested_name, std::strlen(nested_name), &c),
        "nested name is not a top-level name (serde: missing field)");

  // 2. A nested object's key must not shadow the top-level one (serde: state = idle).
  const char *nested_state = "{\"type\":\"led\",\"debug\":{\"state\":\"muted\"},\"state\":\"idle\"}";
  CHECK(parse_control_json(nested_state, std::strlen(nested_state), &c) && c.led == LedState::Idle,
        "nested state ignored; top-level state wins (serde: idle)");

  // 3. A repeated top-level key is a duplicate field, which serde rejects — it must not
  // resolve first-wins (the old bug) or silently last-wins.
  const char *dup_state = "{\"type\":\"led\",\"state\":\"muted\",\"state\":\"idle\"}";
  CHECK(!parse_control_json(dup_state, std::strlen(dup_state), &c),
        "repeated top-level key rejected (serde: duplicate field `state`)");
  const char *dup_same = "{\"type\":\"led\",\"state\":\"idle\",\"state\":\"idle\"}";
  CHECK(!parse_control_json(dup_same, std::strlen(dup_same), &c), "repeated identical key rejected too");
  const char *dup_unknown = "{\"type\":\"led\",\"state\":\"idle\",\"x\":1,\"x\":2}";
  CHECK(parse_control_json(dup_unknown, std::strlen(dup_unknown), &c) && c.led == LedState::Idle,
        "repeated unknown key tolerated (serde ignores unknown fields)");

  // 4. Exactly one JSON value per payload.
  const char *two_objects = "{\"type\":\"led\",\"state\":\"idle\"} {\"type\":\"led\",\"state\":\"muted\"}";
  CHECK(!parse_control_json(two_objects, std::strlen(two_objects), &c),
        "two objects in one payload rejected (serde: trailing characters)");
  const char *padded = "{\"type\":\"led\",\"state\":\"idle\"} \t\r\n";
  CHECK(parse_control_json(padded, std::strlen(padded), &c) && c.led == LedState::Idle,
        "trailing whitespace after the object tolerated");

  // 5. \u escapes decode to the UTF-8 serde produces, not '?'.
  const char *escaped_e = "{\"type\":\"earcon\",\"name\":\"caf\\u00e9\"}";
  CHECK(parse_control_json(escaped_e, std::strlen(escaped_e), &c) && std::strcmp(c.name, "caf\xC3\xA9") == 0,
        "\\u00e9 decodes to the 2-byte UTF-8 cafe-acute");
  const char *escaped_euro = "{\"type\":\"earcon\",\"name\":\"\\u20ac\"}";
  CHECK(parse_control_json(escaped_euro, std::strlen(escaped_euro), &c) &&
            std::strcmp(c.name, "\xE2\x82\xAC") == 0, "\\u20ac decodes to the 3-byte euro sign");
  const char *escaped_emoji = "{\"type\":\"earcon\",\"name\":\"\\ud83d\\ude00\"}";
  CHECK(parse_control_json(escaped_emoji, std::strlen(escaped_emoji), &c) &&
            std::strcmp(c.name, "\xF0\x9F\x98\x80") == 0, "surrogate pair decodes to the 4-byte emoji");
  const char *lone_surrogate = "{\"type\":\"earcon\",\"name\":\"\\ud83d\"}";
  CHECK(!parse_control_json(lone_surrogate, std::strlen(lone_surrogate), &c),
        "lone surrogate rejected (serde rejects it too)");
  const char *nul_escape = "{\"type\":\"earcon\",\"name\":\"a\\u0000b\"}";
  CHECK(!parse_control_json(nul_escape, std::strlen(nul_escape), &c),
        "\\u0000 refused: a NUL cannot live in the C string the caller gets");
  const char *ctrl_escape = "{\"type\":\"earcon\",\"name\":\"a\\u0001b\"}";
  CHECK(parse_control_json(ctrl_escape, std::strlen(ctrl_escape), &c) && std::strlen(c.name) == 3,
        "\\u0001 kept as a byte");
  // A *raw* control byte inside a string is not legal JSON, and a raw NUL would truncate the
  // C string the caller gets (the satellite would play a shorter, wrong earcon). sizeof - 1
  // keeps the embedded NUL in the length, unlike strlen.
  static const char RAW_NUL[] = "{\"type\":\"earcon\",\"name\":\"a\0b\"}";
  CHECK(!parse_control_json(RAW_NUL, sizeof(RAW_NUL) - 1, &c),
        "raw NUL inside a string rejected (illegal JSON; would truncate the name)");
  static const char RAW_TAB[] = "{\"type\":\"earcon\",\"name\":\"a\tb\"}";
  CHECK(!parse_control_json(RAW_TAB, sizeof(RAW_TAB) - 1, &c),
        "raw tab inside a string rejected (illegal JSON)");
  const char *escaped_tab = "{\"type\":\"earcon\",\"name\":\"a\\tb\"}";
  CHECK(parse_control_json(escaped_tab, std::strlen(escaped_tab), &c) && std::strlen(c.name) == 3,
        "escaped tab still accepted as a byte");
  const char *raw_utf8 = "{\"type\":\"earcon\",\"name\":\"caf\xC3\xA9\"}";
  CHECK(parse_control_json(raw_utf8, std::strlen(raw_utf8), &c) && std::strcmp(c.name, "caf\xC3\xA9") == 0,
        "raw UTF-8 name still passes through verbatim");
  const char *type_in_value = "{\"type\":\"earcon\",\"name\":\"\\\"type\\\":\\\"led\\\"\"}";
  CHECK(parse_control_json(type_in_value, std::strlen(type_in_value), &c) &&
            c.type == ControlType::Earcon && std::strcmp(c.name, "\"type\":\"led\"") == 0,
        "a \"type\" inside a string value is not the tag");

  // 6. The buffers hold what the backend legitimately sends (the old name[48]/url[320]
  // dropped these) and refuse — visibly — what is past them.
  char name60[61];
  char url344[345];
  char name600[601];
  char url600[601];
  for (size_t i = 0; i < 60; i++) {
    name60[i] = static_cast<char>('a' + (i % 26));
  }
  name60[60] = '\0';
  for (size_t i = 0; i < 344; i++) {
    url344[i] = static_cast<char>('a' + (i % 26));
  }
  url344[344] = '\0';
  for (size_t i = 0; i < 600; i++) {
    name600[i] = static_cast<char>('a' + (i % 26));
    url600[i] = static_cast<char>('a' + (i % 26));
  }
  name600[600] = '\0';
  url600[600] = '\0';

  char json[768];
  std::snprintf(json, sizeof(json), "{\"type\":\"earcon\",\"name\":\"%s\"}", name60);
  CHECK(parse_control_json(json, std::strlen(json), &c) && std::strcmp(c.name, name60) == 0,
        "60-char earcon name accepted (old name[48] dropped the control)");
  std::snprintf(json, sizeof(json), "{\"type\":\"play\",\"url\":\"%s\",\"format\":\"flac\"}", url344);
  CHECK(parse_control_json(json, std::strlen(json), &c) && std::strcmp(c.url, url344) == 0,
        "344-char play url accepted (old url[320] dropped the control)");
  std::snprintf(json, sizeof(json), "{\"type\":\"earcon\",\"name\":\"%s\"}", name600);
  Control sentinel;
  std::snprintf(sentinel.name, sizeof(sentinel.name), "%s", "sentinel");
  CHECK(!parse_control_json(json, std::strlen(json), &sentinel), "600-char name refused, not cut");
  CHECK(std::strcmp(sentinel.name, "sentinel") == 0,
        "a refused control leaves the caller's value alone (no truncated name to act on)");
  std::snprintf(json, sizeof(json), "{\"type\":\"play\",\"url\":\"%s\",\"format\":\"flac\"}", url600);
  CHECK(!parse_control_json(json, std::strlen(json), &c), "600-char url refused, not cut");

  // 7. Uplink mirror of the same defect: a 300-character button name overflowed the old
  // 256-byte event body and the event was dropped on the way OUT.
  static uint8_t record[TAGGED_HEADER_LEN + MAX_EVENT_JSON + 64];
  char name300[301];
  for (size_t i = 0; i < 300; i++) {
    name300[i] = static_cast<char>('a' + (i % 26));
  }
  name300[300] = '\0';
  // Payload is `{"type":"button","name":"` (25 bytes) + name + `"}` (2 bytes).
  const size_t n = build_event_button(record, sizeof(record), name300);
  CHECK(n == TAGGED_HEADER_LEN + 27 + 300, "300-char button name builds (old body buffer refused it)");
  CHECK(std::memcmp(record + TAGGED_HEADER_LEN + 25, name300, 300) == 0,
        "long button name arrives byte for byte, not truncated");
  CHECK(record[TAGGED_HEADER_LEN + 25 + 300] == '"' && record[TAGGED_HEADER_LEN + 26 + 300] == '}',
        "long button payload is closed exactly after the name");
  char name_over[601];
  for (size_t i = 0; i < 600; i++) {
    name_over[i] = static_cast<char>('a' + (i % 26));
  }
  name_over[600] = '\0';
  CHECK(build_event_button(record, sizeof(record), name_over) == 0,
        "600-char button name refused instead of truncated into a shorter valid name");
  std::printf("PASS wire-contract parity (differential cases)\n");
}

}  // namespace luna_stream_test

int main() {
  using namespace luna_stream_test;
  test_constants();
  test_hello();
  test_audio_record();
  test_events();
  test_control_json();
  test_downlink_whole_and_split();
  test_downlink_truncated();
  test_partial_write_resume();
  test_backoff();
  test_wire_contract_parity();
  if (g_failures != 0) {
    std::printf("%d FAILURE(S) out of %d checks\n", g_failures, g_checks);
    return 1;
  }
  std::printf("ALL PASS (%d checks)\n", g_checks);
  return 0;
}
#endif  // !LUNA_STREAM_NO_TEST_MAIN

#endif  // LUNA_STREAM_HOST_TEST

// ===========================================================================
// ESPHome component (not compiled by the host test)
// ===========================================================================
#if !defined(LUNA_STREAM_HOST_TEST)

#include <atomic>
#include <mutex>
#include <string>
#include <vector>

// Every lwIP declaration this component uses, included explicitly rather than
// relied upon transitively. Note that on this port <netinet/in.h> and
// <arpa/inet.h> are only shims for lwip/inet.h, which defines neither
// sockaddr_in nor AF_INET nor inet_pton — those come from lwip/sockets.h.
#include "lwip/dns.h"      // dns_gethostbyname, dns_found_callback
#include "lwip/err.h"      // err_t, ERR_OK, ERR_INPROGRESS
#include "lwip/ip_addr.h"  // ip_addr_t, IP_IS_V4, ip_2_ip4
#include "lwip/sockets.h"  // sockaddr, sockaddr_in, AF_INET, in_port_t, inet_pton

#include "esphome/core/automation.h"
#include "esphome/core/component.h"
#include "esphome/core/log.h"
#include "esphome/components/media_player/media_player.h"
// MicrophoneSource is how a component requests microphone data in a format of
// its choosing (it pulls in microphone.h and audio/audio.h). It is the
// multi-consumer wrapper micro_wake_word and voice_assistant use too, so the
// physical mic stays shareable.
#include "esphome/components/microphone/microphone_source.h"

#ifdef USE_WIFI
// components/wifi/wifi_component.h — there is no wifi/wifi.h; this is the
// header that declares global_wifi_component and wifi_rssi().
#include "esphome/components/wifi/wifi_component.h"
#endif

namespace esphome {
namespace luna_stream {

class LunaStream : public Component {
 public:
  void setup() override;
  void loop() override;
  void dump_config() override;
  float get_setup_priority() const override { return setup_priority::AFTER_WIFI; }

  void set_backend(const std::string &host, uint16_t port) {
    this->host_ = host;
    this->port_ = port;
  }
  void set_source_id(const std::string &source_id) { this->source_id_ = source_id; }
  void set_room(const std::string &room) { this->room_ = room; }
  void set_token(const std::string &token) { this->token_ = token; }
  void set_firmware_version(const std::string &fw) { this->fw_ = fw; }
  void set_has_speaker(bool has_speaker) { this->has_speaker_ = has_speaker; }
  void set_has_aec(bool has_aec) { this->has_aec_ = has_aec; }
  void set_frame_buffer_frames(uint16_t frames) { this->frame_buffer_frames_ = frames; }
  void set_rx_buffer_bytes(uint16_t bytes) { this->rx_buffer_bytes_ = bytes; }

  // Owned by codegen (microphone.microphone_source_to_code), configured for
  // 16-bit mono and started by this component; see __init__.py.
  void set_mic_source(microphone::MicrophoneSource *mic_source) { this->mic_source_ = mic_source; }
  void set_media_player(media_player::MediaPlayer *media_player) { this->media_player_ = media_player; }

  void set_led_trigger(Trigger<std::string> *trigger) { this->led_trigger_ = trigger; }
  void set_earcon_trigger(Trigger<std::string> *trigger) { this->earcon_trigger_ = trigger; }
  Trigger<std::string> *get_led_trigger() const { return this->led_trigger_; }
  Trigger<std::string> *get_earcon_trigger() const { return this->earcon_trigger_; }

  // Entered the Connected state with a queued hello: the connection itself is
  // the payload, so no arguments. Fired from loop() only (see on_connected_()
  // in the .cpp); registered through `on_connected:` in __init__.py.
  void set_connected_trigger(Trigger<> *trigger) { this->connected_trigger_ = trigger; }
  Trigger<> *get_connected_trigger() const { return this->connected_trigger_; }

  // Uplink macros pinned by the frozen contract (plan B3-1 / src/proto.rs):
  // "for you" is `save_buffer`, every other button is a generic `button`.
  void emit_save_buffer();
  void emit_button(const std::string &name);

  // Reports the media_player reaching idle -> `playback_done`. Registered on
  // the media_player in setup(); public so YAML may also call it directly.
  void notify_media_player_idle();

  bool connected() const;

 protected:
  // Disconnected -> [Resolving] -> Connecting -> Connected. Resolving exists
  // only for hostname configs (FW-1): the numeric fast path never enters it.
  enum class ConnectionState : uint8_t { Disconnected, Resolving, Connecting, Connected };

  bool start_connect_();
  void poll_resolve_(uint32_t now);
  // ::sockaddr_in, not struct sockaddr_in: spelled unqualified inside this
  // namespace the elaborated-type-specifier would declare a new incomplete
  // esphome::luna_stream::sockaddr_in and poison the name for the definitions.
  bool open_socket_(const ::sockaddr_in &addr);
  // lwIP dns_found_callback. Runs on the tcpip thread, so it only publishes an
  // answer into the dns_* atomics below — no locks, no logging, no sockets.
  static void on_dns_found_(const char *hostname, const ip_addr_t *addr, void *arg);
  bool poll_connect_();
  void on_connected_();
  void disconnect_(const char *reason);
  void schedule_reconnect_();
  void start_capture_();
  // Edge detection for the media_player state callback: only a transition into
  // idle emits `playback_done`, so it is tracked here rather than in the
  // callback body.
  void on_media_player_state_(bool idle);
  void flush_outbound_();
  void poll_downlink_();
  void handle_rx_(const uint8_t *data, size_t len);
  void handle_control_(const luna_stream_codec::Control &control);
  void maybe_send_rssi_(uint32_t now);
  void on_mic_data_(const std::vector<uint8_t> &data);
  bool enqueue_(const uint8_t *record, size_t len);

  std::string host_;
  uint16_t port_{10700};
  std::string source_id_;
  std::string room_;
  std::string token_;
  std::string fw_;
  bool has_speaker_{false};
  bool has_aec_{false};
  uint16_t frame_buffer_frames_{16};
  uint16_t rx_buffer_bytes_{512};

  microphone::MicrophoneSource *mic_source_{nullptr};
  media_player::MediaPlayer *media_player_{nullptr};
  Trigger<std::string> *led_trigger_{new Trigger<std::string>()};
  Trigger<std::string> *earcon_trigger_{new Trigger<std::string>()};
  Trigger<> *connected_trigger_{new Trigger<>()};

  int fd_{-1};
  ConnectionState state_{ConnectionState::Disconnected};
  uint32_t attempt_{0};
  uint32_t next_attempt_at_{0};
  uint32_t connect_started_at_{0};
  // FW-4: when Connected was published. The backoff resets only once
  // luna_stream_codec::reconnect_settled() accepts this, not on a bare accept.
  uint32_t connected_at_{0};
  uint32_t last_rssi_at_{0};
  uint32_t seq_{0};
  uint32_t dropped_frames_{0};
  uint32_t dropped_events_{0};
  uint32_t hello_pending_{0};
  bool media_player_idle_{true};

  // FW-1: asynchronous hostname resolution. loop() bumps dns_gen_ before each
  // dns_gethostbyname() request; the tcpip-thread callback publishes the answer
  // and mirrors that generation into dns_done_gen_ (release). loop() consumes
  // when the two match (acquire), so a result is read exactly once and only
  // for the current request. A late callback from an abandoned request can only
  // ever be an answer for this same, config-frozen host, so consuming it is
  // harmless. 0 in dns_addr_ means "no answer" (failure or IPv6-only).
  std::atomic<uint32_t> dns_gen_{0};
  std::atomic<uint32_t> dns_done_gen_{0};
  std::atomic<uint32_t> dns_addr_{0};
  uint32_t dns_started_at_{0};

  // pcm_/pcm_len_ belong to the microphone data callback alone — its own
  // FreeRTOS capture task, never this component's loop(). loop() must not
  // touch them: resetting pcm_len_ from the main loop while the callback was
  // mid-frame dropped up to 63 ms of audio with no sequence gap. A partial
  // frame simply carries across a reconnect, which the backend tolerates like
  // any short outage.
  uint8_t pcm_[luna_stream_codec::FRAME_PCM_BYTES]{0};
  size_t pcm_len_{0};
  luna_stream_codec::DownlinkDecoder decoder_;
  // Threading model (FW-2/FW-3): the microphone capture task runs
  // on_mic_data_() and the ESPHome main loop runs loop() and everything it
  // calls. They share state_, outbound_, seq_ and hello_pending_ through this
  // mutex — every write to state_ happens under it, and on_connected_()
  // publishes Connected in the same critical section that queues the hello, so
  // the capture task can never observe Connected before the hello is queued.
  std::mutex tx_mutex_;
  uint8_t *tx_storage_{nullptr};
  size_t tx_storage_len_{0};
  luna_stream_codec::OutboundBuffer outbound_;
  uint8_t rx_buf_[512]{0};
};

}  // namespace luna_stream
}  // namespace esphome

#endif  // !LUNA_STREAM_HOST_TEST
