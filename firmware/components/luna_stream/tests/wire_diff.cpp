// wire_diff — host-only differential harness for the luna_stream wire contract.
//
// This is the *firmware* side of a cross-implementation comparison, so the Rust
// backend (src/proto.rs + serde_json, normative per docs/protocol.md) can be fed
// exactly what this codec produces and vice versa. Nothing here reimplements the
// codec: every case calls the production `build_*` / `DownlinkDecoder` /
// `parse_control_json` paths from ../luna_stream.h.
//
// Build (see the header's own comment for the flags): compile this file with
// g++ -std=c++17 -Wall -Wextra -Werror -Wno-pragma-once-outside-header -O2
// plus -DLUNA_STREAM_HOST_TEST -DLUNA_STREAM_NO_TEST_MAIN.
//
// Modes (plain text, one case per line, bytes lowercase hex):
//   wire_diff emit   -> "UP <id> <hex>" for every message the firmware can build.
//                       "UP <id> REFUSED" when the production builder declines an
//                       over-cap input, so a refusal is observable and never a
//                       record that was silently truncated into a valid-but-wrong
//                       message.
//   wire_diff check  -> reads "DOWN <id> <hex>" lines from stdin, where <hex> is a
//                       COMPLETE 0x03 control record, and answers
//                       "DOWN <id> accept led <state-hex>"
//                       "DOWN <id> accept earcon <name-hex>"
//                       "DOWN <id> accept play <url-hex> <format-hex>"
//                       "DOWN <id> reject"
//                       Every echoed field is hex so the caller can compare values
//                       without delimiter ambiguity.
//   wire_diff check_frag -> the same decoder, but the record arrives in TCP-shaped
//                       pieces. Reads "DOWN <id> <hex>|<hex>|…" lines (one fragment per
//                       '|'; hex never contains one) and feeds them in order, reporting
//                       every `feed` call:
//                         "FRAG <id> <ordinal> need_more|pending|error"
//                         "CTL  <id> accept …" / "CTL <id> error <code>"
//                       so the caller asserts the *interleaving* — NeedMore until the
//                       record completes, exactly one control per completion, and the
//                       next record still decoding after a resumed partial — instead of
//                       only the final verdict. Bytes left over after a Pending are
//                       re-fed (they belong to the next record), which is what a socket
//                       caller must do too.
//   wire_diff backoff -> self-verifying table of the component-side policy that has no
//                       Rust counterpart to diff against: the reconnect ladder, FW-4's
//                       wrap-safe reconnect_settled(), and the exact 511/512-byte hello
//                       boundary that __init__.py's config validation is written against.
//                       Prints FAIL lines and exits 1 on any disagreement.
#include "../luna_stream.h"

#include <iostream>
#include <string>
#include <vector>

namespace {

using namespace luna_stream_codec;

std::string to_hex(const uint8_t *data, size_t len) {
  static const char HEX[] = "0123456789abcdef";
  std::string out;
  out.reserve(len * 2);
  for (size_t i = 0; i < len; i++) {
    out.push_back(HEX[(data[i] >> 4) & 0x0F]);
    out.push_back(HEX[data[i] & 0x0F]);
  }
  return out;
}

std::string str_hex(const char *s) {
  return to_hex(reinterpret_cast<const uint8_t *>(s), std::strlen(s));
}

int hex_digit(char c) {
  if (c >= '0' && c <= '9') {
    return c - '0';
  }
  if (c >= 'a' && c <= 'f') {
    return c - 'a' + 10;
  }
  if (c >= 'A' && c <= 'F') {
    return c - 'A' + 10;
  }
  return -1;
}

bool from_hex(const std::string &hex, std::vector<uint8_t> *out) {
  if (hex.size() % 2 != 0) {
    return false;
  }
  out->clear();
  out->reserve(hex.size() / 2);
  for (size_t i = 0; i < hex.size(); i += 2) {
    const int hi = hex_digit(hex[i]);
    const int lo = hex_digit(hex[i + 1]);
    if (hi < 0 || lo < 0) {
      return false;
    }
    out->push_back(static_cast<uint8_t>((hi << 4) | lo));
  }
  return true;
}

// ---------------------------------------------------------------------------
// emit: everything the firmware can put on the wire
// ---------------------------------------------------------------------------

int g_failures = 0;

void emit_bytes(const char *id, const uint8_t *data, size_t len, const char *what) {
  if (len == 0) {
    std::printf("FAIL %s: builder refused a message it must be able to build (%s)\n", id, what);
    g_failures++;
    return;
  }
  std::printf("UP %s %s\n", id, to_hex(data, len).c_str());
}

void emit_refused(const char *id, size_t len) {
  if (len != 0) {
    std::printf("FAIL %s: over-cap input was accepted (%zu bytes)\n", id, len);
    g_failures++;
    return;
  }
  std::printf("UP %s REFUSED\n", id);
}

size_t hello_case(char *out, size_t cap, const char *token, bool has_speaker, bool has_aec,
                  const char *source_id, const char *room, const char *fw) {
  HelloFields h;
  h.source_id = source_id;
  h.room = room;
  h.token = token;
  h.fw = fw;
  h.has_speaker = has_speaker;
  h.has_aec = has_aec;
  return build_hello(out, cap, h);
}

// Deterministic PCM: pcm[i] = (i * 7 + 3) & 0xFF.
void fill_pcm(uint8_t *pcm, size_t len) {
  for (size_t i = 0; i < len; i++) {
    pcm[i] = static_cast<uint8_t>((i * 7 + 3) & 0xFF);
  }
}

// Deterministic name of `len` lowercase letters: name[i] = 'a' + (i % 26).
std::string pattern_name(size_t len) {
  std::string name;
  name.reserve(len);
  for (size_t i = 0; i < len; i++) {
    name.push_back(static_cast<char>('a' + (i % 26)));
  }
  return name;
}

// 300 chars: the old fixed `char json[256]` silently refused this and the
// component reported it as "not connected or queue full".
constexpr size_t LONG_BUTTON_NAME = 300;
// 600 chars: still over the retuned event cap, so the refusal must be visible.
constexpr size_t OVERCAP_BUTTON_NAME = 600;

int run_emit() {
  static char hello[1024];
  static uint8_t record[8 * 1024];
  static uint8_t pcm[FRAME_PCM_BYTES];
  static uint8_t pcm_doc[FRAME_PCM_BYTES];
  const auto *hello_bytes = reinterpret_cast<const uint8_t *>(hello);
  fill_pcm(pcm, sizeof(pcm));
  for (size_t i = 0; i < sizeof(pcm_doc); i++) {
    pcm_doc[i] = static_cast<uint8_t>(i & 0xFF);
  }

  // hello — the four shapes the component builds (token present, token null,
  // a value needing escapes, caps on/off).
  emit_bytes("hello_token", hello_bytes,
             hello_case(hello, sizeof(hello), "s3cr3t", true, false, "kitchen", "kitchen", "0.1.0"),
             "hello with token (docs 9.4)");
  emit_bytes("hello_token_null", hello_bytes,
             hello_case(hello, sizeof(hello), nullptr, false, false, "study-pi", "study", "third-party/1.0"),
             "hello with token:null (docs 3.1)");
  emit_bytes("hello_token_escape", hello_bytes,
             hello_case(hello, sizeof(hello), "t\"k\\en\t\u00e9", true, true, "kitchen", "kitchen", "0.1.0"),
             "hello with an escaped token");
  emit_bytes("hello_token_control", hello_bytes,
             hello_case(hello, sizeof(hello), "a\x01z\x1f", true, false, "kitchen", "kitchen", "0.1.0"),
             "hello with control characters in the token");
  emit_bytes("hello_caps_on", hello_bytes,
             hello_case(hello, sizeof(hello), "s3cr3t", true, true, "kitchen", "kitchen", "0.1.0"),
             "hello caps true/true");
  emit_bytes("hello_caps_off", hello_bytes,
             hello_case(hello, sizeof(hello), "s3cr3t", false, false, "kitchen", "kitchen", "0.1.0"),
             "hello caps false/false");
  // The fourth capability combination: a satellite with a speaker but no echo cancellation
  // is a real product shape (a bare speaker board), and it is the one the docs' defaults
  // (false/false) and the two "interesting" combinations above all leave out.
  emit_bytes("hello_caps_ft", hello_bytes,
             hello_case(hello, sizeof(hello), "s3cr3t", false, true, "kitchen", "kitchen", "0.1.0"),
             "hello caps false/true");
  // Empty strings, on every field the contract lets be empty. The protocol document does not
  // forbid them, so the only honest test is whether the two implementations agree — an empty
  // value is also where a length check written as `<= 0` and one written as `< 0` differ.
  // Present-but-empty is a different thing from absent: `check_hello_auth` treats a set
  // server token and `""` as a mismatch, not as no token.
  emit_bytes("hello_token_empty", hello_bytes,
             hello_case(hello, sizeof(hello), "", true, false, "kitchen", "kitchen", "0.1.0"),
             "hello with token:\"\" (present but empty)");
  emit_bytes("hello_empty_fields", hello_bytes,
             hello_case(hello, sizeof(hello), nullptr, false, false, "", "", ""),
             "hello with empty source_id/room/fw");

  // audio: seq 0, the u32 ceiling, and the docs 9.1 pattern.
  emit_bytes("audio_seq0", record, build_audio_record(record, sizeof(record), 0, pcm, sizeof(pcm)),
             "audio seq 0");
  emit_bytes("audio_seq_max", record,
             build_audio_record(record, sizeof(record), 0xFFFFFFFFu, pcm, sizeof(pcm)), "audio seq u32 max");
  emit_bytes("audio_pattern_doc", record,
             build_audio_record(record, sizeof(record), 7, pcm_doc, sizeof(pcm_doc)),
             "audio seq 7 with pcm[i] = i");

  // events: all four variants, including a body that needs escaping and a name
  // that overflowed the old 256-byte body buffer.
  emit_bytes("ev_playback_done", record, build_event_playback_done(record, sizeof(record)),
             "playback_done");
  emit_bytes("ev_save_buffer", record, build_event_save_buffer(record, sizeof(record)), "save_buffer");
  emit_bytes("ev_button", record, build_event_button(record, sizeof(record), "night_mode"), "button");
  emit_bytes("ev_button_escape", record,
             build_event_button(record, sizeof(record), "mo\"de\\x\ty\x01"), "button needing escapes");
  // An empty button name: the contract does not forbid one, so the only question is whether
  // the two encoders agree on it. A writer that special-cases "no name" here would produce a
  // record the backend parses to a different event, and nothing else would notice.
  emit_bytes("ev_button_empty", record, build_event_button(record, sizeof(record), ""),
             "button with an empty name");
  const std::string long_name = pattern_name(LONG_BUTTON_NAME);
  emit_bytes("ev_button_long", record, build_event_button(record, sizeof(record), long_name.c_str()),
             "300-char button name");
  const std::string overcap_name = pattern_name(OVERCAP_BUTTON_NAME);
  emit_refused("ev_button_overcap",
               build_event_button(record, sizeof(record), overcap_name.c_str()));

  // rssi: fixed-point text over several tenths, including 0 and a negative zero-ish value.
  // The .0/.5 values above cannot tell a fixed-point emitter from a float formatter that
  // happens to print one digit; these can. -62.3 and -62.9 exercise a magnitude whose
  // integer part is not a round number, and ±0.1 are the two rails either side of zero,
  // where sign handling and `value / 10` integer division are most likely to disagree with
  // serde's float text. Any mismatch here is a satellite reporting a signal level the
  // backend logs as something else.
  emit_bytes("ev_rssi_neg623", record, build_event_rssi(record, sizeof(record), -623), "rssi -62.3");
  emit_bytes("ev_rssi_neg629", record, build_event_rssi(record, sizeof(record), -629), "rssi -62.9");
  emit_bytes("ev_rssi_neg1", record, build_event_rssi(record, sizeof(record), -1), "rssi -0.1");
  emit_bytes("ev_rssi_pos1", record, build_event_rssi(record, sizeof(record), 1), "rssi 0.1");
  emit_bytes("ev_rssi_0", record, build_event_rssi(record, sizeof(record), 0), "rssi 0.0");
  emit_bytes("ev_rssi_neg5", record, build_event_rssi(record, sizeof(record), -5), "rssi -0.5");
  emit_bytes("ev_rssi_neg625", record, build_event_rssi(record, sizeof(record), -625), "rssi -62.5");
  emit_bytes("ev_rssi_neg1000", record, build_event_rssi(record, sizeof(record), -1000), "rssi -100.0");
  emit_bytes("ev_rssi_pos25", record, build_event_rssi(record, sizeof(record), 25), "rssi 2.5");

  return g_failures == 0 ? 0 : 1;
}

// ---------------------------------------------------------------------------
// check: feed complete control records through the real decoder
// ---------------------------------------------------------------------------

// One verdict string, used by both `check` (whole record) and `check_frag` (fragmented
// record), so the two modes cannot drift apart in how they report what they decoded.
std::string format_verdict(const Control &c) {
  switch (c.type) {
    case ControlType::Led:
      return "accept led " + str_hex(led_state_name(c.led));
    case ControlType::Earcon:
      return "accept earcon " + str_hex(c.name);
    case ControlType::Play:
      return "accept play " + str_hex(c.url) + " " + str_hex(c.format);
    case ControlType::Unknown:
    default:
      return "reject";
  }
}

void print_accept(const std::string &id, const Control &c) {
  std::printf("DOWN %s %s\n", id.c_str(), format_verdict(c).c_str());
}

int run_check() {
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty()) {
      continue;
    }
    // "DOWN <id> <hex>"
    if (line.compare(0, 5, "DOWN ") != 0) {
      std::printf("ERR expected 'DOWN <id> <hex>', got: %s\n", line.c_str());
      return 2;
    }
    const size_t space = line.find(' ', 5);
    if (space == std::string::npos) {
      std::printf("ERR expected 'DOWN <id> <hex>', got: %s\n", line.c_str());
      return 2;
    }
    const std::string id = line.substr(5, space - 5);
    const std::string hex = line.substr(space + 1);
    std::vector<uint8_t> rec;
    if (!from_hex(hex, &rec)) {
      // Malformed input is the caller's bug, not a firmware verdict: say so, so a hex
      // encoding error on the Rust side can never masquerade as "the firmware rejected it".
      std::printf("ERR %s bad hex\n", id.c_str());
      continue;
    }
    if (rec.empty()) {
      std::printf("DOWN %s reject\n", id.c_str());  // nothing to decode is not an accept
      continue;
    }
    DownlinkDecoder decoder;
    size_t consumed = 0;
    const DownlinkDecoder::Status status = decoder.feed(rec.data(), rec.size(), &consumed);
    Control control;
    // A control is accepted when the decoder produced one. Bytes after a complete record
    // are the *next* record's, not this one's, so they do not change this verdict — the
    // backend's parse_control delimits by `len` and ignores them too. A latched error or a
    // truncated record (NeedMore) is a reject.
    if (status == DownlinkDecoder::Status::Pending && decoder.take(&control)) {
      print_accept(id, control);
      continue;
    }
    std::printf("DOWN %s reject\n", id.c_str());
  }
  return 0;
}

// ---------------------------------------------------------------------------
// check_frag: the same decoder, fed the way a socket feeds it
// ---------------------------------------------------------------------------
// Splitting a record across `feed` calls is the normal case on a real TCP connection, and
// it is a different code path from the whole-record one above: the decoder has to hold a
// half-read length prefix and a half-filled payload between calls. `check` cannot see a bug
// there, so this mode reports every `feed` outcome, not just the final verdict.

const char *status_name(DownlinkDecoder::Status status) {
  switch (status) {
    case DownlinkDecoder::Status::NeedMore:
      return "need_more";
    case DownlinkDecoder::Status::Pending:
      return "pending";
    case DownlinkDecoder::Status::Error:
      return "error";
  }
  return "unknown";
}

// "a|b|c" -> ["a","b","c"]. Hex never contains '|', so it is an unambiguous separator.
std::vector<std::string> split_fragments(const std::string &spec) {
  std::vector<std::string> out;
  size_t start = 0;
  while (true) {
    const size_t bar = spec.find('|', start);
    if (bar == std::string::npos) {
      out.push_back(spec.substr(start));
      return out;
    }
    out.push_back(spec.substr(start, bar - start));
    start = bar + 1;
  }
}

int run_check_frag() {
  std::string line;
  while (std::getline(std::cin, line)) {
    if (line.empty()) {
      continue;
    }
    if (line.compare(0, 5, "DOWN ") != 0) {
      std::printf("ERR expected 'DOWN <id> <hex>[|<hex>…]', got: %s\n", line.c_str());
      return 2;
    }
    const size_t space = line.find(' ', 5);
    if (space == std::string::npos) {
      std::printf("ERR expected 'DOWN <id> <hex>[|<hex>…]', got: %s\n", line.c_str());
      return 2;
    }
    const std::string id = line.substr(5, space - 5);
    std::vector<std::vector<uint8_t>> fragments;
    bool malformed = false;
    for (const std::string &segment : split_fragments(line.substr(space + 1))) {
      std::vector<uint8_t> bytes;
      // An empty fragment is a caller bug (it would be a zero-length feed that proves
      // nothing), and bad hex is a Rust-side encoding bug — never a firmware verdict.
      if (segment.empty() || !from_hex(segment, &bytes)) {
        malformed = true;
        break;
      }
      fragments.push_back(bytes);
    }
    if (malformed) {
      std::printf("ERR %s bad hex fragment\n", id.c_str());
      continue;
    }

    DownlinkDecoder decoder;
    size_t ordinal = 0;
    bool stop = false;
    for (size_t f = 0; f < fragments.size() && !stop; f++) {
      size_t off = 0;
      // After a Pending the rest of *this* fragment belongs to the next record, so keep
      // feeding from `consumed` rather than dropping it: a caller that discarded those
      // bytes would never see the record that follows.
      while (off < fragments[f].size()) {
        size_t consumed = 0;
        const DownlinkDecoder::Status status =
            decoder.feed(fragments[f].data() + off, fragments[f].size() - off, &consumed);
        std::printf("FRAG %s %zu %s\n", id.c_str(), ordinal++, status_name(status));
        if (status == DownlinkDecoder::Status::Pending) {
          Control control;
          if (decoder.take(&control)) {
            std::printf("CTL %s %s\n", id.c_str(), format_verdict(control).c_str());
          }
          off += consumed;
          continue;
        }
        if (status == DownlinkDecoder::Status::Error) {
          // The error code names the reason: a payload past MAX_CONTROL_JSON is
          // PAYLOAD_TOO_LONG (2), not BAD_JSON (3), and only the caller can tell.
          std::printf("CTL %s error %u\n", id.c_str(), static_cast<unsigned>(decoder.error_code()));
          stop = true;
          break;
        }
        if (consumed == 0) {
          // NeedMore that consumed nothing would spin here forever; that is a decoder bug
          // and it must be reported, not looped on.
          std::printf("ERR %s feed made no progress\n", id.c_str());
          stop = true;
          break;
        }
        off += consumed;
      }
    }
  }
  return 0;
}

// ---------------------------------------------------------------------------
// backoff: reconnect policy + the firmware's hello-buffer boundary, as a
// self-verifying table. These are component-side decisions (FW-4's settled
// backoff, FW-5's 512-byte hello buffer that __init__.py validates against),
// not wire bytes, so they have no Rust counterpart to diff against — instead
// this mode asserts them itself and exits nonzero on any disagreement.
// ---------------------------------------------------------------------------
int run_backoff() {
  int failures = 0;
  auto check = [&failures](bool ok, const char *what) {
    if (!ok) {
      std::printf("FAIL %s\n", what);
      failures++;
    }
  };

  // The documented ladder: 1 s doubling, capped at 30 s.
  check(reconnect_delay_ms(0) == 1000, "attempt 0 -> 1 s");
  check(reconnect_delay_ms(4) == 16000, "attempt 4 -> 16 s");
  check(reconnect_delay_ms(5) == 30000, "attempt 5 -> 30 s cap");
  check(reconnect_delay_ms(1000) == 30000, "huge attempt -> 30 s cap");

  // FW-4: a connection settles — and may reset the backoff — only after
  // RECONNECT_SETTLE_MS, judged wrap-safely on the 32-bit millis() clock.
  check(!reconnect_settled(RECONNECT_SETTLE_MS - 1, 0), "not settled one ms early");
  check(reconnect_settled(RECONNECT_SETTLE_MS, 0), "settled exactly at the threshold");
  check(reconnect_settled(RECONNECT_SETTLE_MS + 1000, 0), "settled after the threshold");
  // A connection established just before the 49.7-day counter rollover must
  // settle at the same wall-clock distance, not wait another 49.7 days.
  const uint32_t near_wrap = 0xFFFFFF00u;
  check(!reconnect_settled(near_wrap + (RECONNECT_SETTLE_MS - 1), near_wrap),
        "not settled at 4999 ms across the rollover");
  check(reconnect_settled(near_wrap + RECONNECT_SETTLE_MS, near_wrap),
        "settled at 5000 ms across the rollover");

  // FW-5 boundary: the component builds the hello into char[512], and
  // JsonWriter needs one spare byte for the NUL — 511 bytes is the longest
  // line that fits. __init__.py's config validation refuses exactly what is
  // longer than this, so if this boundary ever moves, both sides move.
  {
    char buf[512];
    HelloFields h;
    h.source_id = "kitchen";
    h.room = "kitchen";
    h.token = "";  // the component's empty-token spelling is null; irrelevant to the delta
    h.fw = "0.1.0";
    h.has_speaker = false;
    h.has_aec = false;
    const size_t base = build_hello(buf, sizeof(buf), h);
    check(base > 0 && base <= 511, "hello base builds and leaves room for the boundary probe");
    const std::string fits_token(511 - base, 'a');   // plain ASCII: 1 wire byte each
    h.token = fits_token.c_str();
    check(build_hello(buf, sizeof(buf), h) == 511, "511-byte hello fits the 512-byte buffer");
    const std::string over_token(512 - base, 'a');
    h.token = over_token.c_str();
    check(build_hello(buf, sizeof(buf), h) == 0, "512-byte hello is refused, never truncated");
  }

  if (failures != 0) {
    std::printf("%d FAILURE(S)\n", failures);
    return 1;
  }
  std::printf("PASS backoff + hello-boundary\n");
  return 0;
}

}  // namespace

int main(int argc, char **argv) {
  if (argc != 2) {
    std::fprintf(stderr, "usage: %s emit|check|check_frag\n", argv[0]);
    return 2;
  }
  const std::string mode = argv[1];
  if (mode == "emit") {
    return run_emit();
  }
  if (mode == "check") {
    return run_check();
  }
  if (mode == "check_frag") {
    return run_check_frag();
  }
  if (mode == "backoff") {
    return run_backoff();
  }
  std::fprintf(stderr, "unknown mode '%s' (expected emit, check, check_frag or backoff)\n", mode.c_str());
  return 2;
}
