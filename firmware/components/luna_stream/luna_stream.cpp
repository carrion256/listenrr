// luna_stream ESPHome component: continuous microphone capture streamed to the
// listenrr backend over the frozen TCP protocol, plus the downlink control
// channel that drives the YAML `on_led` / `on_earcon` triggers and the
// `on_connected` trigger for an established connection.
//
// All byte-level protocol decisions live in luna_stream_codec (luna_stream.h),
// which is host-tested with g++ — see the LUNA_STREAM_HOST_TEST block there.
// This file is transport and integration only: sockets, the microphone source,
// the media_player, and the ESPHome component lifecycle.

#include "luna_stream.h"

#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <new>
#include <utility>

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/select.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <unistd.h>

// Explicit, not transitive: the POSIX headers above are only shims for
// lwip/sockets.h on this port, and nothing here may depend on which of them
// happened to pull in which definition.
#include "lwip/dns.h"      // dns_gethostbyname
#include "lwip/err.h"      // err_t, ERR_OK, ERR_INPROGRESS
#include "lwip/ip_addr.h"  // ip_addr_t, IP_IS_V4, ip_2_ip4
#include "lwip/sockets.h"  // sockaddr, sockaddr_in, AF_INET, inet_pton

namespace esphome {
namespace luna_stream {

static const char *const TAG = "luna_stream";

namespace {

constexpr uint32_t CONNECT_TIMEOUT_MS = 5000;
// Safety net for the asynchronous resolve (FW-1). lwIP retries the query and
// calls the callback with a NULL answer on failure, but a lost callback must
// not wedge the component in Resolving, so loop() abandons a query older than
// this and backs off like any other failed attempt.
constexpr uint32_t DNS_GIVEUP_MS = 30000;
constexpr uint32_t RSSI_INTERVAL_MS = 30000;
// Bound the bytes handled per loop() call so a chatty backend cannot starve
// the ESPHome main loop.
constexpr int RX_CALLS_PER_LOOP = 8;
constexpr size_t MAX_TX_QUEUE_FRAMES = 128;

#ifdef USE_WIFI
// RSSI in tenths of dBm, matching the protocol's `{"type":"rssi","dbm":-62.5}`.
bool read_wifi_rssi_tenths(int *out) {
  if (wifi::global_wifi_component == nullptr) {
    return false;
  }
  const float dbm = wifi::global_wifi_component->wifi_rssi();
  const float scaled = dbm * 10.0f;
  *out = static_cast<int>(scaled < 0 ? scaled - 0.5f : scaled + 0.5f);
  return true;
}
#endif

}  // namespace

// ---------------------------------------------------------------------------
// lifecycle
// ---------------------------------------------------------------------------

void LunaStream::setup() {
  if (this->host_.empty()) {
    ESP_LOGE(TAG, "backend_host is not configured");
    this->mark_failed();
    return;
  }
  if (this->mic_source_ == nullptr) {
    ESP_LOGE(TAG, "no microphone configured");
    this->mark_failed();
    return;
  }
  if (this->source_id_.empty() || this->room_.empty()) {
    ESP_LOGE(TAG, "source_id and room are required");
    this->mark_failed();
    return;
  }
  if (this->token_.size() < 16) {
    // Not fatal — the backend may run with server.auth_token unset — but a
    // short literal is almost always a hand-typed token that belongs in a
    // secrets.yaml as `luna_token`.
    ESP_LOGW(TAG, "token looks like a literal (len %u); prefer token: !secret luna_token",
             static_cast<unsigned>(this->token_.size()));
  }

  const uint16_t frames = std::min<uint16_t>(this->frame_buffer_frames_, MAX_TX_QUEUE_FRAMES);
  this->tx_storage_len_ = static_cast<size_t>(frames) * luna_stream_codec::AUDIO_RECORD_LEN + 512;
  this->tx_storage_ = new (std::nothrow) uint8_t[this->tx_storage_len_];
  if (this->tx_storage_ == nullptr) {
    ESP_LOGE(TAG, "out of memory for a %u-frame transmit queue", static_cast<unsigned>(frames));
    this->mark_failed();
    return;
  }
  this->outbound_.init(this->tx_storage_, this->tx_storage_len_);

  this->start_capture_();

  if (this->media_player_ != nullptr) {
    // media_player.h: add_on_state_callback takes one argument, the new
    // MediaPlayerState. Only a transition into idle means playback finished.
    this->media_player_->add_on_state_callback([this](media_player::MediaPlayerState state) {
      this->on_media_player_state_(state == media_player::MEDIA_PLAYER_STATE_IDLE);
    });
  }

  this->next_attempt_at_ = millis();
  this->last_rssi_at_ = millis();
}

void LunaStream::loop() {
  const uint32_t now = millis();
  switch (this->state_) {
    case ConnectionState::Disconnected:
      if (static_cast<int32_t>(now - this->next_attempt_at_) >= 0) {
        this->start_connect_();
      }
      break;
    case ConnectionState::Resolving:
      // FW-1: a hostname query runs on the lwIP tcpip thread; this only checks
      // whether its answer has been published (or the attempt has been given
      // up). Nothing here may block.
      this->poll_resolve_(now);
      break;
    case ConnectionState::Connecting:
      if (!this->poll_connect_() && this->state_ == ConnectionState::Connecting &&
          now - this->connect_started_at_ >= CONNECT_TIMEOUT_MS) {
        // poll_connect_() may already have failed the socket; only time out a
        // handshake that is genuinely still in progress, or the backoff
        // counter would advance twice.
        this->disconnect_("connect timed out");
      }
      break;
    case ConnectionState::Connected:
      // FW-4: the backoff is no longer reset by a TCP accept. A backend that
      // accepts and then closes (bad token, unknown source_id, oversized hello)
      // would otherwise be hammered at ~1 Hz forever; only a connection that
      // has survived RECONNECT_SETTLE_MS proves the link healthy and restores
      // fast recovery for its eventual drop.
      if (this->attempt_ != 0 && luna_stream_codec::reconnect_settled(now, this->connected_at_)) {
        this->attempt_ = 0;
      }
      this->flush_outbound_();
      if (this->state_ == ConnectionState::Connected) {
        this->poll_downlink_();
      }
      if (this->state_ == ConnectionState::Connected) {
        this->maybe_send_rssi_(now);
      }
      break;
  }
}

void LunaStream::dump_config() {
  ESP_LOGCONFIG(TAG, "luna_stream:");
  ESP_LOGCONFIG(TAG, "  backend: %s:%u", this->host_.c_str(), this->port_);
  ESP_LOGCONFIG(TAG, "  source_id: %s  room: %s  fw: %s", this->source_id_.c_str(), this->room_.c_str(),
                this->fw_.c_str());
  ESP_LOGCONFIG(TAG, "  caps: has_speaker=%d has_aec=%d", this->has_speaker_, this->has_aec_);
  ESP_LOGCONFIG(TAG, "  frame: %u samples @ %u Hz (record %u bytes)",
                static_cast<unsigned>(luna_stream_codec::FRAME_SAMPLES),
                static_cast<unsigned>(luna_stream_codec::SAMPLE_RATE_HZ),
                static_cast<unsigned>(luna_stream_codec::AUDIO_RECORD_LEN));
  ESP_LOGCONFIG(TAG, "  tx queue: %zu bytes (%u frames requested)", this->tx_storage_len_,
                static_cast<unsigned>(this->frame_buffer_frames_));
  ESP_LOGCONFIG(TAG, "  rx chunk: %u bytes", static_cast<unsigned>(this->rx_buffer_bytes_));
  ESP_LOGCONFIG(TAG, "  capture: MicrophoneSource, continuous (shared with other consumers)");
  ESP_LOGCONFIG(TAG, "  media_player: %s", this->media_player_ != nullptr ? "configured" : "none");
}

bool LunaStream::connected() const { return this->state_ == ConnectionState::Connected && this->fd_ >= 0; }

// ---------------------------------------------------------------------------
// capture: MicrophoneSource -> 1024-sample frames
// ---------------------------------------------------------------------------

void LunaStream::start_capture_() {
  // Codegen owns the MicrophoneSource (built by
  // microphone.microphone_source_to_code with 16-bit mono, one channel); this
  // component only attaches its callback and starts capture, which then runs
  // continuously — that is what a satellite does, and the backend runs the VAD.
  const audio::AudioStreamInfo info = this->mic_source_->get_audio_stream_info();
  if (info.get_bits_per_sample() != 16 || info.get_channels() != 1 ||
      info.get_sample_rate() != static_cast<uint32_t>(luna_stream_codec::SAMPLE_RATE_HZ)) {
    // __init__.py rejects this during config validation; getting here means the
    // microphone changed behind our back, and every record would be wrong.
    ESP_LOGE(TAG, "microphone supplies %u-bit x%u @ %u Hz; the contract needs 16-bit mono @ %u Hz",
             static_cast<unsigned>(info.get_bits_per_sample()), static_cast<unsigned>(info.get_channels()),
             static_cast<unsigned>(info.get_sample_rate()),
             static_cast<unsigned>(luna_stream_codec::SAMPLE_RATE_HZ));
    this->mark_failed();
    return;
  }
  this->mic_source_->add_data_callback([this](const std::vector<uint8_t> &data) { this->on_mic_data_(data); });
  this->mic_source_->start();
}

void LunaStream::on_mic_data_(const std::vector<uint8_t> &data) {
  // The callback delivers whatever chunk the I2S driver produced; the contract
  // wants exactly 1024 samples, so accumulate across callbacks.
  for (uint8_t byte : data) {
    this->pcm_[this->pcm_len_++] = byte;
    if (this->pcm_len_ < luna_stream_codec::FRAME_PCM_BYTES) {
      continue;
    }
    this->pcm_len_ = 0;

    std::lock_guard<std::mutex> guard(this->tx_mutex_);
    if (this->state_ != ConnectionState::Connected || this->hello_pending_ != 0) {
      // Not connected, or the hello line has not fully left the buffer yet:
      // audio before hello would make the backend close the connection.
      this->dropped_frames_++;
      continue;
    }
    uint8_t record[luna_stream_codec::AUDIO_RECORD_LEN];
    const size_t len = luna_stream_codec::build_audio_record(record, sizeof(record), this->seq_, this->pcm_,
                                                            sizeof(this->pcm_));
    if (len == 0 || !this->outbound_.push(record, len)) {
      // Drop the frame and leave seq_ alone: a skipped seq would read as a
      // network gap (AC2 counts them) when in fact the audio never existed.
      this->dropped_frames_++;
      continue;
    }
    this->seq_++;
  }
}

// ---------------------------------------------------------------------------
// outbound: whole records, resumable partial writes
// ---------------------------------------------------------------------------

bool LunaStream::enqueue_(const uint8_t *record, size_t len) {
  std::lock_guard<std::mutex> guard(this->tx_mutex_);
  if (this->state_ != ConnectionState::Connected) {
    return false;
  }
  return this->outbound_.push(record, len);
}

void LunaStream::flush_outbound_() {
  const char *failure = nullptr;
  {
    std::lock_guard<std::mutex> guard(this->tx_mutex_);
    while (this->outbound_.pending() > 0) {
      const ssize_t sent = ::send(this->fd_, this->outbound_.head(), this->outbound_.pending(), MSG_DONTWAIT);
      if (sent > 0) {
        // Remove exactly what the socket accepted. A short write leaves the rest
        // queued and the next loop() resumes at that byte: never dropped, never
        // resent.
        this->hello_pending_ = static_cast<uint32_t>(sent) >= this->hello_pending_
                                  ? 0
                                  : this->hello_pending_ - static_cast<uint32_t>(sent);
        this->outbound_.consume(static_cast<size_t>(sent));
        continue;
      }
      if (sent == 0) {
        break;
      }
      if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
        break;  // socket full; keep the bytes and try again next loop
      }
      failure = "send failed";
      break;
    }
  }
  // disconnect_() takes the same mutex, so it runs after the lock is released.
  if (failure != nullptr) {
    this->disconnect_(failure);
  }
}

// ---------------------------------------------------------------------------
// connection
// ---------------------------------------------------------------------------

bool LunaStream::start_connect_() {
  ::sockaddr_in addr;
  std::memset(&addr, 0, sizeof(addr));
  addr.sin_family = AF_INET;
  addr.sin_port = htons(this->port_);

  // FW-1: this used to call getaddrinfo() on the ESPHome main loop, where a
  // slow or dead DNS server froze every component until lwIP's query timed
  // out. A numeric IPv4 backend (the shipped example config) needs no DNS at
  // all; a hostname goes through lwIP's asynchronous dns_gethostbyname(), and
  // loop() completes the connect from the published answer.
  if (::inet_pton(AF_INET, this->host_.c_str(), &addr.sin_addr) != 1) {
    this->dns_gen_.fetch_add(1, std::memory_order_relaxed);
    ip_addr_t resolved;
    const err_t rc = dns_gethostbyname(this->host_.c_str(), &resolved, LunaStream::on_dns_found_, this);
    if (rc == ERR_INPROGRESS) {
      this->dns_started_at_ = millis();
      {
        std::lock_guard<std::mutex> guard(this->tx_mutex_);
        this->state_ = ConnectionState::Resolving;
      }
      return true;  // poll_resolve_() finishes the job from loop()
    }
    if (rc != ERR_OK || !IP_IS_V4(&resolved)) {
      // Also covers an ERR_OK whose answer is not IPv4 (e.g. an IPv6 literal
      // or an AAAA-only host): this socket has no address to try.
      ESP_LOGW(TAG, "resolving %s failed (lwip err %d)", this->host_.c_str(), static_cast<int>(rc));
      this->schedule_reconnect_();
      return false;
    }
    addr.sin_addr.s_addr = ip_2_ip4(&resolved)->addr;
  }
  return this->open_socket_(addr);
}

void LunaStream::poll_resolve_(uint32_t now) {
  if (this->dns_done_gen_.load(std::memory_order_acquire) == this->dns_gen_.load(std::memory_order_relaxed)) {
    ::sockaddr_in addr;
    std::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(this->port_);
    addr.sin_addr.s_addr = this->dns_addr_.load(std::memory_order_relaxed);
    if (addr.sin_addr.s_addr == 0) {
      this->disconnect_("resolve failed");
      return;
    }
    this->open_socket_(addr);
    return;
  }
  if (static_cast<int32_t>(now - this->dns_started_at_) >= static_cast<int32_t>(DNS_GIVEUP_MS)) {
    this->disconnect_("resolve timed out");
  }
}

// lwIP dns_found_callback — tcpip thread context (FW-1). Publish the answer and
// nothing else: no logging, no sockets, no locks here. The address is stored
// before the generation is mirrored out (release), so the acquire-load pair in
// poll_resolve_() only ever sees a complete answer.
void LunaStream::on_dns_found_(const char *hostname, const ip_addr_t *addr, void *arg) {
  (void) hostname;  // the name is always this->host_; there is nothing to do with it here
  auto *self = static_cast<LunaStream *>(arg);
  uint32_t word = 0;
  if (addr != nullptr && IP_IS_V4(addr)) {
    word = ip_2_ip4(addr)->addr;
  }
  self->dns_addr_.store(word, std::memory_order_relaxed);
  self->dns_done_gen_.store(self->dns_gen_.load(std::memory_order_relaxed), std::memory_order_release);
}

bool LunaStream::open_socket_(const ::sockaddr_in &addr) {
  const int fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd < 0) {
    ESP_LOGW(TAG, "socket() failed: %s", strerror(errno));
    this->schedule_reconnect_();
    return false;
  }
  const int flags = ::fcntl(fd, F_GETFL, 0);
  if (flags < 0 || ::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) {
    ESP_LOGW(TAG, "fcntl(O_NONBLOCK) failed: %s", strerror(errno));
    ::close(fd);
    this->schedule_reconnect_();
    return false;
  }
#if defined(TCP_NODELAY)
  // 64 ms frames: never let Nagle hold a frame back.
  int one = 1;
  ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
#endif

  this->fd_ = fd;
  {
    std::lock_guard<std::mutex> guard(this->tx_mutex_);
    this->state_ = ConnectionState::Connecting;
  }
  this->connect_started_at_ = millis();

  const int r = ::connect(fd, reinterpret_cast<const ::sockaddr *>(&addr), sizeof(addr));
  if (r == 0) {
    this->on_connected_();
    return true;
  }
  if (errno != EINPROGRESS && errno != EAGAIN) {
    ESP_LOGW(TAG, "connect to %s:%u failed: %s", this->host_.c_str(), this->port_, strerror(errno));
    this->disconnect_("connect failed");
    return false;
  }
  return true;  // in progress; poll_connect_() finishes it
}

bool LunaStream::poll_connect_() {
  fd_set write_fds;
  FD_ZERO(&write_fds);
  FD_SET(this->fd_, &write_fds);
  struct timeval timeout;
  timeout.tv_sec = 0;
  timeout.tv_usec = 0;
  const int ready = ::select(this->fd_ + 1, nullptr, &write_fds, nullptr, &timeout);
  if (ready < 0) {
    if (errno == EINTR) {
      return false;
    }
    this->disconnect_("select failed");
    return false;
  }
  if (ready == 0) {
    return false;  // still handshaking
  }
  int err = 0;
  socklen_t err_len = sizeof(err);
  if (::getsockopt(this->fd_, SOL_SOCKET, SO_ERROR, &err, &err_len) < 0 || err != 0) {
    this->disconnect_(err != 0 ? "connect refused" : "getsockopt failed");
    return false;
  }
  this->on_connected_();
  return true;
}

void LunaStream::on_connected_() {
  // Build the hello first: it reads no shared state, so it stays outside the
  // lock.
  char hello[512];
  // The backend rejects an unterminated hello line longer than MAX_HELLO_LINE
  // (src/proto.rs); a fixed 512-byte buffer can never reach that, and this
  // keeps the two numbers from drifting apart.
  static_assert(sizeof(hello) <= luna_stream_codec::MAX_HELLO_LINE, "hello buffer exceeds the protocol limit");
  luna_stream_codec::HelloFields fields;
  fields.source_id = this->source_id_.c_str();
  fields.room = this->room_.c_str();
  fields.token = this->token_.empty() ? nullptr : this->token_.c_str();
  fields.fw = this->fw_.c_str();
  fields.has_speaker = this->has_speaker_;
  fields.has_aec = this->has_aec_;
  const size_t hello_len = luna_stream_codec::build_hello(hello, sizeof(hello), fields);
  if (hello_len == 0) {
    ESP_LOGE(TAG, "hello does not fit its buffer");
    this->disconnect_("hello too large");
    return;
  }
  this->decoder_.reset();

  bool too_big = false;
  {
    // FW-2: the whole Connected transition — reset seq, drop stale bytes, queue
    // the hello, then publish Connected — happens in ONE critical section.
    // Publishing Connected before the hello was queued (the old split order)
    // left a window where the microphone capture task, which gates audio on
    // state_/hello_pending_ under this same mutex, could put a 0x01 record on
    // the wire ahead of the hello and make the backend close the connection.
    std::lock_guard<std::mutex> guard(this->tx_mutex_);
    this->seq_ = 0;  // sequence is per-connection (seq * 1024 indexes the source ring)
    this->outbound_.clear();
    if (!this->outbound_.push(reinterpret_cast<const uint8_t *>(hello), hello_len)) {
      too_big = true;
    } else {
      this->hello_pending_ = static_cast<uint32_t>(hello_len);
      this->state_ = ConnectionState::Connected;
    }
  }
  if (too_big) {
    // Outside the lock: disconnect_() locks the transmit queue too.
    ESP_LOGE(TAG, "hello does not fit the transmit queue");
    this->disconnect_("hello does not fit the queue");
    return;
  }
  // FW-4: attempt_ is deliberately NOT reset here — an accept proves nothing.
  // loop() resets it once reconnect_settled() accepts connected_at_.
  this->connected_at_ = millis();
  this->last_rssi_at_ = millis();
  ESP_LOGI(TAG, "connected to %s:%u as '%s' (room '%s', fw '%s')", this->host_.c_str(), this->port_,
           this->source_id_.c_str(), this->room_.c_str(), this->fw_.c_str());
  // `on_connected:` (AC2's post-reconnect report is its intended consumer).
  // Last, after the log, so the pair reads in order; and outside the lock
  // above, because the handler is arbitrary YAML. Safe thread-wise by
  // construction: on_connected_() has exactly two callers, both inside
  // poll_connect_(), and poll_connect_() is called only from loop() — this is
  // the ESPHome main loop, never the microphone capture task.
  this->connected_trigger_->trigger();
}

void LunaStream::schedule_reconnect_() {
  const uint32_t delay = luna_stream_codec::reconnect_delay_ms(this->attempt_);
  this->attempt_++;
  this->next_attempt_at_ = millis() + delay;
  ESP_LOGI(TAG, "reconnecting in %u ms (attempt %u)", static_cast<unsigned>(delay),
           static_cast<unsigned>(this->attempt_));
}

void LunaStream::disconnect_(const char *reason) {
  const bool was_connected = this->state_ == ConnectionState::Connected;
  if (this->fd_ >= 0) {
    ::close(this->fd_);
    this->fd_ = -1;
  }
  {
    // FW-2/FW-3: state_ goes to Disconnected under the same mutex the capture
    // task reads it with, together with the queue clear — writing it after the
    // unlock let the callback push one frame into the freshly cleared queue.
    // pcm_/pcm_len_ are deliberately NOT reset: they belong to the capture
    // task alone (see the member comment); a partial frame carried across a
    // reconnect is harmless, while racing the reset was not.
    std::lock_guard<std::mutex> guard(this->tx_mutex_);
    this->outbound_.clear();
    this->hello_pending_ = 0;
    this->state_ = ConnectionState::Disconnected;
  }
  this->decoder_.reset();
  if (was_connected) {
    ESP_LOGW(TAG, "disconnected: %s (frames dropped %u, events dropped %u)", reason,
             static_cast<unsigned>(this->dropped_frames_), static_cast<unsigned>(this->dropped_events_));
  } else {
    ESP_LOGD(TAG, "connect attempt failed: %s", reason);
  }
  this->schedule_reconnect_();
}

// ---------------------------------------------------------------------------
// inbound control
// ---------------------------------------------------------------------------

void LunaStream::poll_downlink_() {
  const size_t chunk = std::min<size_t>(this->rx_buffer_bytes_, sizeof(this->rx_buf_));
  for (int i = 0; i < RX_CALLS_PER_LOOP; i++) {
    const ssize_t got = ::recv(this->fd_, this->rx_buf_, chunk, MSG_DONTWAIT);
    if (got > 0) {
      this->handle_rx_(this->rx_buf_, static_cast<size_t>(got));
      if (this->state_ != ConnectionState::Connected) {
        return;
      }
      continue;
    }
    if (got == 0) {
      this->disconnect_("backend closed the connection");
      return;
    }
    if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
      return;  // nothing more to read right now
    }
    this->disconnect_("recv failed");
    return;
  }
}

void LunaStream::handle_rx_(const uint8_t *data, size_t len) {
  size_t offset = 0;
  while (offset < len) {
    size_t consumed = 0;
    const auto status = this->decoder_.feed(data + offset, len - offset, &consumed);
    offset += consumed;
    if (status == luna_stream_codec::DownlinkDecoder::Status::Error) {
      // A control the firmware cannot represent is refused here, never truncated into a
      // different-but-valid control, so a dropped play/earcon is always visible in the log.
      ESP_LOGW(TAG,
               "control dropped (decoder error %u): bad framing/JSON, or a name/url past the "
               "firmware caps (%u/%u bytes)",
               static_cast<unsigned>(this->decoder_.error_code()),
               static_cast<unsigned>(luna_stream_codec::MAX_CONTROL_NAME),
               static_cast<unsigned>(luna_stream_codec::MAX_CONTROL_URL));
      this->disconnect_("control framing error");
      return;
    }
    if (status == luna_stream_codec::DownlinkDecoder::Status::NeedMore) {
      return;  // partial control; the rest arrives on the next read
    }
    luna_stream_codec::Control control;
    if (!this->decoder_.take(&control)) {
      return;
    }
    this->handle_control_(control);
    if (consumed == 0) {
      return;  // defensive: never spin on a feed that made no progress
    }
  }
}

void LunaStream::handle_control_(const luna_stream_codec::Control &control) {
  switch (control.type) {
    case luna_stream_codec::ControlType::Led:
      ESP_LOGD(TAG, "control led=%s", luna_stream_codec::led_state_name(control.led));
      if (this->led_trigger_ != nullptr) {
        this->led_trigger_->trigger(std::string(luna_stream_codec::led_state_name(control.led)));
      }
      break;
    case luna_stream_codec::ControlType::Earcon:
      ESP_LOGD(TAG, "control earcon=%s", control.name);
      if (this->earcon_trigger_ != nullptr) {
        this->earcon_trigger_->trigger(std::string(control.name));
      }
      break;
    case luna_stream_codec::ControlType::Play:
      // Reserved by the protocol for the later direct-TTS path; HA
      // play_media is the shipped playback route, so this is a no-op by design.
      ESP_LOGD(TAG, "control play reserved (url=%s format=%s); ignored", control.url, control.format);
      break;
    case luna_stream_codec::ControlType::Unknown:
    default:
      break;
  }
}

// ---------------------------------------------------------------------------
// uplink events
// ---------------------------------------------------------------------------

void LunaStream::emit_save_buffer() {
  uint8_t record[64];
  const size_t len = luna_stream_codec::build_event_save_buffer(record, sizeof(record));
  if (len == 0 || !this->enqueue_(record, len)) {
    this->dropped_events_++;
    ESP_LOGW(TAG, "save_buffer dropped (not connected or queue full)");
    return;
  }
  ESP_LOGI(TAG, "save_buffer sent (missed-wake capture)");
}

void LunaStream::emit_button(const std::string &name) {
  // Room for a full event body (TAGGED_HEADER_LEN + MAX_EVENT_JSON): a button name that
  // overflows the body used to drop the event and report it as "not connected".
  uint8_t record[luna_stream_codec::TAGGED_HEADER_LEN + luna_stream_codec::MAX_EVENT_JSON];
  const size_t len = luna_stream_codec::build_event_button(record, sizeof(record), name.c_str());
  if (len == 0) {
    this->dropped_events_++;
    ESP_LOGW(TAG,
             "button '%s' NOT sent: the name needs more than the %u-byte event body; nothing "
             "was sent (a truncated name would look like a different button)",
             name.c_str(), static_cast<unsigned>(luna_stream_codec::MAX_EVENT_JSON));
    return;
  }
  if (!this->enqueue_(record, len)) {
    this->dropped_events_++;
    ESP_LOGW(TAG, "button '%s' dropped (not connected or queue full)", name.c_str());
    return;
  }
  ESP_LOGI(TAG, "button '%s' sent", name.c_str());
}

void LunaStream::on_media_player_state_(bool idle) {
  // Debounce: `playback_done` is an edge, not a level. Called on the main
  // loop thread by the media_player callback.
  const bool was_idle = this->media_player_idle_;
  this->media_player_idle_ = idle;
  if (idle && !was_idle) {
    this->notify_media_player_idle();
  }
}

void LunaStream::notify_media_player_idle() {
  uint8_t record[64];
  const size_t len = luna_stream_codec::build_event_playback_done(record, sizeof(record));
  if (len == 0 || !this->enqueue_(record, len)) {
    this->dropped_events_++;
    ESP_LOGW(TAG, "playback_done dropped (not connected or queue full)");
    return;
  }
  ESP_LOGI(TAG, "playback_done sent");
}

void LunaStream::maybe_send_rssi_(uint32_t now) {
  if (now - this->last_rssi_at_ < RSSI_INTERVAL_MS) {
    return;
  }
  this->last_rssi_at_ = now;
  int dbm_tenths = 0;
#ifdef USE_WIFI
  if (!read_wifi_rssi_tenths(&dbm_tenths)) {
    return;
  }
#else
  return;
#endif
  uint8_t record[64];
  const size_t len = luna_stream_codec::build_event_rssi(record, sizeof(record), dbm_tenths);
  if (len == 0 || !this->enqueue_(record, len)) {
    this->dropped_events_++;
    return;
  }
  ESP_LOGD(TAG, "rssi %d.%d dBm sent", dbm_tenths / 10, dbm_tenths < 0 ? -dbm_tenths % 10 : dbm_tenths % 10);
}

}  // namespace luna_stream
}  // namespace esphome
