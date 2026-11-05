package dev.listenrr.tablet;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The listenrr source wire format (docs/protocol.md; src/proto.rs is authoritative).
 * Pure Java: no android.* imports, so the host-side WireTest can run it.
 */
public final class Wire {
    private Wire() {}

    public static final int PROTOCOL_VERSION = 1;
    public static final int TAG_AUDIO = 0x01;
    public static final int TAG_EVENT = 0x02;
    public static final int TAG_CONTROL = 0x03;
    public static final int FRAME_SAMPLES = 1024;
    public static final int AUDIO_RECORD_LEN = 1 + 4 + FRAME_SAMPLES * 2; // 2053
    /** A control payload larger than this is read and discarded, never stored. */
    public static final int MAX_CONTROL_PAYLOAD = 4096;

    private static final Charset UTF8 = Charset.forName("UTF-8");

    // ---------------------------------------------------------------- uplink

    /** The hello line: one JSON object + '\n', no tag byte, exact field order of docs section 9.4. */
    public static byte[] hello(String sourceId, String room, String token,
                               boolean hasSpeaker, boolean hasAec, String fw) {
        StringBuilder sb = new StringBuilder(160);
        sb.append("{\"v\":").append(PROTOCOL_VERSION);
        sb.append(",\"source_id\":").append(jsonString(sourceId));
        sb.append(",\"room\":").append(jsonString(room));
        sb.append(",\"token\":").append(jsonString(token));
        sb.append(",\"caps\":{\"has_speaker\":").append(hasSpeaker);
        sb.append(",\"has_aec\":").append(hasAec).append('}');
        sb.append(",\"fw\":").append(jsonString(fw));
        sb.append("}\n");
        return sb.toString().getBytes(UTF8);
    }

    /** One audio record: 0x01, u32 LE seq, 1024 x i16 LE. Exactly {@link #AUDIO_RECORD_LEN} bytes. */
    public static byte[] audioRecord(int seq, short[] pcm) {
        if (pcm.length != FRAME_SAMPLES) {
            throw new IllegalArgumentException("frame needs " + FRAME_SAMPLES + " samples, got " + pcm.length);
        }
        byte[] out = new byte[AUDIO_RECORD_LEN];
        out[0] = (byte) TAG_AUDIO;
        out[1] = (byte) seq;
        out[2] = (byte) (seq >>> 8);
        out[3] = (byte) (seq >>> 16);
        out[4] = (byte) (seq >>> 24);
        int o = 5;
        for (int i = 0; i < FRAME_SAMPLES; i++) {
            short s = pcm[i];
            out[o++] = (byte) s;
            out[o++] = (byte) (s >> 8);
        }
        return out;
    }

    /** Event record: 0x02, u16 LE len, JSON payload. */
    public static byte[] event(String json) {
        byte[] p = json.getBytes(UTF8);
        if (p.length > 0xFFFF) {
            throw new IllegalArgumentException("event payload too long");
        }
        byte[] out = new byte[3 + p.length];
        out[0] = (byte) TAG_EVENT;
        out[1] = (byte) p.length;
        out[2] = (byte) (p.length >>> 8);
        System.arraycopy(p, 0, out, 3, p.length);
        return out;
    }

    public static byte[] playbackDone() {
        return event("{\"type\":\"playback_done\"}");
    }

    public static byte[] rssi(double dbm) {
        return event("{\"type\":\"rssi\",\"dbm\":" + String.format(Locale.ROOT, "%.1f", dbm) + "}");
    }

    // -------------------------------------------------------------- downlink

    /** A decoded control. Fields not carried by the control type are null. */
    public static final class Control {
        public final String type;   // "led" | "earcon" | "play" | "stop" | anything the backend adds later
        public final String state;  // led
        public final String name;   // earcon
        public final String url;    // play
        public final String format; // play

        Control(String type, String state, String name, String url, String format) {
            this.type = type;
            this.state = state;
            this.name = name;
            this.url = url;
            this.format = format;
        }
    }

    public interface Sink {
        void onControl(Control c);
    }

    /** Thrown when the downlink carries something the protocol forbids (a tag other than 0x03). */
    public static final class ProtocolException extends IOException {
        public ProtocolException(String msg) {
            super(msg);
        }
    }

    /**
     * Incremental downlink decoder: feed it whatever a socket read returned, split anywhere.
     * One fixed 4096-byte body buffer; a longer payload is skipped byte-wise without storing.
     * Every control is consumed by its declared len, recognised or not.
     */
    public static final class ControlDecoder {
        private static final int ST_TAG = 0, ST_LEN0 = 1, ST_LEN1 = 2, ST_BODY = 3, ST_DISCARD = 4;

        private final Sink sink;
        private final byte[] body = new byte[MAX_CONTROL_PAYLOAD];
        private int state = ST_TAG;
        private int len;
        private int have;
        private long discardLeft;

        private long discardedBytes;
        private int oversized;
        private int malformed;
        private int unknown;
        private String lastUnknown;

        public ControlDecoder(Sink sink) {
            this.sink = sink;
        }

        /** Size of the only payload buffer this decoder ever owns. */
        public int bufferCapacity() {
            return body.length;
        }

        public long discardedBytes() {
            return discardedBytes;
        }

        /** Controls whose declared len exceeded {@link #MAX_CONTROL_PAYLOAD}. */
        public int oversizedControls() {
            return oversized;
        }

        /** Controls whose payload was not a parseable JSON object. */
        public int malformedControls() {
            return malformed;
        }

        /** Controls whose "type" is not one of led / earcon / play / stop: consumed and skipped. */
        public int unknownControls() {
            return unknown;
        }

        /** The last unknown control type seen, for the log line. */
        public String lastUnknownType() {
            return lastUnknown;
        }

        public void feed(byte[] buf, int off, int n) throws ProtocolException {
            int end = off + n;
            int i = off;
            while (i < end) {
                switch (state) {
                    case ST_TAG: {
                        int tag = buf[i++] & 0xFF;
                        if (tag != TAG_CONTROL) {
                            throw new ProtocolException("unexpected downlink tag 0x" + Integer.toHexString(tag));
                        }
                        state = ST_LEN0;
                        break;
                    }
                    case ST_LEN0:
                        len = buf[i++] & 0xFF;
                        state = ST_LEN1;
                        break;
                    case ST_LEN1:
                        len |= (buf[i++] & 0xFF) << 8;
                        have = 0;
                        if (len > MAX_CONTROL_PAYLOAD) {
                            oversized++;
                            discardLeft = len;
                            state = ST_DISCARD;
                        } else if (len == 0) {
                            complete();
                        } else {
                            state = ST_BODY;
                        }
                        break;
                    case ST_BODY: {
                        int take = Math.min(len - have, end - i);
                        System.arraycopy(buf, i, body, have, take);
                        have += take;
                        i += take;
                        if (have == len) {
                            complete();
                        }
                        break;
                    }
                    default: { // ST_DISCARD
                        int take = (int) Math.min(discardLeft, (long) (end - i));
                        i += take;
                        discardLeft -= take;
                        discardedBytes += take;
                        if (discardLeft == 0) {
                            state = ST_TAG;
                        }
                        break;
                    }
                }
            }
        }

        private void complete() {
            state = ST_TAG;
            Map<String, Object> m = Json.parseObject(new String(body, 0, len, UTF8));
            if (m == null || !(m.get("type") instanceof String)) {
                malformed++;
                return;
            }
            // Section 8: a control we do not implement is still consumed by its len, then
            // dropped here; the caller only ever sees the four types that need action.
            String type = (String) m.get("type");
            if (!"led".equals(type) && !"earcon".equals(type) && !"play".equals(type)
                    && !"stop".equals(type)) {
                unknown++;
                lastUnknown = type;
                return;
            }
            sink.onControl(new Control(type, str(m, "state"), str(m, "name"),
                    str(m, "url"), str(m, "format")));
        }

        private static String str(Map<String, Object> m, String k) {
            Object v = m.get(k);
            return v instanceof String ? (String) v : null;
        }
    }

    // ------------------------------------------------------------------ JSON

    /**
     * JSON string literal with the escapes JSON requires; null becomes the literal null.
     * Public so the conformance test can check that what the encoder escapes, it can read back.
     */
    public static String jsonString(String s) {
        if (s == null) {
            return "null";
        }
        StringBuilder sb = new StringBuilder(s.length() + 2);
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '"': sb.append("\\\""); break;
                case '\\': sb.append("\\\\"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '\t': sb.append("\\t"); break;
                default:
                    if (c < 0x20) {
                        sb.append(String.format(Locale.ROOT, "\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
            }
        }
        return sb.append('"').toString();
    }

    /**
     * Tiny JSON reader, enough for the flat control objects (nested values are parsed and kept
     * as Map / List / String / Double / Boolean / null). Returns null for anything malformed.
     */
    static final class Json {
        private final String s;
        private int p;

        private Json(String s) {
            this.s = s;
        }

        @SuppressWarnings("unchecked")
        static Map<String, Object> parseObject(String text) {
            try {
                Json j = new Json(text);
                j.ws();
                Object v = j.value();
                j.ws();
                if (j.p != text.length() || !(v instanceof Map)) {
                    return null;
                }
                return (Map<String, Object>) v;
            } catch (RuntimeException e) {
                return null; // IndexOutOfBounds / IllegalArgument / NumberFormat: all mean malformed
            }
        }

        private void ws() {
            while (p < s.length()) {
                char c = s.charAt(p);
                if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                    p++;
                } else {
                    break;
                }
            }
        }

        private char peek() {
            return s.charAt(p);
        }

        private void expect(char c) {
            if (s.charAt(p) != c) {
                throw new IllegalArgumentException("expected " + c + " at " + p);
            }
            p++;
        }

        private Object value() {
            char c = peek();
            if (c == '{') {
                return object();
            } else if (c == '[') {
                return array();
            } else if (c == '"') {
                return string();
            } else if (s.startsWith("true", p)) {
                p += 4;
                return Boolean.TRUE;
            } else if (s.startsWith("false", p)) {
                p += 5;
                return Boolean.FALSE;
            } else if (s.startsWith("null", p)) {
                p += 4;
                return null;
            }
            return number();
        }

        private Map<String, Object> object() {
            Map<String, Object> m = new LinkedHashMap<String, Object>();
            expect('{');
            ws();
            if (peek() == '}') {
                p++;
                return m;
            }
            while (true) {
                ws();
                String k = string();
                ws();
                expect(':');
                ws();
                m.put(k, value());
                ws();
                char c = s.charAt(p++);
                if (c == '}') {
                    return m;
                }
                if (c != ',') {
                    throw new IllegalArgumentException("expected , or } at " + (p - 1));
                }
            }
        }

        private List<Object> array() {
            List<Object> l = new ArrayList<Object>();
            expect('[');
            ws();
            if (peek() == ']') {
                p++;
                return l;
            }
            while (true) {
                ws();
                l.add(value());
                ws();
                char c = s.charAt(p++);
                if (c == ']') {
                    return l;
                }
                if (c != ',') {
                    throw new IllegalArgumentException("expected , or ] at " + (p - 1));
                }
            }
        }

        private String string() {
            expect('"');
            StringBuilder sb = new StringBuilder();
            while (true) {
                char c = s.charAt(p++);
                if (c == '"') {
                    return sb.toString();
                }
                if (c != '\\') {
                    sb.append(c);
                    continue;
                }
                char e = s.charAt(p++);
                switch (e) {
                    case '"': sb.append('"'); break;
                    case '\\': sb.append('\\'); break;
                    case '/': sb.append('/'); break;
                    case 'b': sb.append('\b'); break;
                    case 'f': sb.append('\f'); break;
                    case 'n': sb.append('\n'); break;
                    case 'r': sb.append('\r'); break;
                    case 't': sb.append('\t'); break;
                    case 'u':
                        sb.append((char) Integer.parseInt(s.substring(p, p + 4), 16));
                        p += 4;
                        break;
                    default:
                        throw new IllegalArgumentException("bad escape at " + (p - 1));
                }
            }
        }

        private Double number() {
            int start = p;
            while (p < s.length() && "+-0123456789.eE".indexOf(s.charAt(p)) >= 0) {
                p++;
            }
            if (p == start) {
                throw new IllegalArgumentException("unexpected character at " + p);
            }
            return Double.valueOf(s.substring(start, p));
        }
    }
}
