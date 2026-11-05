import dev.listenrr.tablet.Wire;

import java.io.ByteArrayOutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * Host-side wire conformance for dev.listenrr.tablet.Wire. Plain main, no JUnit.
 * Golden vectors are copied verbatim from docs/protocol.md section 9 (which is generated
 * from src/proto.rs). Run with: bash tablet/build.sh --test
 */
public final class WireTest {
    private static final Charset UTF8 = Charset.forName("UTF-8");
    private static int checks;
    private static int failures;

    public static void main(String[] args) {
        helloGolden();
        audioGolden();
        controlLedGolden();
        rssiEventBytes();
        playbackDoneEventBytes();
        playDecodes();
        playSplitByteByByte();
        stopDecodes();
        unknownControlSkippedByLength();
        oversizedControlDiscarded();
        escapedStringsRoundTrip();

        if (failures != 0) {
            System.out.println(failures + " of " + checks + " checks FAILED");
            System.exit(1);
        }
        System.out.println("ALL PASS (" + checks + " checks)");
    }

    // ------------------------------------------------------------------ cases

    /** docs/protocol.md 9.4: 120 bytes of JSON + the terminating '\n' = 121 bytes. */
    static void helloGolden() {
        String json = "{\"v\":1,\"source_id\":\"kitchen\",\"room\":\"kitchen\",\"token\":\"s3cr3t\","
                + "\"caps\":{\"has_speaker\":true,\"has_aec\":false},\"fw\":\"0.1.0\"}";
        byte[] got = Wire.hello("kitchen", "kitchen", "s3cr3t", true, false, "0.1.0");
        byte[] want = (json + "\n").getBytes(UTF8);
        check("hello is 121 bytes on the wire", got.length == 121);
        check("hello matches the documented 121-byte example", eq(got, want));
        check("hello JSON is 120 bytes", json.length() == 120);
        check("hello ends with exactly one newline",
                got[got.length - 1] == '\n' && got[got.length - 2] == '}');
        // The field order the backend's serde struct declares must be literal in the bytes.
        String s = new String(got, 0, got.length - 1, UTF8);
        check("hello field order is v,source_id,room,token,caps,fw",
                s.indexOf("\"v\"") < s.indexOf("\"source_id\"")
                        && s.indexOf("\"source_id\"") < s.indexOf("\"room\"")
                        && s.indexOf("\"room\"") < s.indexOf("\"token\"")
                        && s.indexOf("\"token\"") < s.indexOf("\"caps\"")
                        && s.indexOf("\"caps\"") < s.indexOf("\"fw\""));
        // A token with characters that need escaping must not break the framing.
        byte[] tricky = Wire.hello("a\"b", "c\\d", "é\n", true, false, "0.1.0");
        check("tricky hello still terminates with one newline",
                tricky[tricky.length - 1] == '\n' && count(tricky, (byte) '\n') == 1);
    }

    /** docs/protocol.md 9.1: seq = 7, pcm[i] = i, 2053 bytes. */
    static void audioGolden() {
        short[] pcm = new short[1024];
        for (int i = 0; i < 1024; i++) {
            pcm[i] = (short) i;
        }
        byte[] rec = Wire.audioRecord(7, pcm);
        check("audio record is 2053 bytes", rec.length == 2053);
        check("audio tag is 0x01", rec[0] == 0x01);
        check("audio seq 7 is u32 LE at offset 1", eqRange(rec, 1, new int[] {0x07, 0x00, 0x00, 0x00}));
        check("first 13 bytes match section 9.1",
                eq(rec, hex("01 07 00 00 00 00 00 01 00 02 00 03 00"), 0));
        check("pcm[1021] at 2047 is FD 03", eqRange(rec, 2047, new int[] {0xFD, 0x03}));
        check("pcm[1022] at 2049 is FE 03", eqRange(rec, 2049, new int[] {0xFE, 0x03}));
        check("last 4 bytes match section 9.1 (FF 03 last)",
                eqRange(rec, 2049, new int[] {0xFE, 0x03, 0xFF, 0x03}));
        // Negative samples are two's complement: the doc's pcm[0] = -2 example is FE FF.
        short[] neg = new short[1024];
        neg[0] = -2;
        byte[] nrec = Wire.audioRecord(0, neg);
        check("pcm[0] = -2 encodes as FE FF", eqRange(nrec, 5, new int[] {0xFE, 0xFF}));
        try {
            Wire.audioRecord(0, new short[1023]);
            check("a short frame is refused", false);
        } catch (IllegalArgumentException e) {
            check("a short frame is refused", true);
        }
    }

    /** docs/protocol.md 9.2: 03 22 00 + {"type":"led","state":"listening"}. */
    static void controlLedGolden() {
        byte[] rec = hex("03 22 00 "
                + "7B 22 74 79 70 65 22 3A 22 6C 65 64 22 2C 22 73 74 61 74 65 22 3A 22 6C 69 73 74 65 6E 69 6E 67 22 7D");
        check("led listening record is 37 bytes", rec.length == 37);
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            d.feed(rec, 0, rec.length);
        } catch (Wire.ProtocolException e) {
            check("led record decodes", false);
            return;
        }
        check("led listening decodes to one control", c.controls.size() == 1);
        if (!c.controls.isEmpty()) {
            Wire.Control ctl = c.controls.get(0);
            check("led type is 'led'", "led".equals(ctl.type));
            check("led state is 'listening'", "listening".equals(ctl.state));
        }
    }

    /** docs/protocol.md section 7: rssi payload 27 bytes, record 30. */
    static void rssiEventBytes() {
        byte[] rec = Wire.rssi(-62.5);
        check("rssi record is 30 bytes", rec.length == 30);
        check("rssi tag is 0x02", rec[0] == 0x02);
        check("rssi len is 27 u16 LE", eqRange(rec, 1, new int[] {0x1B, 0x00}));
        check("rssi payload is the canonical JSON",
                new String(rec, 3, rec.length - 3, UTF8).equals("{\"type\":\"rssi\",\"dbm\":-62.5}"));
    }

    /** docs/protocol.md section 7: playback_done payload 24 bytes, record 27. */
    static void playbackDoneEventBytes() {
        byte[] rec = Wire.playbackDone();
        check("playback_done record is 27 bytes", rec.length == 27);
        check("playback_done tag is 0x02", rec[0] == 0x02);
        check("playback_done len is 24 u16 LE", eqRange(rec, 1, new int[] {0x18, 0x00}));
        check("playback_done payload is the canonical JSON",
                new String(rec, 3, rec.length - 3, UTF8).equals("{\"type\":\"playback_done\"}"));
    }

    /** docs/protocol.md section 8 play row: url and format reach the caller. */
    static void playDecodes() {
        String json = "{\"type\":\"play\",\"url\":\"http://homeassistant.local:8123/api/tts_proxy/abc.mp3\",\"format\":\"mp3\"}";
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            d.feed(record(json), 0, 3 + json.getBytes(UTF8).length);
        } catch (Wire.ProtocolException e) {
            check("play decodes", false);
            return;
        }
        check("play decodes to one control", c.controls.size() == 1);
        if (!c.controls.isEmpty()) {
            Wire.Control ctl = c.controls.get(0);
            check("play type is 'play'", "play".equals(ctl.type));
            check("play url survives intact",
                    "http://homeassistant.local:8123/api/tts_proxy/abc.mp3".equals(ctl.url));
            check("play format survives intact", "mp3".equals(ctl.format));
        }
    }

    /** A record may arrive one byte at a time (section 10 rule 3). */
    static void playSplitByteByByte() {
        String json = "{\"type\":\"play\",\"url\":\"http://192.168.1.20:8123/api/tts_proxy/x.flac\",\"format\":\"flac\"}";
        byte[] rec = record(json);
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            for (int i = 0; i < rec.length; i++) {
                d.feed(rec, i, 1);
            }
        } catch (Wire.ProtocolException e) {
            check("play split byte by byte decodes", false);
            return;
        }
        check("play split byte by byte yields exactly one control", c.controls.size() == 1);
        if (!c.controls.isEmpty()) {
            check("split play url is intact",
                    "http://192.168.1.20:8123/api/tts_proxy/x.flac".equals(c.controls.get(0).url));
            check("split play format is intact", "flac".equals(c.controls.get(0).format));
        }
    }

    /** Section 8 stop row: a stop control reaches the caller, and is never counted unknown. */
    static void stopDecodes() {
        String json = "{\"type\":\"stop\"}";
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            d.feed(record(json), 0, 3 + json.getBytes(UTF8).length);
        } catch (Wire.ProtocolException e) {
            check("stop decodes", false);
            return;
        }
        check("stop decodes to one control", c.controls.size() == 1);
        check("stop is not counted as unknown", d.unknownControls() == 0);
        if (!c.controls.isEmpty()) {
            Wire.Control ctl = c.controls.get(0);
            check("stop type is 'stop'", "stop".equals(ctl.type));
            check("stop carries no other fields",
                    ctl.state == null && ctl.name == null && ctl.url == null && ctl.format == null);
        }
        // The same record split anywhere (section 10 rule 3) must decode identically.
        byte[] rec = record(json);
        Captured c2 = new Captured();
        Wire.ControlDecoder d2 = new Wire.ControlDecoder(c2);
        try {
            for (int i = 0; i < rec.length; i++) {
                d2.feed(rec, i, 1);
            }
        } catch (Wire.ProtocolException e) {
            check("stop split byte by byte decodes", false);
            return;
        }
        check("stop split byte by byte yields exactly one control", c2.controls.size() == 1);
        if (!c2.controls.isEmpty()) {
            check("split stop type is 'stop'", "stop".equals(c2.controls.get(0).type));
        }
    }

    /** Section 8: an unimplemented control must still be consumed by its len. */
    static void unknownControlSkippedByLength() {
        String unknown = "{\"type\":\"hologram\",\"mode\":\"spin\"}";
        String led = "{\"type\":\"led\",\"state\":\"speaking\"}";
        byte[] a = record(unknown);
        byte[] b = record(led);
        byte[] both = new byte[a.length + b.length];
        System.arraycopy(a, 0, both, 0, a.length);
        System.arraycopy(b, 0, both, a.length, b.length);
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            d.feed(both, 0, both.length);
        } catch (Wire.ProtocolException e) {
            check("unknown control does not raise", false);
            return;
        }
        check("unknown control produces no callback", c.controls.size() == 1);
        check("unknown control is counted as skipped",
                d.unknownControls() == 1 && "hologram".equals(d.lastUnknownType()));
        if (!c.controls.isEmpty()) {
            check("the record after an unknown control still decodes",
                    "led".equals(c.controls.get(0).type) && "speaking".equals(c.controls.get(0).state));
        }
        // A malformed payload is dropped, not fatal: the following record must still parse.
        Captured c2 = new Captured();
        Wire.ControlDecoder d2 = new Wire.ControlDecoder(c2);
        try {
            d2.feed(record("{not json"), 0, 3 + "{not json".length());
            d2.feed(record(led), 0, 3 + led.length());
        } catch (Wire.ProtocolException e) {
            check("malformed control does not raise", false);
            return;
        }
        check("malformed control is counted and skipped", c2.controls.size() == 1 && d2.malformedControls() == 1);
        check("the record after a malformed control still decodes",
                "speaking".equals(c2.controls.get(0).state));
    }

    /** A control longer than 4096 bytes is discarded, never stored, and must not desync. */
    static void oversizedControlDiscarded() {
        final int len = 60000;
        byte[] head = new byte[] {0x03, (byte) (len & 0xFF), (byte) (len >>> 8)};
        byte[] filler = new byte[len];
        for (int i = 0; i < len; i++) {
            filler[i] = (byte) ('A' + (i % 26));
        }
        String led = "{\"type\":\"led\",\"state\":\"idle\"}";
        byte[] tail = record(led);
        Captured c = new Captured();
        Wire.ControlDecoder d = new Wire.ControlDecoder(c);
        try {
            d.feed(head, 0, head.length);
            d.feed(filler, 0, 1500); // arbitrary read boundaries, none of them the whole payload
            d.feed(filler, 1500, 41000);
            d.feed(filler, 42500, len - 42500);
            d.feed(tail, 0, 2);
            d.feed(tail, 2, tail.length - 2);
        } catch (Wire.ProtocolException e) {
            check("oversized control does not raise", false);
            return;
        }
        check("oversized control is never stored (buffer stays 4096)", d.bufferCapacity() == 4096);
        check("oversized control emits nothing", c.controls.size() == 1);
        check("oversized control is counted", d.oversizedControls() == 1);
        check("all 60000 bytes were consumed", d.discardedBytes() == len);
        check("the record after an oversized control still decodes",
                !c.controls.isEmpty() && "led".equals(c.controls.get(0).type)
                        && "idle".equals(c.controls.get(0).state));
        // A non-control tag on the downlink is a protocol error, so the socket can be recycled.
        Captured c2 = new Captured();
        Wire.ControlDecoder d2 = new Wire.ControlDecoder(c2);
        boolean raised = false;
        try {
            d2.feed(new byte[] {0x01, 0, 0}, 0, 3);
        } catch (Wire.ProtocolException e) {
            raised = true;
        }
        check("a wrong downlink tag raises", raised);
    }

    /** JSON escaping on the way out must be readable on the way back in. */
    static void escapedStringsRoundTrip() {
        String url = "http://h/api/tts_proxy/a\"b\\c\nd?x=1&e=2";
        String json = "{\"type\":\"play\",\"url\":" + Wire.jsonString(url) + ",\"format\":\"wav\"}";
        byte[] rec = record(json);
        Captured c = new Captured();
        try {
            new Wire.ControlDecoder(c).feed(rec, 0, rec.length);
        } catch (Wire.ProtocolException e) {
            check("escaped url decodes", false);
            return;
        }
        check("escaped url decodes back unchanged",
                c.controls.size() == 1 && url.equals(c.controls.get(0).url));
    }

    // ---------------------------------------------------------------- helpers

    static final class Captured implements Wire.Sink {
        final List<Wire.Control> controls = new ArrayList<Wire.Control>();

        public void onControl(Wire.Control c) {
            controls.add(c);
        }
    }

    static void check(String name, boolean ok) {
        checks++;
        if (!ok) {
            failures++;
            System.out.println("FAIL: " + name);
            return;
        }
        System.out.println("PASS: " + name);
    }

    /** tag 0x03 + u16 LE len + payload, the way the backend frames a control. */
    static byte[] record(String json) {
        byte[] p = json.getBytes(UTF8);
        ByteArrayOutputStream out = new ByteArrayOutputStream(3 + p.length);
        out.write((byte) 0x03);
        out.write((byte) (p.length & 0xFF));
        out.write((byte) (p.length >>> 8));
        out.write(p, 0, p.length);
        return out.toByteArray();
    }

    static boolean eq(byte[] got, byte[] want) {
        if (got.length != want.length) {
            return false;
        }
        for (int i = 0; i < got.length; i++) {
            if (got[i] != want[i]) {
                return false;
            }
        }
        return true;
    }

    static boolean eq(byte[] got, byte[] want, int off) {
        if (got.length - off < want.length) {
            return false;
        }
        for (int i = 0; i < want.length; i++) {
            if (got[off + i] != want[i]) {
                return false;
            }
        }
        return true;
    }

    static boolean eqRange(byte[] got, int off, int[] want) {
        if (got.length - off < want.length) {
            return false;
        }
        for (int i = 0; i < want.length; i++) {
            if ((got[off + i] & 0xFF) != want[i]) {
                return false;
            }
        }
        return true;
    }

    static int count(byte[] b, byte v) {
        int n = 0;
        for (int i = 0; i < b.length; i++) {
            if (b[i] == v) {
                n++;
            }
        }
        return n;
    }

    static byte[] hex(String s) {
        String[] parts = s.trim().split("\\s+");
        byte[] out = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return out;
    }
}
