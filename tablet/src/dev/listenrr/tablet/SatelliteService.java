package dev.listenrr.tablet;

import android.app.Notification;
import android.app.NotificationChannel;
import android.app.NotificationManager;
import android.app.PendingIntent;
import android.app.Service;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.content.pm.ServiceInfo;
import android.media.AudioFormat;
import android.media.AudioRecord;
import android.media.MediaRecorder;
import android.net.wifi.WifiManager;
import android.os.Build;
import android.os.PowerManager;
import android.util.Log;

import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayDeque;

/**
 * The listenrr source: this tablet's microphone, streamed to the backend, and its speaker.
 *
 * <p>One TCP connection is one source. This app opens it, sends the hello line once per
 * connection, then streams 2053-byte audio records and event records, and reads the
 * backend's control records. Reconnect uses 1 s doubling to a 30 s cap, restarts {@code seq}
 * at 0 and re-sends the hello (docs/protocol.md section 12).
 *
 * <p>Three threads: capture (microphone -> bounded queue), writer (queue -> socket), and this
 * service's network loop (connect, read controls, back off). The capture thread never touches
 * the socket, so a stalled network cannot stall the microphone: frames are dropped and the
 * backend sees an honest {@code seq} gap instead.
 */
public final class SatelliteService extends Service {
    private static final String TAG = "listenrr";
    private static final String CHANNEL_ID = "listenrr-source";
    private static final int NOTIFICATION_ID = 1;

    private static final int SAMPLE_RATE = 16000;
    private static final int QUEUE_FRAMES = 16;
    private static final int CONNECT_TIMEOUT_MS = 5000;
    private static final long SETTLE_MS = 5000;
    private static final long BACKOFF_BASE_MS = 1000;
    private static final long BACKOFF_CAP_MS = 30000;
    private static final long RSSI_INTERVAL_MS = 30000;
    private static final int READ_BUF = 4096;

    private final Object startLock = new Object();
    private boolean started;
    private volatile boolean running;
    private volatile boolean connected;

    private Outbox outbox;
    private Player player;
    private Thread networkThread;
    private Thread captureThread;
    private Thread rssiThread;
    private PowerManager.WakeLock wakeLock;
    private WifiManager.WifiLock wifiLock;

    // ------------------------------------------------------------- lifecycle

    @Override
    public void onCreate() {
        super.onCreate();
        outbox = new Outbox();
        player = new Player(this, new Player.Done() {
            public void onPlaybackDone() {
                // Dropped without a word if the link is down: there is nobody to tell.
                outbox.offerEvent(Wire.playbackDone());
            }
        });
        try {
            startForegroundNotification();
        } catch (RuntimeException e) {
            Log.e(TAG, "cannot become a foreground service: " + e, e);
            stopSelf();
            return;
        }
        if (checkSelfPermission(android.Manifest.permission.RECORD_AUDIO)
                != PackageManager.PERMISSION_GRANTED) {
            Log.e(TAG, "RECORD_AUDIO is not granted; the source cannot capture. Open the app once.");
            stopSelf();
            return;
        }
        acquireLocks();
        running = true;
        networkThread = new Thread(new Runnable() {
            public void run() {
                networkLoop();
            }
        }, "listenrr-net");
        captureThread = new Thread(new Runnable() {
            public void run() {
                captureLoop();
            }
        }, "listenrr-capture");
        rssiThread = new Thread(new Runnable() {
            public void run() {
                rssiLoop();
            }
        }, "listenrr-rssi");
        networkThread.start();
        captureThread.start();
        rssiThread.start();
        Log.i(TAG, "source '" + Config.SOURCE_ID + "' started, target "
                + Config.BACKEND_HOST + ":" + Config.BACKEND_PORT + " fw " + Config.FW_VERSION);
    }

    @Override
    public int onStartCommand(Intent intent, int flags, int startId) {
        synchronized (startLock) {
            if (started) {
                return START_STICKY; // a second start is a no-op; one source, one connection
            }
            started = true;
        }
        // onCreate did the work. If the microphone permission was missing it stopped itself,
        // and Android will not usefully retry that from the background - the app is opened.
        return START_STICKY;
    }

    @Override
    public void onDestroy() {
        Log.i(TAG, "source stopping");
        running = false;
        if (outbox != null) {
            outbox.shutdown();
        }
        joinQuietly(networkThread);
        joinQuietly(captureThread);
        joinQuietly(rssiThread);
        if (player != null) {
            player.close();
        }
        releaseLocks();
        synchronized (startLock) {
            started = false;
        }
        stopForeground(true);
        super.onDestroy();
    }

    @Override
    public android.os.IBinder onBind(Intent intent) {
        return null; // not bound; started only
    }

    // ---------------------------------------------------------- network loop

    private void networkLoop() {
        final byte[] hello = Wire.hello(Config.SOURCE_ID, Config.ROOM, Config.TOKEN,
                true, false, Config.FW_VERSION);
        int failures = 0; // consecutive connections that did not survive 5 s
        while (running) {
            Socket sock = null;
            long began = System.currentTimeMillis();
            try {
                sock = new Socket();
                sock.connect(new InetSocketAddress(Config.BACKEND_HOST, Config.BACKEND_PORT),
                        CONNECT_TIMEOUT_MS);
                sock.setTcpNoDelay(true);
                sock.setKeepAlive(true);
                connected = true;
                notifyState();
                Log.i(TAG, "connected to " + Config.BACKEND_HOST + ":" + Config.BACKEND_PORT
                        + " as '" + Config.SOURCE_ID + "', sending hello (" + hello.length
                        + " bytes)");
                final OutputStream out = new BufferedOutputStream(sock.getOutputStream());
                out.write(hello);
                out.flush();
                outbox.openLink(); // seq restarts at 0 on every connection

                Thread writer = new Thread(new Runnable() {
                    public void run() {
                        writerLoop(out);
                    }
                }, "listenrr-writer");
                writer.start();
                try {
                    readerLoop(sock.getInputStream());
                } finally {
                    connected = false;
                    outbox.closeLink(); // stops the writer, so the join below is immediate
                    writer.join(2000);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (IOException e) {
                Log.w(TAG, "connection lost: " + e);
            } finally {
                outbox.closeLink();
                connected = false;
                notifyState();
                closeQuietly(sock);
            }
            if (!running) {
                break;
            }
            // The ladder resets only on a connection that survived 5 s, not on a TCP connect:
            // there is no hello acknowledgement, so survival is the only proof of admission.
            if (settled(began)) {
                failures = 0;
            }
            long delay = backoff(failures);
            failures++;
            Log.i(TAG, "reconnecting in " + delay + " ms (consecutive short connections: "
                    + failures + ")");
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        Log.i(TAG, "network loop exited");
    }

    /** Read and dispatch control records until the socket closes or breaks. */
    private void readerLoop(InputStream in) throws IOException {
        final Wire.ControlDecoder decoder = new Wire.ControlDecoder(new Wire.Sink() {
            public void onControl(Wire.Control c) {
                handleControl(c);
            }
        });
        byte[] buf = new byte[READ_BUF];
        int n;
        while (running && (n = in.read(buf)) > 0) {
            try {
                decoder.feed(buf, 0, n);
            } catch (Wire.ProtocolException e) {
                Log.w(TAG, "control framing error: " + e + "; reconnecting");
                return;
            }
        }
        StringBuilder skipped = new StringBuilder();
        if (decoder.oversizedControls() > 0) {
            skipped.append(", ").append(decoder.oversizedControls()).append(" oversized discarded");
        }
        if (decoder.unknownControls() > 0) {
            skipped.append(", ").append(decoder.unknownControls())
                    .append(" unknown skipped (last '").append(decoder.lastUnknownType()).append("')");
        }
        if (decoder.malformedControls() > 0) {
            skipped.append(", ").append(decoder.malformedControls()).append(" malformed dropped");
        }
        Log.i(TAG, "downlink closed by peer" + skipped);
    }

    /** Runs on the reader thread: keep it short, the socket is being read here. */
    private void handleControl(Wire.Control c) {
        if ("led".equals(c.type)) {
            Log.i(TAG, "led " + c.state + " (no display in this version)");
        } else if ("earcon".equals(c.type)) {
            player.earcon(c.name);
        } else if ("stop".equals(c.type)) {
            player.stop();
        } else { // the decoder only hands out led / earcon / play / stop
            player.play(c.url, c.format);
        }
    }

    /** Drain the frame queue onto the socket. Events go out before frames. */
    private void writerLoop(OutputStream out) {
        try {
            byte[] rec;
            while ((rec = outbox.take()) != null) {
                out.write(rec);
                out.flush();
            }
        } catch (IOException e) {
            Log.w(TAG, "uplink write failed: " + e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt(); // the service is going away
        } finally {
            outbox.closeLink(); // also unblocks the reader: closing the stream closes the socket
            closeQuietly(out);
        }
    }

    // ---------------------------------------------------------- capture thread

    private void captureLoop() {
        final short[] buf = new short[Wire.FRAME_SAMPLES];
        long levelSumSq = 0;
        long levelSamples = 0;
        int levelPeak = 0;
        int levelFrames = 0;
        while (running) {
            AudioRecord rec = newAudioRecord();
            if (rec == null) {
                sleep(1000);
                continue;
            }
            boolean broke = false;
            try {
                rec.startRecording();
                if (rec.getRecordingState() != AudioRecord.RECORDSTATE_RECORDING) {
                    Log.w(TAG, "AudioRecord did not start recording (microphone withheld?)");
                    broke = true;
                }
                while (running && !broke) {
                    int n = rec.read(buf, 0, buf.length);
                    if (n < 0) {
                        Log.w(TAG, "microphone read error " + n);
                        broke = true;
                    } else if (n == buf.length) {
                        outbox.offerFrame(buf);
                        // Level diagnostics: a healthy stream of near-silent samples is exactly
                        // what a withheld or gated microphone looks like to the backend, so the
                        // app reports what it actually captured, every 10 s (about 156 frames).
                        long sumSq = 0;
                        int framePeak = 0;
                        for (int i = 0; i < n; i++) {
                            int s = buf[i];
                            sumSq += (long) s * s;
                            int a = s < 0 ? -s : s;
                            if (a > framePeak) {
                                framePeak = a;
                            }
                        }
                        levelSumSq += sumSq;
                        levelSamples += n;
                        if (framePeak > levelPeak) {
                            levelPeak = framePeak;
                        }
                        if (++levelFrames >= 156) {
                            double rms = Math.sqrt((double) levelSumSq / levelSamples);
                            Log.i(TAG, String.format(java.util.Locale.US,
                                    "mic level over 10 s: rms %.1f dBFS, peak %.1f dBFS",
                                    20.0 * Math.log10(Math.max(rms, 1.0) / 32768.0),
                                    20.0 * Math.log10(Math.max(levelPeak, 1) / 32768.0)));
                            levelSumSq = 0;
                            levelSamples = 0;
                            levelPeak = 0;
                            levelFrames = 0;
                        }
                    } else {
                        // Blocking read returns the full count or an error; anything else is
                        // worth a line, because those samples are gone.
                        Log.w(TAG, "microphone returned a partial frame of " + n + " samples");
                    }
                }
            } catch (RuntimeException e) {
                Log.w(TAG, "capture failed: " + e, e);
                broke = true;
            } finally {
                try {
                    rec.stop();
                } catch (RuntimeException e) {
                    Log.w(TAG, "AudioRecord.stop threw: " + e);
                }
                rec.release();
            }
            if (broke && running) {
                sleep(1000); // the microphone is being denied; do not spin
            }
        }
        Log.i(TAG, "capture loop exited");
    }

    private AudioRecord newAudioRecord() {
        int min;
        try {
            min = AudioRecord.getMinBufferSize(SAMPLE_RATE, AudioFormat.CHANNEL_IN_MONO,
                    AudioFormat.ENCODING_PCM_16BIT);
        } catch (RuntimeException e) {
            Log.w(TAG, "getMinBufferSize threw: " + e);
            return null;
        }
        if (min <= 0) {
            Log.w(TAG, "16 kHz mono PCM is not available (getMinBufferSize=" + min + ")");
            return null;
        }
        int size = Math.max(min, Wire.FRAME_SAMPLES * 2 * 4); // >= 4 frames of headroom
        AudioRecord rec;
        try {
            // CAMCORDER, not VOICE_RECOGNITION: measured on the SM-X230 (Samsung), speech through
            // VOICE_RECOGNITION, MIC and UNPROCESSED peaked at -47/-33/-43 dBFS, while CAMCORDER
            // gave -9 dBFS peak at the same distance, so the wake model never fired on the others.
            rec = new AudioRecord(MediaRecorder.AudioSource.CAMCORDER, SAMPLE_RATE,
                    AudioFormat.CHANNEL_IN_MONO, AudioFormat.ENCODING_PCM_16BIT, size);
        } catch (RuntimeException e) {
            Log.w(TAG, "AudioRecord could not be created: " + e);
            return null;
        }
        if (rec.getState() != AudioRecord.STATE_INITIALIZED) {
            Log.w(TAG, "AudioRecord failed to initialise");
            rec.release();
            return null;
        }
        Log.i(TAG, "capturing 16 kHz mono PCM, buffer " + size + " bytes, frame "
                + Wire.FRAME_SAMPLES + " samples");
        return rec;
    }

    // ------------------------------------------------------------- rssi thread

    private void rssiLoop() {
        WifiManager wifi = (WifiManager) getApplicationContext()
                .getSystemService(Context.WIFI_SERVICE);
        while (running) {
            sleep(RSSI_INTERVAL_MS);
            if (!running || !outbox.isOpen() || wifi == null) {
                continue;
            }
            int dbm;
            try {
                dbm = wifi.getConnectionInfo().getRssi();
            } catch (RuntimeException e) {
                Log.w(TAG, "rssi read failed: " + e);
                continue;
            }
            if (dbm <= -127) {
                Log.w(TAG, "rssi unavailable; skipping this heartbeat");
                continue; // best-effort: the backend treats a silent heartbeat as none
            }
            outbox.offerEvent(Wire.rssi(dbm));
        }
        Log.i(TAG, "rssi loop exited");
    }

    // ------------------------------------------------------------ foreground

    /** Re-post the ongoing notification so its one line tells the truth about the link. */
    private void notifyState() {
        try {
            startForegroundNotification();
        } catch (RuntimeException e) {
            Log.w(TAG, "notification not updated: " + e);
        }
    }

    private void startForegroundNotification() {
        NotificationManager nm = (NotificationManager) getSystemService(Context.NOTIFICATION_SERVICE);
        NotificationChannel channel = new NotificationChannel(CHANNEL_ID, "listenrr source",
                NotificationManager.IMPORTANCE_LOW);
        channel.setShowBadge(false);
        channel.setDescription("Streams this tablet's microphone to the listenrr backend.");
        if (nm != null) {
            nm.createNotificationChannel(channel);
        }
        Intent open = new Intent(this, MainActivity.class);
        open.setFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
        PendingIntent tap = PendingIntent.getActivity(this, 0, open,
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);
        Notification.Builder builder = new Notification.Builder(this, CHANNEL_ID)
                .setSmallIcon(android.R.drawable.ic_btn_speak_now)
                .setContentTitle("listenrr source: " + Config.SOURCE_ID)
                .setContentText((connected ? "streaming to " : "reconnecting to ")
                        + Config.BACKEND_HOST + ":" + Config.BACKEND_PORT)
                .setContentIntent(tap)
                .setOngoing(true);
        Notification notification = builder.build();
        if (Build.VERSION.SDK_INT >= 29) {
            startForeground(NOTIFICATION_ID, notification,
                    ServiceInfo.FOREGROUND_SERVICE_TYPE_MICROPHONE);
        } else {
            startForeground(NOTIFICATION_ID, notification);
        }
    }

    private void acquireLocks() {
        PowerManager pm = (PowerManager) getSystemService(Context.POWER_SERVICE);
        if (pm != null) {
            wakeLock = pm.newWakeLock(PowerManager.PARTIAL_WAKE_LOCK, "listenrr:capture");
            wakeLock.setReferenceCounted(false);
            wakeLock.acquire();
        }
        WifiManager wm = (WifiManager) getApplicationContext()
                .getSystemService(Context.WIFI_SERVICE);
        if (wm != null) {
            @SuppressWarnings("deprecation")
            WifiManager.WifiLock wl = wm.createWifiLock(WifiManager.WIFI_MODE_FULL_HIGH_PERF,
                    "listenrr:stream");
            wl.setReferenceCounted(false);
            wl.acquire();
            wifiLock = wl;
        }
        Log.i(TAG, "wake lock and high-performance Wi-Fi lock held");
    }

    private void releaseLocks() {
        try {
            if (wakeLock != null && wakeLock.isHeld()) {
                wakeLock.release();
            }
        } catch (RuntimeException e) {
            Log.w(TAG, "wake lock release threw: " + e);
        }
        try {
            if (wifiLock != null && wifiLock.isHeld()) {
                wifiLock.release();
            }
        } catch (RuntimeException e) {
            Log.w(TAG, "wifi lock release threw: " + e);
        }
        wakeLock = null;
        wifiLock = null;
    }

    // ---------------------------------------------------------------- helpers

    /** A connection that lived at least 5 s is proof the backend admitted us (section 12). */
    private static boolean settled(long beganMillis) {
        return System.currentTimeMillis() - beganMillis >= SETTLE_MS;
    }

    private static long backoff(int attempt) {
        if (attempt <= 0) {
            return BACKOFF_BASE_MS;
        }
        long delay = BACKOFF_BASE_MS << Math.min(attempt, 5);
        return Math.min(BACKOFF_CAP_MS, delay);
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void joinQuietly(Thread t) {
        if (t == null) {
            return;
        }
        try {
            t.interrupt();
            t.join(3000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c == null) {
            return;
        }
        try {
            c.close();
        } catch (IOException ignored) {
            // a socket we are dropping needs no error handling
        }
    }

    /**
     * The bounded uplink queue. {@code seq} is assigned here, at capture time, under the same
     * lock that hands frames to the writer - so a frame's number can never disagree with its
     * position in the stream, and a reconnect can never leak a stale number into the new
     * connection. Full means drop the oldest: the backend then counts a real seq gap, which is
     * the honest accounting (section 11: a missing seq is loss, not an error).
     */
    private final class Outbox {
        private final ArrayDeque<byte[]> frames = new ArrayDeque<byte[]>();
        private final ArrayDeque<byte[]> events = new ArrayDeque<byte[]>();
        private int nextSeq;
        private boolean open;
        private boolean closed;
        private long dropped;

        synchronized void openLink() {
            frames.clear();
            events.clear();
            nextSeq = 0;
            open = true;
            notifyAll();
        }

        synchronized void closeLink() {
            open = false;
            frames.clear();
            notifyAll();
        }

        synchronized boolean isOpen() {
            return open && !closed;
        }

        /** Called from the capture thread only. Assigns seq and enqueues one frame. */
        synchronized void offerFrame(short[] samples) {
            if (!open || closed) {
                return;
            }
            byte[] record = Wire.audioRecord(nextSeq, samples);
            if (frames.size() >= QUEUE_FRAMES) {
                int lostSeq = nextSeq - frames.size(); // the oldest queued frame
                frames.pollFirst();
                dropped++;
                Log.w(TAG, "queue full: dropped seq " + lostSeq + " (total dropped " + dropped
                        + "); the backend will count one seq gap");
            }
            frames.addLast(record);
            nextSeq++;
            notifyAll();
        }

        /** Events (playback_done, rssi) jump the queue: they are latency-sensitive. */
        synchronized void offerEvent(byte[] record) {
            if (closed) {
                return;
            }
            events.addLast(record);
            notifyAll();
        }

        /** Blocks until there is something to send, or returns null when the link is down. */
        synchronized byte[] take() throws InterruptedException {
            while (!closed && open && frames.isEmpty() && events.isEmpty()) {
                wait(500);
            }
            if (closed) {
                return null;
            }
            if (!events.isEmpty()) {
                return events.pollFirst();
            }
            return frames.pollFirst();
        }

        synchronized void shutdown() {
            closed = true;
            open = false;
            frames.clear();
            events.clear();
            notifyAll();
        }
    }
}
