package dev.listenrr.tablet;

import android.content.Context;
import android.media.AudioAttributes;
import android.media.MediaPlayer;
import android.os.Handler;
import android.os.HandlerThread;
import android.util.Log;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Plays what the backend asks for.
 *
 * <p>{@code play}: fetch and play a URL with MediaPlayer, then send exactly one
 * {@code playback_done} per play - when it finishes, when it fails, or when a newer play
 * replaces it. docs/protocol.md section 8: the backend keeps the source's microphone out of
 * the wake detector until it hears that event, otherwise it waits out a 30 s timeout, so a
 * missed event costs the household up to 30 s of deafness. Erring towards sending it is the
 * safe direction.
 *
 * <p>{@code earcon}: the bundled ack chime, which sends no {@code playback_done} (it is not
 * a reply).
 *
 * <p>Every MediaPlayer call runs on one private handler thread, so the "exactly once" flag
 * needs no lock.
 */
public final class Player {
    private static final String TAG = "listenrr";

    /** A play needs reporting: called from this class's own thread, at most once per play. */
    public interface Done {
        void onPlaybackDone();
    }

    /** Seconds a reply may spend fetching/preparing before we call it a failure. */
    private static final long PREPARE_TIMEOUT_MS = 15000;

    private final Context context;
    private final Done done;
    private final HandlerThread thread;
    private final Handler handler;

    private MediaPlayer reply;
    private boolean replyPending; // the current play has not reported yet
    private Runnable watchdog;
    private File chimeFile; // ack.wav copied out of assets, once

    public Player(Context context, Done done) {
        this.context = context.getApplicationContext();
        this.done = done;
        this.thread = new HandlerThread("listenrr-play");
        this.thread.start();
        this.handler = new Handler(this.thread.getLooper());
    }

    /** Fetch and play a URL the backend sent. Replaces any reply still running. */
    public void play(final String url, final String format) {
        handler.post(new Runnable() {
            public void run() {
                finishReply(); // reports the replaced play, if it had not reported
                if (url == null || url.isEmpty()) {
                    Log.w(TAG, "play with no url; reporting playback_done");
                    done.onPlaybackDone(); // this play is already over
                    return;
                }
                replyPending = true;
                MediaPlayer mp = new MediaPlayer();
                reply = mp;
                try {
                    AudioAttributes attrs = new AudioAttributes.Builder()
                            .setUsage(AudioAttributes.USAGE_MEDIA)
                            .setContentType(AudioAttributes.CONTENT_TYPE_SPEECH)
                            .build();
                    mp.setAudioAttributes(attrs);
                    mp.setOnPreparedListener(new MediaPlayer.OnPreparedListener() {
                        public void onPrepared(MediaPlayer mp) {
                            cancelWatchdog();
                            Log.i(TAG, "play started (reply)");
                            mp.start();
                        }
                    });
                    mp.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
                        public void onCompletion(MediaPlayer mp) {
                            Log.i(TAG, "play finished, reporting playback_done");
                            finishReply();
                        }
                    });
                    mp.setOnErrorListener(new MediaPlayer.OnErrorListener() {
                        public boolean onError(MediaPlayer mp, int what, int extra) {
                            Log.w(TAG, "play failed what=" + what + " extra=" + extra
                                    + ", reporting playback_done");
                            finishReply();
                            return true;
                        }
                    });
                    Log.i(TAG, "play " + url + " (format=" + format + ")");
                    mp.setDataSource(url);
                    mp.prepareAsync();
                    armWatchdog();
                } catch (Exception e) {
                    // setDataSource/prepare throw for a bad URL, a refused cleartext route or
                    // an unsupported container. The reply is never heard: that is a failure.
                    Log.w(TAG, "play could not start: " + e, e);
                    finishReply();
                }
            }
        });
    }

    /** Play a locally stored acknowledgement sound. Never reports playback_done. */
    public void earcon(final String name) {
        handler.post(new Runnable() {
            public void run() {
                if (!"ack".equals(name)) {
                    Log.i(TAG, "earcon '" + name + "' is not bundled; ignored");
                    return;
                }
                File f = chime();
                if (f == null) {
                    return;
                }
                releaseChime();
                try {
                    MediaPlayer mp = prepareChime(f);
                    if (mp != null) {
                        mp.start();
                    }
                } catch (Exception e) {
                    Log.w(TAG, "earcon failed: " + e);
                    releaseChime();
                }
            }
        });
    }

    /**
     * A spoken "stop": end the reply now. Posted onto the same handler thread as every other
     * MediaPlayer call, so the "exactly once" rule still holds: {@code playback_done} goes out
     * only if a reply was actually pending, and nothing goes out if nothing was playing.
     */
    public void stop() {
        handler.post(new Runnable() {
            public void run() {
                Log.i(TAG, "stop: ending reply");
                finishReply();
            }
        });
    }

    /** Release everything. An unreported reply is abandoned: the socket is going away too. */
    public void close() {
        handler.post(new Runnable() {
            public void run() {
                replyPending = false;
                cancelWatchdog();
                if (reply != null) {
                    reply.release();
                    reply = null;
                }
                releaseChime();
            }
        });
        thread.quitSafely();
    }

    // ------------------------------------------------------------- internals

    /** End the current reply and report it once. Safe to call twice. */
    private void finishReply() {
        cancelWatchdog();
        if (reply != null) {
            try {
                reply.reset();
            } catch (RuntimeException e) {
                Log.w(TAG, "reply reset threw: " + e);
            }
            reply.release();
            reply = null;
        }
        if (replyPending) {
            replyPending = false;
            done.onPlaybackDone();
        }
    }

    private void armWatchdog() {
        cancelWatchdog();
        watchdog = new Runnable() {
            public void run() {
                Log.w(TAG, "play did not become ready in " + PREPARE_TIMEOUT_MS + " ms"
                        + ", reporting playback_done");
                finishReply();
            }
        };
        handler.postDelayed(watchdog, PREPARE_TIMEOUT_MS);
    }

    private void cancelWatchdog() {
        if (watchdog != null) {
            handler.removeCallbacks(watchdog);
            watchdog = null;
        }
    }

    private MediaPlayer prepareChime(File f) {
        MediaPlayer mp = new MediaPlayer();
        try {
            AudioAttributes attrs = new AudioAttributes.Builder()
                    .setUsage(AudioAttributes.USAGE_ASSISTANCE_SONIFICATION)
                    .setContentType(AudioAttributes.CONTENT_TYPE_SONIFICATION)
                    .build();
            mp.setAudioAttributes(attrs);
            mp.setOnCompletionListener(new MediaPlayer.OnCompletionListener() {
                public void onCompletion(MediaPlayer mp) {
                    releaseChime();
                }
            });
            mp.setOnErrorListener(new MediaPlayer.OnErrorListener() {
                public boolean onError(MediaPlayer mp, int what, int extra) {
                    Log.w(TAG, "earcon error what=" + what + " extra=" + extra);
                    releaseChime();
                    return true;
                }
            });
            mp.setDataSource(f.getAbsolutePath());
            mp.prepare();
            return mp;
        } catch (Exception e) {
            Log.w(TAG, "earcon could not be prepared: " + e);
            mp.release();
            return null;
        }
    }

    private void releaseChime() {
        if (chime != null) {
            try {
                chime.release();
            } catch (RuntimeException e) {
                Log.w(TAG, "chime release threw: " + e);
            }
            chime = null;
        }
    }

    private MediaPlayer chime;

    /** assets/ack.wav, copied to the cache dir once: MediaPlayer wants a path or fd. */
    private File chime() {
        if (chimeFile != null && chimeFile.exists()) {
            return chimeFile;
        }
        File out = new File(context.getCacheDir(), "ack.wav");
        InputStream in = null;
        OutputStream os = null;
        try {
            in = context.getAssets().open("ack.wav");
            os = new FileOutputStream(out);
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) {
                os.write(buf, 0, n);
            }
            chimeFile = out;
            return out;
        } catch (Exception e) {
            Log.w(TAG, "bundled chime is unavailable: " + e);
            return null;
        } finally {
            closeQuietly(in);
            closeQuietly(os);
        }
    }

    private static void closeQuietly(java.io.Closeable c) {
        if (c != null) {
            try {
                c.close();
            } catch (Exception ignored) {
                // nothing useful to do on a close failure of a copy buffer
            }
        }
    }
}
