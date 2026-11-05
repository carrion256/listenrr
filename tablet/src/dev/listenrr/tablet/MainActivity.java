package dev.listenrr.tablet;

import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;
import android.widget.LinearLayout;
import android.widget.TextView;

/**
 * The whole UI: one line of status, then get out of the way.
 *
 * <p>Its job is to own the runtime permission request - Android will not hand the microphone to
 * a service started from the background - and to start the foreground service while the app is
 * visible. Then it finishes so FreeKiosk comes back to the foreground.
 */
public final class MainActivity extends Activity {
    private static final String TAG = "listenrr";
    private static final int REQ_PERMISSIONS = 7;

    private TextView status;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        LinearLayout layout = new LinearLayout(this);
        layout.setOrientation(LinearLayout.VERTICAL);
        layout.setPadding(48, 96, 48, 48);
        status = new TextView(this);
        status.setTextSize(16f);
        layout.addView(status);
        setContentView(layout);

        setStatus("source '" + Config.SOURCE_ID + "' -> " + Config.BACKEND_HOST + ":"
                + Config.BACKEND_PORT);

        String[] needed = requiredPermissions();
        if (needed.length == 0) {
            startSource();
            return;
        }
        Log.i(TAG, "requesting " + needed.length + " permission(s)");
        try {
            requestPermissions(needed, REQ_PERMISSIONS);
        } catch (RuntimeException e) {
            setStatus("permission request failed: " + e);
            finish();
        }
    }

    @Override
    public void onRequestPermissionsResult(int requestCode, String[] permissions, int[] grants) {
        if (requestCode != REQ_PERMISSIONS) {
            return;
        }
        boolean mic = false;
        for (int i = 0; i < permissions.length; i++) {
            if (android.Manifest.permission.RECORD_AUDIO.equals(permissions[i])) {
                mic = grants[i] == PackageManager.PERMISSION_GRANTED;
            } else if (grants[i] != PackageManager.PERMISSION_GRANTED) {
                // POST_NOTIFICATIONS is not required to run; it only decides whether the
                // ongoing notification is visible on Android 13+.
                Log.i(TAG, permissions[i] + " not granted; continuing without it");
            }
        }
        if (mic) {
            startSource();
        } else {
            setStatus("microphone permission denied - the tablet cannot be a source. "
                    + "Grant it in Settings > Apps > listenrr tablet.");
            finish();
        }
    }

    /** RECORD_AUDIO always; POST_NOTIFICATIONS from Android 13, but never waited on. */
    private String[] requiredPermissions() {
        boolean needMic = checkSelfPermission(android.Manifest.permission.RECORD_AUDIO)
                != PackageManager.PERMISSION_GRANTED;
        boolean needNotif = Build.VERSION.SDK_INT >= 33
                && checkSelfPermission(android.Manifest.permission.POST_NOTIFICATIONS)
                        != PackageManager.PERMISSION_GRANTED;
        if (needMic && needNotif) {
            return new String[] {android.Manifest.permission.RECORD_AUDIO,
                    android.Manifest.permission.POST_NOTIFICATIONS};
        }
        if (needMic) {
            return new String[] {android.Manifest.permission.RECORD_AUDIO};
        }
        return new String[0]; // microphone already granted; notifications are not blocking
    }

    private void startSource() {
        setStatus("starting the source service...");
        try {
            startForegroundService(new Intent(this, SatelliteService.class));
            setStatus("source '" + Config.SOURCE_ID + "' is running. This window will close.");
            Log.i(TAG, "service started from the activity");
        } catch (RuntimeException e) {
            setStatus("could not start the service: " + e);
            Log.e(TAG, "startForegroundService failed: " + e, e);
        }
        // Leave the kiosk app in charge after a moment long enough to read one line.
        status.postDelayed(new Runnable() {
            public void run() {
                finish();
            }
        }, 1500);
    }

    private void setStatus(String text) {
        Log.i(TAG, "ui: " + text);
        status.setText(text + "\nversion " + Config.FW_VERSION);
    }
}
