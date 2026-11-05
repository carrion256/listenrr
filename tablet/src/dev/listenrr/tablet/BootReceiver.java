package dev.listenrr.tablet;

import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.util.Log;

/**
 * Brings the source back after the tablet reboots.
 *
 * <p>Android 14+ refuses to start a microphone foreground service from a boot receiver, so this
 * does not try: it opens {@link MainActivity}, which is a visible window and may start the
 * service (the path that works by hand). A background activity start is blocked too, unless the
 * app holds "display over other apps", which the install recipe grants over adb.
 */
public final class BootReceiver extends BroadcastReceiver {
    private static final String TAG = "listenrr";

    @Override
    public void onReceive(Context context, Intent intent) {
        if (intent == null || !Intent.ACTION_BOOT_COMPLETED.equals(intent.getAction())) {
            return;
        }
        try {
            context.startActivity(new Intent(context, MainActivity.class)
                    .addFlags(Intent.FLAG_ACTIVITY_NEW_TASK));
            Log.i(TAG, "boot completed: opened the activity to start the source");
        } catch (RuntimeException e) {
            Log.e(TAG, "boot completed, but the activity could not be opened: " + e, e);
        }
    }
}
