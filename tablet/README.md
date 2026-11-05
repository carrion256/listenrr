# listenrr tablet

Makes an Android tablet a **source** for the [listenrr](../README.md) voice backend: it streams
its microphone to `listenrr` exactly like the ESP32 boards do, and plays the spoken reply
through its own speaker. Wake word, endpointing, ASR and the Home Assistant pipeline all stay
on the backend.

Plain Java, no AndroidX, no Kotlin, no Gradle: `javac`, `d8`, `aapt2`, `zipalign`, `apksigner`.
The wire format is the one in [`docs/protocol.md`](../docs/protocol.md) (section 13 is the
conformance checklist this app implements); `src/proto.rs` is the contract file.

The backend side is opt-in per source: the source's `[[sources]]` entry needs
`playback = "source"` and `has_speaker = true`, which is what makes the backend send it a
`play` control instead of asking Home Assistant to play on a `media_player`.

**Stop.** App 0.2.0 also obeys the `stop` control (protocol.md §8): it stops the reply that is
playing and reports `playback_done`, so the conversation ends instead of opening the follow-up
listener. An app older than 0.2.0 does not know the type, skips the record, and carries on
playing the reply to its end — nothing breaks, but "luna stop" does nothing until that tablet is
reinstalled (`just tablet-install <name>`).

## Layout

| File | What it is |
|---|---|
| `src/dev/listenrr/tablet/Wire.java` | The wire format. Pure Java, no `android.*` imports, so the host test can run it. |
| `src/dev/listenrr/tablet/SatelliteService.java` | Foreground service: microphone capture, socket, reconnect, rssi heartbeat. |
| `src/dev/listenrr/tablet/Player.java` | `MediaPlayer` wrapper: `play` + `playback_done`, the ack chime, and the 0.2.0 `stop`. |
| `src/dev/listenrr/tablet/MainActivity.java` | The only UI: one line of status, the permission request, then `finish()`. |
| `AndroidManifest.xml` | Permissions, the microphone foreground-service type, cleartext HTTP. |
| `test/WireTest.java` | Golden-vector conformance for `Wire.java`, plain `main`. |
| `build.sh` | `--test` and APK build. No network, no Gradle. |
| `VERSION` | `fw` in the hello, `versionName`, and the derived `versionCode`. |

`build/` is generated and git-ignored, including `build/<name>/gen/.../Config.java`, which
`build.sh` writes from the shared satellite config. It holds the hello token, so it must never
be committed.

## Build and test

```sh
just tablet-test              # or: bash tablet/build.sh --test
just tablet-build office-tab  # or: bash tablet/build.sh office-tab
just tablet-install office-tab
```

The justfile recipes read the `[[tablets]]` entries in `config/satellites.toml`
(`name`, `source_id`, `room`, `host`) plus the top-level `backend_host` / `backend_port`, and
`luna_token` from `config/secrets.yaml`. They are deliberately not `[[satellites]]` entries:
the firmware recipes would try to flash an ESPHome image onto a tablet.

To build against a different config than your own fleet file — a dry run, CI:

```sh
LISTENRR_SATELLITES_TOML=/tmp/sats.toml LISTENRR_SECRETS_YAML=/tmp/secrets.yaml \
  bash tablet/build.sh office-tab
```

Output: `tablet/build/<name>/listenrr-tablet-<name>.apk`, and at the end the `apksigner
verify` output plus an `aapt2 dump badging` summary. A missing toolchain fails the script with
the tool it wanted: set `ANDROID_HOME`, or leave it unset and use the pinned SDK. `build.sh`
borrows `nixpkgs#jdk17` when `javac` is not on `PATH`.

The signing key lives **outside** the repo, at
`${XDG_DATA_HOME:-$HOME/.local/share}/listenrr/tablet.keystore`, and is created on first use
(alias `listenrr`, store and key password `listenrr-tablet`, RSA 2048, 10000 days). That is
what lets a rebuilt APK install over the installed one. Delete it and the next build still
succeeds, but the tablet needs `adb uninstall` first — the app has no state, so nothing is
lost.

## Install and run

```sh
adb -s <serial> install -r tablet/build/office-tab/listenrr-tablet-office-tab.apk
adb -s <serial> shell pm grant dev.listenrr.tablet android.permission.RECORD_AUDIO
adb -s <serial> shell am start -n dev.listenrr.tablet/.MainActivity
```

`am start` is the step that matters: the app shows one line, starts the foreground service, and
closes itself so FreeKiosk is back in front. `pm grant` is not required (the activity asks) but
it means nothing pops up over the kiosk.

After a reboot the app is meant to restart itself, and **that has not been verified on a real
reboot**. Android 14+ does not let a boot receiver start a microphone foreground service, so
`BootReceiver` does not: it opens `MainActivity`, which is a visible window and may start the
service. Android also blocks a background activity start unless the app holds "display over
other apps", which `just tablet-install` grants over adb (`appops set ... SYSTEM_ALERT_WINDOW
allow`). Two things can still defeat it: a lock screen (Android withholds `BOOT_COMPLETED`
until the tablet is unlocked) and a package left in the stopped state (`am force-stop`), which
never receives it. `BOOT_COMPLETED` cannot be faked from adb on a production build, so the only
test is a real restart with the app started normally first.

Watch it:

```sh
adb -s <serial> logcat -s listenrr   # or: just tablet-logs office-tab
adb -s <serial> shell dumpsys activity services dev.listenrr.tablet  # isForeground=true
```

On the backend host, `journalctl -u listenrr -f` should show `source registered` for the tablet's
`source_id`, and the stream-health line should hold roughly 900 frames a minute with
`seq_gaps: 0`. A spoken "luna …" near the tablet should produce `wake accepted`, an endpoint,
a transcript, `ha: tts-end`, `playback started` for that source, and an inbound
`playback_done`.

Useful once, so the tablet is not dozed out of the stream:

```sh
adb -s <serial> shell dumpsys deviceidle whitelist +dev.listenrr.tablet
```

## Known limits

- **No LED.** `led` controls are logged, not shown. An on-screen state needs
  `SYSTEM_ALERT_WINDOW` over the kiosk; deliberately deferred.
- **No echo cancellation** (`has_aec = false`). The backend keeps the source's microphone out
  of the wake detector while it believes the source is speaking, and the follow-up listener
  waits before it starts, so the tablet normally does not wake itself. If it ever does, the
  fallback is `AudioSource.VOICE_COMMUNICATION`.
- **Reboot recovery is untested.** `BootReceiver` is built and installed, but nobody has
  rebooted a tablet and watched the app come back by itself (see "After a reboot"). If it does
  not, open the app once by hand: `am start` above, or from FreeKiosk.
- **A reply longer than the prepare timeout is cut short**: 15 s to fetch and start counts as a
  failure, and the reply is reported done so the group is not left deaf for 30 s.
- **The hello token is in the APK.** The APK is a local, git-ignored artifact; rotate
  `server.auth_token` if one escapes.
