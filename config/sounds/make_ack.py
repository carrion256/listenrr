#!/usr/bin/env python3
"""Generate config/sounds/ack.wav — the `ack` earcon played on wake.

Stdlib only (wave/math/struct) so it runs anywhere; regenerating must stay
byte-stable: the WAV is embedded into the satellite's flash at compile time
(media_player `files:`), so a changed file means a reflash for every board.

Sound design (a bedroom device that chimes while you are already speaking):
  * ONE soft bell strike, not a beep pair. The strike note is C4 (261.63 Hz):
    low enough to be warm, high enough for the board's small speaker to
    reproduce (it has almost nothing below ~200 Hz). The earlier version was
    G5 -> C6 sine beeps at 800-1050 Hz, which sat right in the ear's most
    sensitive band and read as shrill.
  * Bell character comes from the partials, not the pitch: the real-bell
    prime, tierce (minor third, 1.2x), quint (1.5x), nominal (2x) and a faint
    inharmonic 2.76x, each decaying exponentially, upper ones faster, so the
    sound opens bright for a few milliseconds and settles into the warm
    fundamental, as a struck bell does.
  * ~0.5 s total with a hard fade to zero. The tail is kept short on purpose:
    the chime plays as the wake word is spoken, the microphone is in the same
    box, and a long ring would sit under the first word of the command.
  * 8 ms raised-cosine attack (no click for the NS4150B to amplify) and a
    raised-cosine fade over the last 80 ms.
  * peak normalised to exactly -12.04 dBFS (0.25 linear): audible at the
    restored volume (0.75) but deliberately quiet — this fires on every wake,
    including at 3am.

Output format is fixed by the board's audio path: 16 kHz mono 16-bit PCM WAV,
the one rate the slave I2S speaker accepts. The media_player pipeline hands
the file to the announcement pipeline untranscoded; a WAV at exactly the
pipeline's rate needs no resampler detour.

Run:  python3 config/sounds/make_ack.py   (writes config/sounds/ack.wav)
"""

import math
import struct
import wave
from pathlib import Path

RATE_HZ = 16000
DURATION_S = 0.500  # a short bell strike, not a ring
PEAK = 0.25  # -12.04 dBFS

STRIKE_HZ = 261.63  # C4

# (ratio to the strike note, relative amplitude, decay time constant in seconds)
PARTIALS = [
    (1.00, 1.00, 0.140),  # prime: the body of the note
    (1.20, 0.55, 0.110),  # tierce (minor third): what makes it a bell, not an organ
    (1.50, 0.35, 0.090),  # quint
    (2.00, 0.45, 0.075),  # nominal
    (2.76, 0.14, 0.040),  # faint inharmonic partial: the metallic "tink" of the strike
]
ATTACK_S = 0.008
FADE_S = 0.080


def edge(t: float) -> float:
    """Raised-cosine attack and a raised-cosine fade to exactly zero at the end."""
    if t < 0.0 or t > DURATION_S:
        return 0.0
    gain = 1.0
    if t < ATTACK_S:
        gain *= 0.5 - 0.5 * math.cos(math.pi * t / ATTACK_S)
    if t > DURATION_S - FADE_S:
        gain *= 0.5 - 0.5 * math.cos(math.pi * (DURATION_S - t) / FADE_S)
    return gain


def main() -> None:
    n = int(RATE_HZ * DURATION_S)
    samples = []
    for i in range(n):
        t = i / RATE_HZ
        value = 0.0
        for ratio, amp, tau in PARTIALS:
            value += amp * math.exp(-t / tau) * math.sin(2.0 * math.pi * STRIKE_HZ * ratio * t)
        samples.append(value * edge(t))

    peak = max(abs(s) for s in samples)
    # Divide first, then quantise: the written file's peak must measure -12 dBFS,
    # not the float buffer's.
    scale = PEAK / peak
    pcm = [int(round(s * scale * 32767.0)) for s in samples]

    out = Path(__file__).with_name("ack.wav")
    with wave.open(str(out), "wb") as w:
        w.setnchannels(1)
        w.setsampwidth(2)
        w.setframerate(RATE_HZ)
        w.writeframes(struct.pack(f"<{len(pcm)}h", *pcm))

    # Self-check: re-read and assert the properties the pipeline depends on.
    with wave.open(str(out), "rb") as w:
        assert w.getnchannels() == 1 and w.getsampwidth() == 2 and w.getframerate() == RATE_HZ
        frames = struct.unpack(f"<{w.getnframes()}h", w.readframes(w.getnframes()))
    measured_dbfs = 20.0 * math.log10(max(abs(f) for f in frames) / 32767.0)
    duration_ms = 1000.0 * len(frames) / RATE_HZ
    assert 0.300 <= duration_ms / 1000.0 <= 0.600, duration_ms
    assert -13.5 <= measured_dbfs <= -11.0, measured_dbfs
    # No click at either edge: first and last samples sit at (essentially) zero.
    assert abs(frames[0]) <= 2 and abs(frames[-1]) <= 2, (frames[0], frames[-1])
    print(
        f"{out}: {RATE_HZ} Hz mono s16, {duration_ms:.0f} ms, "
        f"peak {measured_dbfs:+.2f} dBFS, {out.stat().st_size} bytes"
    )


if __name__ == "__main__":
    main()
