{ pkgs, ... }:
let
  # The pieces `tablet/build.sh` needs to make an APK with no Gradle and no network: build-tools
  # (aapt2, aapt, d8, zipalign, apksigner) and one platform jar (android.jar) to compile against.
  # Proven by building and verifying an APK on 2026-11-03.
  androidSdk =
    (pkgs.androidenv.composeAndroidPackages {
      buildToolsVersions = [ "35.0.0" ];
      platformVersions = [ "34" ];
      includeEmulator = false;
      includeSystemImages = false;
    }).androidsdk;
in
{
  # https://devenv.sh/reference/options/
  #
  # Deliberately NOT a Rust toolchain. rust-toolchain.toml pins 1.99.0 and the host's Nix profile
  # supplies it; a second compiler here would change how the sherpa-onnx build resolves it. Cargo
  # runs under the memory cap through the justfile (`just gate`), which is the supported path.
  packages = [
    # adb: the Android tablets (Office Tab, Hallway Tablet) are installed and debugged over it.
    pkgs.android-tools
    # The repo's everyday commands (`just --list`).
    pkgs.just
    # The justfile's fleet and tablet recipes read config/satellites.toml with tomllib, which
    # needs Python 3.11+.
    pkgs.python3
    # javac / keytool for the tablet app (`tablet/build.sh`).
    pkgs.jdk17
  ];

  # `tablet/build.sh` reads this instead of its built-in fallback path.
  env.ANDROID_HOME = "${androidSdk}/libexec/android-sdk";

  enterShell = ''
    echo "listenrr devshell: $(adb version | head -1), just $(just --version | cut -d' ' -f2), $(javac -version 2>&1)"
  '';
}
