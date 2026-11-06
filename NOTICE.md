# Third-party components

listenrr's own code is dual licensed under Apache-2.0 or MIT (see `LICENSE-APACHE` and
`LICENSE-MIT`). It builds on, downloads or is designed to work with the projects below. They keep
their own licences. Check each one yourself before you redistribute anything.

| Component | How it is used | Licence (as published upstream) |
|---|---|---|
| [sherpa-onnx](https://github.com/k2-fsa/sherpa-onnx) | Rust crate `sherpa-onnx`: keyword spotting and VAD runtime. A prebuilt static library is downloaded at build time | Apache-2.0 |
| `sherpa-onnx-kws-zipformer-gigaspeech-3.3M-2024-01-01` | Keyword spotter model, fetched by `scripts/get_models.sh`, not stored in this repository | Apache-2.0 per the upstream release. Confirm on the model page |
| [Silero VAD](https://github.com/snakers4/silero-vad) | Voice activity detection model, fetched by `scripts/get_models.sh` | MIT |
| [wyoming-onnx-stt](https://github.com/lmoe/wyoming-onnx-stt) | An ASR server listenrr talks to over the Wyoming protocol. Not included | See that repository |
| NVIDIA Parakeet TDT 0.6b-v3 | The ASR model it was developed against. Not included | CC-BY-4.0 per the model card. Confirm before redistributing |
| [ESPHome](https://esphome.io) | `firmware/components/luna_stream` is an ESPHome external component and is compiled into an ESPHome firmware image | ESPHome publishes its C++ runtime under GPL-3.0 and its Python tooling under MIT. If you distribute firmware images you build, read ESPHome's licence terms |
| [esphome-waveshare-esp32-s3-audio-va](https://github.com/MichalZaniewicz/esphome-waveshare-esp32-s3-audio-va) | `config/satellite.example.yaml` takes its pin map, I2S layout and codec settings from this package, which is MIT licensed | MIT. Copyright its author |
| [Home Assistant](https://www.home-assistant.io) | The control plane listenrr connects to. Not included | Apache-2.0 |
| [Hermes Agent](https://github.com/NousResearch/hermes-agent) | The conversation agent the author's Home Assistant pipeline calls. Not included, and listenrr does not depend on it | See that repository |
| [ElevenLabs](https://elevenlabs.io) | The TTS voice in the author's pipeline, through a Home Assistant TTS entity. A hosted service, not included | Commercial service, see its terms |

The Rust dependencies are listed in `Cargo.toml` and `Cargo.lock`, each under its own licence.
