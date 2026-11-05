//! The verify gate: "KWS proposes, transcript disposes" (plan S4.2, principle 1).
//!
//! A loose spotter plus a strict transcript check. Wake-initiated text reaches Home
//! Assistant **only** after this gate passes (plan §3 invariant); a rejection produces a
//! cancelled LED, an Idle session, and a clip + transcript under
//! `clips/false_accept/` ([`crate::clips`]).
//!
//! The rule, exactly as the plan states it:
//! - a token matches when the stripped token (trailing punctuation removed,
//!   case-insensitive) equals one of the configured `kws.wake_words`, or is one of them
//!   followed by an apostrophe (`luna'`, the ASR's rendering of `luna's`),
//! - the **word-boundary** rule is what excludes `lunatic` / `lunacy`,
//! - blind Levenshtein is deliberately *not* used — "tuna" is distance 1 from "luna", and
//!   a fuzzy match there is a false accept, not a win. List the spellings your ASR really
//!   produces for the wake word in `kws.wake_words` instead,
//! - the keyword and adjacent filler ("hey", a trailing comma) are stripped from the text
//!   handed to the pipeline.
//!
//! Follow-up turns (`wake_initiated == false`, S6.1) have no keyword to require: they are
//! accepted as-is apart from filler stripping, and the optional transcript-plausibility
//! check of S6.2 stays off by default.
//!
//! LANE-OWNED SEAM (Lane A, S4.2). Public signatures are frozen — implement the bodies;
//! add only private items. Report a mismatch instead of editing.

/// Filler words stripped when they sit adjacent to the keyword (plan S4.2: "strip keyword
/// + adjacent filler (`hey`, trailing comma)"). Kept deliberately small: over-stripping
///   loses command words.
pub const FILLER_TOKENS: [&str; 1] = ["hey"];

/// Punctuation stripped from a token's edges before comparing (ASR output keeps commas
/// and question marks). The ASCII subset of what [`is_edge_punctuation`] accepts; the
/// apostrophe listed here is the one character that survives stripping, because it carries
/// the possessive form of a wake word.
pub const STRIP_CHARS: &str = ".,!?;:\"'()[]";

/// A rejected transcript, kept for the false-accept clip's sidecar (S8.1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FalseAccept {
    /// The transcript as transcribed, unmodified — so the clip and the text that caused
    /// the rejection always agree.
    pub transcript: String,
}

impl std::fmt::Display for FalseAccept {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "verify gate rejected transcript: {:?}", self.transcript)
    }
}

impl std::error::Error for FalseAccept {}

/// Check a transcript and return the stripped text to hand the Assist pipeline.
///
/// `wake_initiated` is `false` only for a follow-up turn (S6.1), where no keyword is
/// required. On failure the caller emits the LED cancel, returns the session to Idle and
/// writes the clip + transcript to `clips/false_accept/` (S4.2).
///
/// `wake_words` is `kws.wake_words`: with the default list, `luna` / `lunas` / `luna's` /
/// `lunar` / `loona` pass; `tuna`, `saluna`, `"the loon"`, `lunatic`, and a plain command
/// with no keyword fail.
pub fn verify_utterance(
    transcript: &str,
    wake_initiated: bool,
    wake_words: &[String],
) -> Result<String, FalseAccept> {
    let words: Vec<&str> = transcript.split_whitespace().collect();
    let normalized: Vec<String> = words.iter().map(|word| normalize(word)).collect();

    // The closed token set, compared against whole tokens: that *is* the word-boundary
    // rule, so "lunatic" and "saluna" cannot reach the set, and no edit distance is ever
    // consulted. A follow-up (S6.1) has no keyword to require, so nothing is matched and
    // its text is accepted as-is apart from the filler stripped below.
    let keyword_at = if wake_initiated {
        match normalized.iter().position(|word| is_wake_token(word, wake_words)) {
            Some(index) => Some(index),
            None => {
                return Err(FalseAccept {
                    transcript: transcript.to_owned(),
                })
            }
        }
    } else {
        None
    };

    // Everything up to and including the keyword is its context ("hey", "okay,") and goes
    // with it, *unless* nothing meaningful follows the keyword. Then the keyword was the
    // last word of the sentence — "what's the weather, Luna?" — and the words before it
    // are the command (the owner asked for this on 2026-11-03). Which side holds the
    // command is decided by what is *left*, not by guessing at punctuation: ASR does end a
    // question with "?", but "turn the lights off, Luna." ends with a period and "hey Luna"
    // ends with nothing, and position says the same thing in all three.
    let command_start = keyword_at.map_or(0, |index| index + 1);
    let after = command_words(&words, &normalized, command_start..words.len());
    let after_is_nothing = after
        .iter()
        .all(|word| is_trailing_politeness(&normalize(word)));
    let (command, from_before_keyword) = match keyword_at {
        Some(index) if index > 0 && after_is_nothing => {
            let before = command_words(&words, &normalized, 0..index);
            if before.is_empty() {
                (after, false)
            } else {
                (before, true)
            }
        }
        _ => (after, false),
    };

    // Command text is handed on as transcribed; only the keyword and its adjacent filler
    // are removed (plan S4.2), so the pipeline sees the speaker's own words. A command taken
    // from before the keyword also loses the comma that joined it to "Luna" ("what's the
    // weather," + "Luna?" is the question "what's the weather").
    let joined = command.join(" ");
    Ok(if from_before_keyword {
        let mut text = joined
            .trim_end_matches([',', ';', ':', '-', '—', ' '])
            .to_owned();
        // ASR puts the sentence's "?" on its last token, which here is the keyword.
        // Dropping the keyword must not turn a question into a statement.
        if keyword_at.is_some_and(|i| words[i].trim_end().ends_with('?')) && !text.ends_with('?') {
            text.push('?');
        }
        text
    } else {
        joined
    })
}

/// The words of `words[range]` that are command text: bare punctuation tokens dropped, and
/// filler adjacent to the start of the range (a leading "hey") skipped.
fn command_words<'a>(
    words: &[&'a str],
    normalized: &[String],
    range: std::ops::Range<usize>,
) -> Vec<&'a str> {
    let mut command: Vec<&'a str> = Vec::new();
    for (word, normalized) in words[range.clone()].iter().zip(&normalized[range]) {
        if normalized.is_empty() {
            continue; // a bare punctuation token ("—", ",") is not command text
        }
        if command.is_empty() && is_filler(normalized) {
            continue; // filler adjacent to the keyword, or a follow-up's leading "hey"
        }
        command.push(*word);
    }
    command
}

/// Words that may trail the keyword without being the command ("what's the weather, Luna,
/// please"). Private and small for the same reason [`FILLER_TOKENS`] is: stripping more loses
/// command words.
fn is_trailing_politeness(normalized: &str) -> bool {
    matches!(normalized, "please" | "thanks")
}

/// Lowercase a token with its surrounding punctuation removed (S4.2).
///
/// The typographic apostrophes an ASR emits for the possessive — U+2019 `’` and U+02BC
/// `ʼ` — fold to the ASCII form first. Neither is lowercase-mapped, and an internal one
/// is not edge punctuation, so `luna’s` would otherwise not match the possessive form,
/// which is spelled with an ASCII `'`.
fn normalize(token: &str) -> String {
    token
        .replace(['\u{2019}', '\u{02BC}'], "'")
        .trim_matches(is_edge_punctuation)
        .to_lowercase()
}

/// Surrounding punctuation: every character in [`STRIP_CHARS`] except the apostrophe,
/// plus the Unicode punctuation ASR puts around a word (curly quotes, dashes). The
/// apostrophe survives because it is what the possessive form of a wake word matches on.
fn is_edge_punctuation(c: char) -> bool {
    c != '\'' && (STRIP_CHARS.contains(c) || !c.is_alphanumeric())
}

/// A configured wake word as a whole token, or that word followed by an apostrophe
/// (`luna'`, `luna's`; S4.2).
fn is_wake_token(normalized: &str, wake_words: &[String]) -> bool {
    wake_words.iter().any(|word| {
        normalized == word
            || normalized
                .strip_prefix(word.as_str())
                .is_some_and(|rest| rest.starts_with('\''))
    })
}

fn is_filler(normalized: &str) -> bool {
    FILLER_TOKENS.contains(&normalized)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn luna() -> Vec<String> {
        ["luna", "lunar", "loona", "lunas"].map(String::from).to_vec()
    }

    fn verify_utterance(transcript: &str, wake_initiated: bool) -> Result<String, FalseAccept> {
        super::verify_utterance(transcript, wake_initiated, &luna())
    }

    /// AC1's pass rows: every accepted form, bare and in sentence context.
    #[test]
    fn wake_tokens_pass_and_are_stripped() {
        let cases: &[(&str, &str)] = &[
            // bare forms
            ("luna", ""),
            ("lunas", ""),
            ("luna's", ""),
            // V-1: the typographic apostrophes ASR emits for the possessive must fold to
            // the ASCII form — the only apostrophe in the token set.
            ("luna’s", ""),
            ("lunaʼs", ""),
            ("lunar", ""),
            ("loona", ""),
            ("luna'", ""), // the ASR's "luna's" rendering
            // case and surrounding punctuation are noise
            ("Luna", ""),
            ("LUNA.", ""),
            ("luna,", ""),
            ("(loona)", ""),
            ("\"lunas\"!", ""),
            ("luna's?", ""),
            ("“Luna,”", ""),
            // in sentence context: the command survives, the keyword does not
            ("hey luna, turn on the lights", "turn on the lights"),
            ("luna, what's the weather?", "what's the weather?"),
            ("okay, lunar. play some jazz", "play some jazz"),
            ("loona play music", "play music"),
            (
                "Lunas, dim the kitchen to 20 percent",
                "dim the kitchen to 20 percent",
            ),
            ("could you luna turn it down", "turn it down"),
            (
                "luna's — set a timer for 5 minutes",
                "set a timer for 5 minutes",
            ),
            ("luna’s, turn on the lights", "turn on the lights"),
            // filler adjacent to the keyword goes too, however it's punctuated
            ("hey, luna, hey, play music", "play music"),
            ("hey luna hey what time is it", "what time is it"),
            // keyword LAST: nothing follows it, so the sentence before it is the command
            ("what's the weather, luna?", "what's the weather?"),
            ("What's the weather, Luna?", "What's the weather?"),
            ("turn off the lights, luna.", "turn off the lights"),
            ("hey, what time is it luna", "what time is it"),
            ("what's the weather luna please", "what's the weather"),
            ("play some jazz, luna, thanks", "play some jazz"),
        ];
        for (transcript, expected) in cases {
            let stripped = verify_utterance(transcript, true)
                .unwrap_or_else(|rejected| panic!("{transcript:?} should pass, got {rejected:?}"));
            assert_eq!(stripped, *expected, "stripped text for {transcript:?}");
        }
    }

    /// AC1's fail rows, including the near-misses a blind edit distance would accept.
    #[test]
    fn lookalikes_and_plain_commands_are_rejected() {
        let cases: &[&str] = &[
            "tuna", // edit distance 1 from "luna" — must fail
            "Tuna, play music",
            "tuna’s", // the typographic apostrophe does not rescue a near-miss
            "saluna",   // the keyword inside a longer token
            "the loon", // "loon" is not a wake token
            "lunatic",
            "lunacy",
            "luna-tic",           // still one token, not the keyword
            "turn on the lights", // a plain command with no keyword
            "",
            "   ",
            ",",
            "moon over the loon",
            "aluna",
            "lun",
        ];
        for transcript in cases {
            assert!(
                verify_utterance(transcript, true).is_err(),
                "{transcript:?} must be a false accept"
            );
        }
    }

    /// A rejection keeps the transcript verbatim for the false-accept clip sidecar.
    #[test]
    fn false_accept_carries_the_transcript_verbatim() {
        let rejected = verify_utterance("Tuna, play music", true).unwrap_err();
        assert_eq!(rejected.transcript, "Tuna, play music");
        assert!(
            rejected.to_string().contains("Tuna, play music"),
            "the log line quotes the transcript: {rejected}"
        );
    }

    /// The word-boundary rule, isolated: a keyword token passes anywhere in a sentence,
    /// a keyword-shaped substring never does.
    #[test]
    fn word_boundary_is_the_rule_not_edit_distance() {
        assert_eq!(verify_utterance("hey, luna, stop", true).unwrap(), "stop");
        assert!(verify_utterance("hey, lunatic, stop", true).is_err());
        assert!(verify_utterance("the luna's brother", true).is_ok()); // "luna's" is a token
        assert!(verify_utterance("the luna's", true).is_ok());
        assert!(
            verify_utterance("luna'tic", true).is_ok(),
            "starts with luna'"
        );
        assert!(verify_utterance("xluna play", true).is_err());
    }

    /// S6.1: a follow-up has no keyword to require, so the check is skipped and only the
    /// leading filler is stripped.
    #[test]
    fn follow_ups_skip_the_keyword_check() {
        assert_eq!(
            verify_utterance("turn on the lights", false).unwrap(),
            "turn on the lights"
        );
        assert_eq!(
            verify_utterance("what's the weather?", false).unwrap(),
            "what's the weather?"
        );
        assert_eq!(
            verify_utterance("hey, play music", false).unwrap(),
            "play music"
        );
        assert_eq!(
            verify_utterance("hey hey play music", false).unwrap(),
            "play music"
        );
        assert_eq!(
            verify_utterance("tuna", false).unwrap(),
            "tuna",
            "no gate on a follow-up"
        );
        assert_eq!(verify_utterance("", false).unwrap(), "");
        // Nothing to strip on a follow-up: there is no keyword to remove.
        assert_eq!(
            verify_utterance("luna, play music", false).unwrap(),
            "luna, play music"
        );
    }

    /// The gate follows the configured words: another wake word passes, the old one does
    /// not, and the lookalike rules still hold for it.
    #[test]
    fn the_wake_words_are_configuration() {
        let words = ["computer".to_string()];
        let verify = |s: &str| super::verify_utterance(s, true, &words);
        assert_eq!(verify("hey computer, lights on").unwrap(), "lights on");
        assert_eq!(verify("computer's lights on").unwrap(), "lights on");
        assert!(verify("luna, lights on").is_err());
        assert!(verify("computers, lights on").is_err());
        assert!(verify("supercomputer lights on").is_err());
    }

    /// The filler set is closed: widening it silently loses command words.
    #[test]
    fn filler_set_is_the_pinned_one() {
        assert_eq!(FILLER_TOKENS, ["hey"]);
        assert!(!is_wake_token("tuna", &luna()));
        assert!(!is_wake_token("lunaatic", &luna()));
        assert!(!is_filler("okay")); // over-stripping loses command words
    }
}
