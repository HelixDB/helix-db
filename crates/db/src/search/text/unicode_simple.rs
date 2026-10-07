//! Alphanumeric tokenizer that does not split on Unicode combining marks.
//!
//! Tantivy's [`SimpleTokenizer`](tantivy::tokenizer::SimpleTokenizer) ends a token at
//! every `!char::is_alphanumeric()` scalar. Devanagari virama (U+094D) and nukta
//! (U+093C) are nonspacing marks, so क्या is indexed as क + या and a search for
//! क्या false-hits या. This tokenizer keeps marks, ZWJ, and ZWNJ attached to the
//! current alphanumeric run. Punctuation and whitespace still split tokens, so ASCII
//! text matches `SimpleTokenizer`.

use std::str::CharIndices;

use tantivy::tokenizer::{Token, TokenStream, Tokenizer};

use super::unicode_marks::is_unicode_mark;

/// Like Tantivy's simple tokenizer, but Indic conjuncts stay one token.
#[derive(Clone, Default)]
pub struct UnicodeSimpleTokenizer {
    token: Token,
}

/// Token stream produced by [`UnicodeSimpleTokenizer`].
pub struct UnicodeSimpleTokenStream<'a> {
    text: &'a str,
    chars: CharIndices<'a>,
    token: &'a mut Token,
}

impl UnicodeSimpleTokenizer {
    /// A tokenizer whose ASCII splits match Tantivy's `SimpleTokenizer`.
    pub fn new() -> Self {
        Self::default()
    }
}

fn continues_token(c: char) -> bool {
    c.is_alphanumeric()
        || is_unicode_mark(c)
        || matches!(c, '\u{200C}' | '\u{200D}')
}

impl Tokenizer for UnicodeSimpleTokenizer {
    type TokenStream<'a> = UnicodeSimpleTokenStream<'a>;

    fn token_stream<'a>(&'a mut self, text: &'a str) -> UnicodeSimpleTokenStream<'a> {
        self.token.reset();
        UnicodeSimpleTokenStream {
            text,
            chars: text.char_indices(),
            token: &mut self.token,
        }
    }
}

impl UnicodeSimpleTokenStream<'_> {
    fn search_token_end(&mut self) -> usize {
        (&mut self.chars)
            .filter(|(_, c)| !continues_token(*c))
            .map(|(offset, _)| offset)
            .next()
            .unwrap_or(self.text.len())
    }
}

impl TokenStream for UnicodeSimpleTokenStream<'_> {
    fn advance(&mut self) -> bool {
        self.token.text.clear();
        self.token.position = self.token.position.wrapping_add(1);
        while let Some((offset_from, c)) = self.chars.next() {
            if c.is_alphanumeric() {
                let offset_to = self.search_token_end();
                self.token.offset_from = offset_from;
                self.token.offset_to = offset_to;
                self.token.text.push_str(&self.text[offset_from..offset_to]);
                return true;
            }
        }
        false
    }

    fn token(&self) -> &Token {
        self.token
    }

    fn token_mut(&mut self) -> &mut Token {
        self.token
    }
}

#[cfg(test)]
mod tests {
    use super::super::{build_text_analyzer, search_documents, TextDocumentInput};
    use super::*;
    use crate::config::{TextAnalyzerKind, TextIndexDefinition};
    use tantivy::tokenizer::{LowerCaser, SimpleTokenizer, TextAnalyzer};

    fn terms(kind: TextAnalyzerKind, text: &str) -> Vec<String> {
        let mut analyzer = build_text_analyzer(kind);
        let mut stream = analyzer.token_stream(text);
        let mut terms = Vec::new();
        stream.process(&mut |token| terms.push(token.text.clone()));
        terms
    }

    fn simple_terms(text: &str) -> Vec<String> {
        let mut analyzer = TextAnalyzer::builder(SimpleTokenizer::default())
            .filter(LowerCaser)
            .build();
        let mut stream = analyzer.token_stream(text);
        let mut terms = Vec::new();
        stream.process(&mut |token| terms.push(token.text.clone()));
        terms
    }

    #[test]
    fn ascii_tokens_match_tantivy_simple_tokenizer() {
        for text in [
            "Running QUICKLY, 2026-07-24",
            "Hello, happy tax payer!",
            "don't stop",
            "alpha_beta",
            "Alpha-Beta GAMMA",
            "postgamma20260417T154822",
            "",
            "   ...",
        ] {
            assert_eq!(
                terms(TextAnalyzerKind::Standard, text),
                simple_terms(text),
                "{text}"
            );
        }
    }

    #[test]
    fn english_stemmer_still_stems_ascii() {
        assert_eq!(
            terms(
                TextAnalyzerKind::StandardStemEn,
                "Running runners jumped quickly"
            ),
            ["run", "runner", "jump", "quick"]
        );
    }

    #[test]
    fn indic_conjuncts_stay_one_token() {
        // Virama U+094D splits क्या into क + या under SimpleTokenizer.
        assert_eq!(simple_terms("क्या"), ["क", "या"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "क्या"), ["क्या"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "प्यार"), ["प्यार"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "हिन्दी"), ["हिन्दी"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "स्कूल"), ["स्कूल"]);
        // Nukta U+093C, written as scalars so the split does not depend on NFC.
        let zar = "\u{091C}\u{093C}\u{093E}\u{0930}";
        let bazaar = "\u{092C}\u{093E}\u{091C}\u{093C}\u{093E}\u{0930}";
        assert_eq!(simple_terms(zar), ["ज", "ार"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, zar), [zar]);
        assert_eq!(terms(TextAnalyzerKind::Standard, bazaar), [bazaar]);
        assert_eq!(
            terms(TextAnalyzerKind::StandardStemEn, "प्यार"),
            ["प्यार"]
        );
        // Tamil pulli and Bengali hasant are the same class of mark.
        assert_eq!(terms(TextAnalyzerKind::Standard, "தமிழ்"), ["தமிழ்"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "தமிழ்நாடு"), ["தமிழ்நாடு"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "স্বাধীন"), ["স্বাধীন"]);
        // ZWNJ between a virama and the next consonant stays inside the word.
        let zwnj = "\u{0915}\u{094D}\u{200C}\u{0937}";
        assert_eq!(terms(TextAnalyzerKind::Standard, zwnj), [zwnj]);
        // Danda is punctuation, so it still splits.
        assert_eq!(
            terms(TextAnalyzerKind::Standard, "क्या।प्यार"),
            ["क्या", "प्यार"]
        );
    }

    #[test]
    fn indic_token_offsets_cover_the_whole_word() {
        let text = "क्या या";
        let mut tokenizer = UnicodeSimpleTokenizer::new();
        let mut stream = tokenizer.token_stream(text);
        assert!(stream.advance());
        let first = stream.token().clone();
        assert!(stream.advance());
        let second = stream.token().clone();
        assert!(!stream.advance());
        assert_eq!(first.position, 0);
        assert_eq!(&text[first.offset_from..first.offset_to], "क्या");
        assert_eq!(second.position, 1);
        assert_eq!(&text[second.offset_from..second.offset_to], "या");
    }

    #[test]
    fn hindi_bm25_does_not_match_virama_or_nukta_fragments() {
        let documents = vec![
            TextDocumentInput::new(1, "मैंने उसे किताब दी"),
            TextDocumentInput::new(2, "हिन्दी एक भाषा है"),
            TextDocumentInput::new(3, "स्कूल में बच्चे पढ़ते हैं"),
            TextDocumentInput::new(4, "कल हम बाज़ार गए"),
            TextDocumentInput::new(5, "tamil: தமிழ் ஒரு மொழி"),
            TextDocumentInput::new(6, "मेरा यार आज आया"),
            TextDocumentInput::new(7, "चाय या कॉफ़ी"),
            TextDocumentInput::new(8, "मुझे तुमसे प्यार है"),
            TextDocumentInput::new(9, "क्या आप आओगे"),
        ];
        let definition =
            TextIndexDefinition::new_node("Doc", "body").expect("test text definition is valid");

        let ids = |query: &str| {
            search_documents(&definition, &documents, query, 10)
                .expect("search")
                .into_iter()
                .map(|hit| hit.entity_id)
                .collect::<Vec<_>>()
        };

        assert_eq!(ids("क्या"), [9]);
        assert_eq!(ids("प्यार"), [8]);
        assert_eq!(ids("यार"), [6]);
        assert_eq!(ids("हिन्दी"), [2]);
        assert_eq!(ids("स्कूल"), [3]);
        assert_eq!(ids("स"), Vec::<u64>::new());
        assert_eq!(ids("\u{091C}\u{093C}\u{093E}\u{0930}"), Vec::<u64>::new());
        assert_eq!(ids("தமிழ்"), [5]);
        assert_eq!(ids("या"), [7]);
    }

    #[test]
    fn decomposed_latin_accent_stays_on_its_letter() {
        let decomposed = "cafe\u{0301}";
        assert_eq!(simple_terms(decomposed), ["cafe"]);
        assert_eq!(terms(TextAnalyzerKind::Standard, decomposed), [decomposed]);
        assert_eq!(terms(TextAnalyzerKind::Standard, "café"), ["café"]);
    }
}
