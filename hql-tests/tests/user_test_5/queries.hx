QUERY GetLexicalEntriesByLexiconSlug(lexiconSlug: String) =>
    lexicon <- N<Lexicon>({ slug: lexiconSlug })
    entries <- lexicon::Out<LexiconHasEntry>
    RETURN entries::|e|{
        entryID: e::ID,
        senses: e::Out<EntryHasSense>::{
            senseID: ID,
            ..
        },
        ..
    }


N::Lexicon {
    INDEX slug: String,
}

N::Entry {
}

N::Sense {
}

E::LexiconHasEntry {
    From: Lexicon,
    To: Entry
}

E::EntryHasSense {
    From: Entry,
    To: Sense
}

QUERY get_all_posts() =>
    posts <- N<Post>
    RETURN posts

QUERY search_posts_vec(query: [F32], k: I32) =>
    vecs <- SearchV<Content>(query, k)
    posts <- vecs::In<EmbeddingOf>
    RETURN posts::{subreddit, title, content, url}
