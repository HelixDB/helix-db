QUERY upsertPerson(name: String, age: U32) =>
    person <- UpsertN<Person>({name: name, age: age})
    RETURN person


QUERY upsertFriendship(id1: ID, id2: ID) =>
    person1 <- N<Person>(id1)
    person2 <- N<Person>(id2)
    UpsertE<Knows>::From(person1)::To(person2)
    RETURN "done"


QUERY upsertDocEmbed(text: String) =>
    doc <- UpsertV<Document>(Embed(text), {content: text})
    RETURN doc
