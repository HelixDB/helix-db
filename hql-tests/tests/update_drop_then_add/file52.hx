QUERY updateEntity (entity_id: ID, name: String, name_embedding: [F32], group_id: String, summary: String, created_at: Date, labels: [String], attributes: String) =>
    entity <- N<Entity>(entity_id)::UPDATE({name: name, group_id: group_id, summary: summary, created_at: created_at, labels: labels, attributes: attributes})
    DROP N<Entity>(entity_id)::Out<Entity_to_Embedding>
    DROP N<Entity>(entity_id)::OutE<Entity_to_Embedding>
    embedding <- AddV<Entity_Embedding>(name_embedding, {name_embedding: name_embedding})
    edge <- AddE<Entity_to_Embedding>({group_id: group_id})::From(entity)::To(embedding)
    RETURN entity
