N::Entity {
    name: String,
    group_id: String,
    labels: [String],
    created_at: Date DEFAULT NOW,
    summary: String,
    attributes: String
}

E::Entity_to_Embedding {
    From: Entity,
    To: Entity_Embedding,
    Properties: {
        group_id: String
    }
}

V::Entity_Embedding {
    name_embedding: [F32],
}
