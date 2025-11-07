N::Story_Cluster1 {
    INDEX uuid: String,
    username: String,
    title: String,
    text: String,
    created_at: Date,
    url: String,
    score: I64,
    klive: I64
}

E::Story_to_Comment_Cluster1 {
    From: Story_Cluster1,
    To: Comment_Cluster1,
    Properties: {
        story_uuid: String,
        comment_uuid: String
    }
}

N::Comment_Cluster1 {
    INDEX uuid: String,
    username: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
}

E::Comment_to_Comment_Cluster1 {
    From: Comment_Cluster1,
    To: Comment_Cluster1,
    Properties: {
    }
}

E::Story_to_Chunk_Cluster1 {
    From: Story_Cluster1,
    To: Chunk_Cluster1,
    Properties: {
    }
}

N::Chunk_Cluster1 {
    INDEX uuid: String,
    story_uuid: String,
    text: String,
    metadata: String
}

E::Chunk_to_Event_Cluster1 {
    From: Chunk_Cluster1,
    To: Event_Cluster1,
    Properties: {
    }
}

N::Event_Cluster1 {
    INDEX uuid: String,
    chunk_uuid: String,
    statement: String,
    triplets: [String],
    statement_type: String,
    temporal_type: String,
    created_at: Date,
    valid_at: Date,
    expired_at: Date,
    invalid_at: Date,
    invalidated_by: String,
}

E::Event_to_Embedding_Cluster1 {
    From: Event_Cluster1,
    To: EventEmbedding_Cluster1,
    Properties: {
    }
}

V::EventEmbedding_Cluster1 {
    embedding: [F64]
}


E::Invalidated_By_Cluster1 {
    From: Event_Cluster1,
    To: Event_Cluster1,
    Properties: {
    }
}

E::Event_to_Triplet_Cluster1 {
    From: Event_Cluster1,
    To: Triplet_Cluster1,
    Properties: {
    }
}

E::Event_to_Entity_Cluster1 {
    From: Event_Cluster1,
    To: Entity_Cluster1,
    Properties: {
    }
}

N::Triplet_Cluster1 {
    INDEX uuid: String,
    event_uuid: String,
    subject_name: String,
    subject_uuid: String,
    predicate: String,
    object_name: String,
    object_uuid: String,
    value: String,
    created_at: Date DEFAULT NOW
}

E::Triplet_to_Subject_Cluster1 {
    From: Triplet_Cluster1,
    To: Entity_Cluster1,
    Properties: {
    }
}

E::Triplet_to_Object_Cluster1 {
    From: Triplet_Cluster1,
    To: Entity_Cluster1,
    Properties: {
    }
}

N::Entity_Cluster1 {
    INDEX uuid: String,
    event_uuid: String,
    name: String,
    entity_type: String,
    description: String,
    resolved_id: String,
    created_at: Date DEFAULT NOW
}

E::Resolved_Cluster1 {
    From: Entity_Cluster1,
    To: Entity_Cluster1,
    Properties: {
    }
}

// #########################################################
//                  Cluster 2
// #########################################################


N::Story_Cluster2 {
    INDEX uuid: String,
    title: String,
    text: String,
    created_at: Date,
    url: String,
    score: I64,
    klive: I64
}

E::Story_to_Comment_Cluster2 {
    From: Story_Cluster2,
    To: Comment_Cluster2,
    Properties: {
        story_uuid: String,
        comment_uuid: String
    }
}

N::Comment_Cluster2 {
    INDEX uuid: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
}

E::Comment_to_Comment_Cluster2 {
    From: Comment_Cluster2,
    To: Comment_Cluster2,
    Properties: {
    }
}

V::CommentEmbedding_Cluster2 {
    content: String,
}

V::StoryEmbedding_Cluster2 {
    content: String,
}

E::Comment_to_CommentEmbedding_Cluster2 {
    From: Comment_Cluster2,
    To: CommentEmbedding_Cluster2,
    Properties: {
    }
}

E::Story_to_StoryEmbedding_Cluster2 {
    From: Story_Cluster2,
    To: StoryEmbedding_Cluster2,
    Properties: {
    }
}

N::User_Cluster2 {
    INDEX username: String,
    created_at: Date DEFAULT NOW
}

E::User_to_Story_Cluster2 {
    From: User_Cluster2,
    To: Story_Cluster2,
}

E::User_to_Comments_Cluster2 {
    From: User_Cluster2,
    To: Comment_Cluster2,
}
