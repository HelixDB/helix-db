// #########################################################
//                         Story
// #########################################################
QUERY insert_story_Cluster1 (
    uuid: String,
    username: String,
    title: String,
    text: String,
    created_at: Date,
    url: String,
    score: I64,
    klive: I64
) =>
    story <- AddN<Story_Cluster1>({
        uuid: uuid,
        username: username,
        title: title,
        text: text,
        created_at: created_at,
        url: url,
        score: score,
        klive: klive
    })
    RETURN story

QUERY get_all_stories_Cluster1 () =>
    stories <- N<Story_Cluster1>
    RETURN stories

QUERY get_story_by_uuid_Cluster1 (
    uuid: String
) =>
    story <- N<Story_Cluster1>({uuid: uuid})
    RETURN story

// #########################################################
//                         Comment
// #########################################################

QUERY insert_comment_Cluster1 (
    uuid: String,
    username: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
) =>
    comment <- AddN<Comment_Cluster1>({
        uuid: uuid,
        username: username,
        text: text,
        created_at: created_at,
        klive: klive,
        parent_uuid: parent_uuid
    })
    story <- N<Story_Cluster1>({uuid: parent_uuid})
    AddE<Story_to_Comment_Cluster1>()::From(story)::To(comment)
    RETURN comment

QUERY insert_sub_comment_Cluster1 (
    uuid: String,
    username: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
) =>
    comment <- AddN<Comment_Cluster1>({
        uuid: uuid,
        username: username,
        text: text,
        created_at: created_at,
        klive: klive,
        parent_uuid: parent_uuid
    })
    parent_comment <- N<Comment_Cluster1>({uuid: parent_uuid})
    AddE<Comment_to_Comment_Cluster1>()::From(parent_comment)::To(comment)
    RETURN comment

QUERY get_comment_by_uuid_Cluster1 (
    uuid: String
) =>
    comment <- N<Comment_Cluster1>({uuid: uuid})
    RETURN comment

QUERY get_comments_by_story_uuid_Cluster1 (
    story_uuid: String
) =>
    story <- N<Story_Cluster1>({uuid: story_uuid})
    comments <- story::Out<Story_to_Comment_Cluster1>
    RETURN comments

QUERY get_sub_comments_by_parent_uuid_Cluster1 (
    parent_uuid: String
) =>
    comment <- N<Comment_Cluster1>({uuid: parent_uuid})
    comments <- comment::Out<Comment_to_Comment_Cluster1>
    RETURN comments

// #########################################################
//                         Chunk
// #########################################################

QUERY insert_chunk_Cluster1 (
    uuid: String,
    story_uuid: String,
    text: String,
    metadata: String
) =>
    chunk <- AddN<Chunk_Cluster1>({
        uuid: uuid,
        story_uuid: story_uuid,
        text: text,
        metadata: metadata
    })
    story <- N<Story_Cluster1>({uuid: story_uuid})
    AddE<Story_to_Chunk_Cluster1>()::From(story)::To(chunk)
    RETURN chunk

QUERY get_chunk_by_uuid_Cluster1 (
    uuid: String
) =>
    chunk <- N<Chunk_Cluster1>({uuid: uuid})
    RETURN chunk

QUERY get_chunks_by_story_uuid_Cluster1 (
    story_uuid: String
) =>
    story <- N<Story_Cluster1>({uuid: story_uuid})
    chunks <- story::Out<Story_to_Chunk_Cluster1>
    RETURN chunks

// #########################################################
//                         Event
// #########################################################

QUERY insert_event_Cluster1 (
    uuid: String,
    chunk_uuid: String,
    statement: String,
    embedding: [F64],
    triplets: [String],
    statement_type: String,
    temporal_type: String,
    created_at: Date,
    valid_at: Date
) =>
    event <- AddN<Event_Cluster1>({
        uuid: uuid,
        chunk_uuid: chunk_uuid,
        statement: statement,
        triplets: triplets,
        statement_type: statement_type,
        temporal_type: temporal_type,
        created_at: created_at,
        valid_at: valid_at,
    })
    chunk <- N<Chunk_Cluster1>({uuid: chunk_uuid})
    AddE<Chunk_to_Event_Cluster1>()::From(chunk)::To(event)
    vector <- AddV<EventEmbedding_Cluster1>(embedding, {embedding: embedding})
    AddE<Event_to_Embedding_Cluster1>()::From(event)::To(vector)
    RETURN event

QUERY get_event_by_uuid_Cluster1 (
    uuid: String
) =>
    event <- N<Event_Cluster1>({uuid: uuid})
    embedding <- event::Out<Event_to_Embedding_Cluster1>
    RETURN event, embedding

QUERY get_event_chunk_uuid_Cluster1 (
    uuid: String
) =>
    event <- N<Event_Cluster1>({uuid: uuid})
    RETURN event::{chunk_uuid}

QUERY get_all_events_without_embeddings_Cluster1 () =>
    events <- N<Event_Cluster1>
    RETURN events

QUERY get_event_embedding_by_event_uuid_Cluster1 (
    event_uuid: String
) =>
    event <- N<Event_Cluster1>({uuid: event_uuid})
    embedding <- event::Out<Event_to_Embedding_Cluster1>
    RETURN embedding

QUERY update_event_Cluster1 (
    uuid: String,
    chunk_uuid: String,
    statement: String,
    embedding: [F64],
    triplets: [String],
    statement_type: String,
    temporal_type: String,
    created_at: Date,
    valid_at: Date
) =>
    event <- N<Event_Cluster1>({uuid: uuid})::UPDATE({
        statement: statement,
        triplets: triplets,
        statement_type: statement_type,
        temporal_type: temporal_type,
        created_at: created_at,
        valid_at: valid_at
    })
    DROP event::Out<Event_to_Embedding_Cluster1>
    vector <- AddV<EventEmbedding_Cluster1>(embedding, {embedding: embedding})
    AddE<Event_to_Embedding_Cluster1>()::From(event)::To(vector)
    RETURN event


QUERY update_event_chunk_Cluster1 (
    uuid: String,
    chunk_uuid: String
) =>
    event <- N<Event_Cluster1>({uuid: uuid})::UPDATE({
        chunk_uuid: chunk_uuid
    })
    DROP event::InE<Chunk_to_Event_Cluster1>
    chunk <- N<Chunk_Cluster1>({uuid: chunk_uuid})
    AddE<Chunk_to_Event_Cluster1>()::From(chunk)::To(event)
    RETURN event

QUERY invalidate_event_Cluster1 (
    uuid: String,
    invalidated_by: String,
    invalid_at: Date
) =>
    event <- N<Event_Cluster1>({uuid: uuid})::UPDATE({
        invalidated_by: invalidated_by,
        invalid_at: invalid_at
    })
    RETURN event

QUERY expire_event_Cluster1 (
    uuid: String,
    expired_at: Date
) =>
    event <- N<Event_Cluster1>({uuid: uuid})::UPDATE({
        expired_at: expired_at
    })
    RETURN event

QUERY has_events_Cluster1 () =>
    events <- N<Event_Cluster1>::WHERE(_::{statement_type}::EQ("FACT"))
    RETURN events

// #########################################################
//                         Triplet
// #########################################################

QUERY insert_triplet_Cluster1 (
    uuid: String,
    event_uuid: String,
    subject_name: String,
    subject_uuid: String,
    predicate: String,
    object_name: String,
    object_uuid: String,
    value: String,
    created_at: Date
) =>
    triplet <- AddN<Triplet_Cluster1>({
        uuid: uuid,
        event_uuid: event_uuid,
        subject_name: subject_name,
        subject_uuid: subject_uuid,
        predicate: predicate,
        object_name: object_name,
        object_uuid: object_uuid,
        value: value,
        created_at: created_at
    })
    event <- N<Event_Cluster1>({uuid: event_uuid})
    AddE<Event_to_Triplet_Cluster1>()::From(event)::To(triplet)
    subject_entity <- N<Entity_Cluster1>({uuid: subject_uuid})
    object_entity <- N<Entity_Cluster1>({uuid: object_uuid})
    AddE<Triplet_to_Subject_Cluster1>()::From(triplet)::To(subject_entity)
    AddE<Triplet_to_Object_Cluster1>()::From(triplet)::To(object_entity)
    RETURN triplet

QUERY get_triplet_by_uuid_Cluster1 (
    uuid: String
) =>
    triplet <- N<Triplet_Cluster1>({uuid: uuid})
    RETURN triplet

QUERY get_all_triplets_Cluster1 () =>
    triplets <- N<Triplet_Cluster1>
    RETURN triplets

QUERY get_triplets_by_subject_uuid_Cluster1 (
    subject_uuid: String
) =>
    triplets <- N<Triplet_Cluster1>::WHERE(_::{subject_uuid}::EQ(subject_uuid))
    RETURN triplets

QUERY get_triplets_by_object_uuid_Cluster1 (
    object_uuid: String
) =>
    triplets <- N<Triplet_Cluster1>::WHERE(_::{object_uuid}::EQ(object_uuid))
    RETURN triplets

QUERY update_triplet_subject_Cluster1 (
    uuid: String,
    subject_uuid: String
) =>
    triplet <- N<Triplet_Cluster1>({uuid: uuid})::UPDATE({
        subject_uuid: subject_uuid
    })
    subject_entity <- N<Entity_Cluster1>({uuid: subject_uuid})
    DROP triplet::OutE<Triplet_to_Subject_Cluster1>
    AddE<Triplet_to_Subject_Cluster1>()::From(triplet)::To(subject_entity)
    RETURN triplet

QUERY update_triplet_object_Cluster1 (
    uuid: String,
    object_uuid: String
) =>
    triplet <- N<Triplet_Cluster1>({uuid: uuid})::UPDATE({
        object_uuid: object_uuid
    })
    object_entity <- N<Entity_Cluster1>({uuid: object_uuid})
    DROP triplet::OutE<Triplet_to_Object_Cluster1>
    AddE<Triplet_to_Object_Cluster1>()::From(triplet)::To(object_entity)
    RETURN triplet

// #########################################################
//                         Entity
// #########################################################

QUERY insert_entity_Cluster1 (
    uuid: String,
    event_uuid: String,
    name: String,
    entity_type: String,
    description: String,
    created_at: Date
) =>
    entity <- AddN<Entity_Cluster1>({
        uuid: uuid,
        event_uuid: event_uuid,
        name: name,
        entity_type: entity_type,
        description: description,
        created_at: created_at
    })
    event <- N<Event_Cluster1>({uuid: event_uuid})
    AddE<Event_to_Entity_Cluster1>()::From(event)::To(entity)
    RETURN entity

QUERY get_entity_by_uuid_Cluster1 (
    uuid: String
) =>
    entity <- N<Entity_Cluster1>({uuid: uuid})
    RETURN entity

QUERY get_all_entities_Cluster1 () =>
    entities <- N<Entity_Cluster1>
    RETURN entities

QUERY get_entity_by_resolved_id_Cluster1 (
    resolved_id: String
) =>
    entities <- N<Entity_Cluster1>::WHERE(_::{resolved_id}::EQ(resolved_id))
    RETURN entities

QUERY get_triplet_as_subject_Cluster1 (
    uuid: String
) =>
    entity <- N<Entity_Cluster1>({uuid: uuid})
    triplets <- entity::In<Triplet_to_Subject_Cluster1>
    RETURN triplets

QUERY get_triplet_as_object_Cluster1 (
    uuid: String
) =>
    entity <- N<Entity_Cluster1>({uuid: uuid})
    triplets <- entity::In<Triplet_to_Object_Cluster1>
    RETURN triplets

QUERY update_entity_resolved_id_Cluster1 (
    uuid: String,
    resolved_id: String
) =>
    entity <- N<Entity_Cluster1>({uuid: uuid})::UPDATE({
        resolved_id: resolved_id
    })
    resolved_entity <- N<Entity_Cluster1>({uuid: resolved_id})
    DROP entity::OutE<Resolved_Cluster1>
    AddE<Resolved_Cluster1>()::From(entity)::To(resolved_entity)
    RETURN entity

QUERY resolve_entity_Cluster1 (
    uuid: String,
    resolved_id: String
) =>
    new_entity <- N<Entity_Cluster1>({uuid: uuid})::UPDATE({
        resolved_id: resolved_id
    })
    old_entity <- N<Entity_Cluster1>({uuid: resolved_id})
    AddE<Resolved_Cluster1>()::From(old_entity)::To(new_entity)
    RETURN new_entity

QUERY remove_entity_Cluster1 (
    uuid: String
) =>
    DROP N<Entity_Cluster1>({uuid: uuid})
    RETURN "Success"

// #########################################################
//                      Vector Search
// #########################################################

QUERY vector_search_events_Cluster1 (
    query_embedding: [F64],
    k: I32
) =>
    matching_embeddings <- SearchV<EventEmbedding_Cluster1>(query_embedding, k)
    events <- matching_embeddings::In<Event_to_Embedding_Cluster1>
    triplets <- events::Out<Event_to_Triplet_Cluster1>
    entities <- triplets::Out<Triplet_to_Subject_Cluster1>
    chunks <- events::In<Chunk_to_Event_Cluster1>
    RETURN events, triplets, entities, chunks

// #########################################################
//                   BASIC GRAPH TRAVERSAL QUERIES
//                 (For KG agent tools)
// #########################################################

// Find stories that mention a specific entity (as subject)
QUERY get_stories_mentioning_entity_as_subject_Cluster1 (
    entity_uuid: String
) =>
    entity <- N<Entity_Cluster1>({uuid: entity_uuid})
    triplets <- entity::In<Triplet_to_Subject_Cluster1>
    events <- triplets::In<Event_to_Triplet_Cluster1>
    chunks <- events::In<Chunk_to_Event_Cluster1>
    stories <- chunks::In<Story_to_Chunk_Cluster1>
    RETURN stories, chunks, events, triplets

// Find stories that mention a specific entity (as object)  
QUERY get_stories_mentioning_entity_as_object_Cluster1 (
    entity_uuid: String
) =>
    entity <- N<Entity_Cluster1>({uuid: entity_uuid})
    triplets <- entity::In<Triplet_to_Object_Cluster1>
    events <- triplets::In<Event_to_Triplet_Cluster1>
    chunks <- events::In<Chunk_to_Event_Cluster1>
    stories <- chunks::In<Story_to_Chunk_Cluster1>
    RETURN stories, chunks, events, triplets

// Find all entities mentioned in a story
QUERY get_entities_in_story_Cluster1 (
    story_uuid: String
) =>
    story <- N<Story_Cluster1>({uuid: story_uuid})
    chunks <- story::Out<Story_to_Chunk_Cluster1>
    events <- chunks::Out<Chunk_to_Event_Cluster1>
    triplets <- events::Out<Event_to_Triplet_Cluster1>
    subject_entities <- triplets::Out<Triplet_to_Subject_Cluster1>
    object_entities <- triplets::Out<Triplet_to_Object_Cluster1>
    RETURN subject_entities, object_entities, triplets, events, chunks

// Find stories by predicate/relationship type
QUERY get_stories_by_predicate_Cluster1 (
    predicate: String
) =>
    triplets <- N<Triplet_Cluster1>::WHERE(_::{predicate}::EQ(predicate))
    events <- triplets::In<Event_to_Triplet_Cluster1>
    chunks <- events::In<Chunk_to_Event_Cluster1>
    stories <- chunks::In<Story_to_Chunk_Cluster1>
    RETURN stories, triplets, events, chunks

// Search entities by exact name and get connected stories
QUERY search_entity_with_stories_by_name_Cluster1 (
    entity_name: String
) =>
    entities <- N<Entity_Cluster1>::WHERE(_::{name}::EQ(entity_name))
    subject_triplets <- entities::In<Triplet_to_Subject_Cluster1>
    object_triplets <- entities::In<Triplet_to_Object_Cluster1>
    subject_events <- subject_triplets::In<Event_to_Triplet_Cluster1>
    object_events <- object_triplets::In<Event_to_Triplet_Cluster1>
    subject_chunks <- subject_events::In<Chunk_to_Event_Cluster1>
    object_chunks <- object_events::In<Chunk_to_Event_Cluster1>
    subject_stories <- subject_chunks::In<Story_to_Chunk_Cluster1>
    object_stories <- object_chunks::In<Story_to_Chunk_Cluster1>
    RETURN entities, subject_stories, object_stories, subject_triplets, object_triplets

// Find story by title (for getting comments)
QUERY get_story_by_title_Cluster1 (
    title: String
) =>
    stories <- N<Story_Cluster1>::WHERE(_::{title}::EQ(title))
    RETURN stories

QUERY drop_all_tables_Cluster1 () =>
    DROP N<Story_Cluster1>
    DROP N<Comment_Cluster1>
    DROP N<Chunk_Cluster1>
    DROP N<Event_Cluster1>::Out<Event_to_Embedding_Cluster1>
    DROP N<Event_Cluster1>
    DROP N<Triplet_Cluster1>
    DROP N<Entity_Cluster1>
    RETURN "Success"

// #########################################################
// Cluster 2
// #########################################################

// Story operations
QUERY insert_story_Cluster2 (
    uuid: String,
    title: String,
    text: String,
    created_at: Date,
    url: String,
    by: String,
    score: I64,
    klive: I64
) =>
    story <- AddN<Story_Cluster2>({
        uuid: uuid,
        title: title,
        text: text,
        created_at: created_at,
        url: url,
        score: score,
        klive: klive
    })
    user <- N<User_Cluster2>({username: by})
    AddE<User_to_Story_Cluster2>()::From(user)::To(story)
    RETURN story

QUERY get_all_stories_Cluster2 () =>
    stories <- N<Story_Cluster2>
    RETURN stories

QUERY get_story_by_uuid_Cluster2 (
    uuid: String
) =>
    story <- N<Story_Cluster2>({uuid: uuid})
    RETURN story

// Comment operations
QUERY insert_comment_Cluster2 (
    uuid: String,
    username: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
) =>
    comment <- AddN<Comment_Cluster2>({
        uuid: uuid,
        text: text,
        created_at: created_at,
        klive: klive,
        parent_uuid: parent_uuid
    })
    story <- N<Story_Cluster2>({uuid: parent_uuid})
    AddE<Story_to_Comment_Cluster2>()::From(story)::To(comment)
    user <- N<User_Cluster2>({username: username})
    AddE<User_to_Comments_Cluster2>()::From(user)::To(comment)
    RETURN comment

QUERY insert_sub_comment_Cluster2 (
    uuid: String,
    username: String,
    text: String,
    created_at: Date,
    klive: I64,
    parent_uuid: String
) =>
    comment <- AddN<Comment_Cluster2>({
        uuid: uuid,
        text: text,
        created_at: created_at,
        klive: klive,
        parent_uuid: parent_uuid
    })
    parent_comment <- N<Comment_Cluster2>({uuid: parent_uuid})
    AddE<Comment_to_Comment_Cluster2>()::From(parent_comment)::To(comment)
    user <- N<User_Cluster2>({username: username})
    AddE<User_to_Comments_Cluster2>()::From(user)::To(comment)
    RETURN comment

QUERY get_all_comments_Cluster2 () =>
    comments <- N<Comment_Cluster2>
    RETURN comments

QUERY get_comment_by_uuid_Cluster2 (
    uuid: String
) =>
    comment <- N<Comment_Cluster2>({uuid: uuid})
    RETURN comment

QUERY get_comments_by_story_uuid_Cluster2 (
    story_uuid: String
) =>
    story <- N<Story_Cluster2>({uuid: story_uuid})
    comments <- story::Out<Story_to_Comment_Cluster2>
    RETURN comments

QUERY get_sub_comments_by_parent_uuid_Cluster2 (
    parent_uuid: String
) =>
    comment <- N<Comment_Cluster2>({uuid: parent_uuid})
    sub_comments <- comment::Out<Comment_to_Comment_Cluster2>
    RETURN sub_comments

// Story Embedding operations
QUERY add_story_embedding_Cluster2 (
    story_uuid: String,
    embedding: [F64],
    content: String
) =>
    story <- N<Story_Cluster2>({uuid: story_uuid})
    vector <- AddV<StoryEmbedding_Cluster2>(embedding, {content: content})
    AddE<Story_to_StoryEmbedding_Cluster2>()::From(story)::To(vector)
    RETURN story

// Comment Embedding operations
QUERY add_comment_embedding_Cluster2 (
    comment_uuid: String,
    embedding: [F64],
    content: String
) =>
    comment <- N<Comment_Cluster2>({uuid: comment_uuid})
    vector <- AddV<CommentEmbedding_Cluster2>(embedding, {content: content})
    AddE<Comment_to_CommentEmbedding_Cluster2>()::From(comment)::To(vector)
    RETURN comment

QUERY search_similar_stories_Cluster2 (
    query_embedding: [F64],
    k: I64
) =>
    matching_embeddings <- SearchV<StoryEmbedding_Cluster2>(query_embedding, k)
    stories <- matching_embeddings::In<Story_to_StoryEmbedding_Cluster2>
    RETURN stories

QUERY insert_user_Cluster2 (
    username: String
) =>
    user <- AddN<User_Cluster2>({
        username: username
    })
    RETURN user

QUERY get_user_by_username_Cluster2 (
    username: String
) =>
    user <- N<User_Cluster2>({username: username})
    RETURN user

QUERY get_all_users_Cluster2 () =>
    users <- N<User_Cluster2>
    RETURN users

QUERY connect_user_to_story_Cluster2 (
    username: String,
    story_uuid: String
) =>
    user <- N<User_Cluster2>({username: username})
    story <- N<Story_Cluster2>({uuid: story_uuid})
    edge <- AddE<User_to_Story_Cluster2>::From(user)::To(story)
    RETURN edge

QUERY connect_user_to_comment_Cluster2 (
    username: String,
    comment_uuid: String
) =>
    user <- N<User_Cluster2>({username: username})
    comment <- N<Comment_Cluster2>({uuid: comment_uuid})
    edge <- AddE<User_to_Comments_Cluster2>::From(user)::To(comment)
    RETURN edge

QUERY drop_all_comments_Cluster2 (k: I32) =>
    DROP N<Comment_Cluster2>::RANGE(0, k)
    RETURN "Success"

QUERY drop_all_users_Cluster2 () =>
    DROP N<User_Cluster2>
    RETURN "Success"

QUERY drop_all_story_and_embeddings_Cluster2 () =>
    DROP N<Story_Cluster2>::Out<Story_to_StoryEmbedding_Cluster2>
    DROP N<Story_Cluster2>
    RETURN "Success"

QUERY count_all_stories_Cluster2 () =>
    stories <- N<Story_Cluster2>::COUNT
    RETURN stories

QUERY drop_all_Cluster2 () =>
    DROP N<User_Cluster2>
    DROP N<Story_Cluster2>::Out<Story_to_StoryEmbedding_Cluster2>
    DROP N<Story_Cluster2>::Out<Story_to_Comment_Cluster2>
    DROP N<Story_Cluster2>
    DROP N<Comment_Cluster2>::Out<Comment_to_CommentEmbedding_Cluster2>
    DROP N<Comment_Cluster2>
    RETURN "Success"