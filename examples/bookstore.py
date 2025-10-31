# N::Chapter {
#     chapter_index: I64
# }
# 
# N::SubChapter {
#     title: String,
#     content: String
# }
# 
# E::Contains {
#     From: Chapter,
#     To: SubChapter,
#     Properties: {
#     }
# }
# 
# V::Embedding {
#     chunk: String
# }
# 
# E::EmbeddingOf {
#     From: SubChapter,
#     To: Embedding,
#     Properties: {
#         chunk: String
#     }
# }

# QUERY loaddocs_rag(chapters: [{ id: I64, subchapters: [{ title: String, content: String, chunks: [{chunk: String, vector: [F64]}]}] }]) =>
#     FOR {id, subchapters} IN chapters {
#         chapter_node <- AddN<Chapter>({ chapter_index: id })
#         FOR {title, content, chunks} IN subchapters {
#             subchapter_node <- AddN<SubChapter>({ title: title, content: content })
#             AddE<Contains>::From(chapter_node)::To(subchapter_node)
#             FOR {chunk, vector} IN chunks {
#                 vec <- AddV<Embedding>(vector)
#                 AddE<EmbeddingOf>({chunk: chunk})::From(subchapter_node)::To(vec)
#             }
#         }
#     }
#     RETURN "Success"

# QUERY searchdocs_rag(query: [F64], k: I32) =>
#     vecs <- SearchV<Embedding>(query, k)
#     subchapters <- vecs::In<EmbeddingOf>
#     RETURN subchapters::{title, content}

# QUERY edge_node() => 
#     e <- N<Chapter>::OutE<Contains>
#     RETURN e

# QUERY edge_node_id(id: ID) => 
#     e <- N<Chapter>::OutE<Contains>(id)
#     RETURN e
# 

import helix

db = helix.Db()

class Chapter(db.Node):
    @index
    index: helix.I64

class SubChapter(db.Node):
    title: helix.String
    content: helix.String

    embedding: EmbeddingVector

class EmbeddingVector(db.Vector(dimensions=1536, hnsw=helix.cosine)):
    pass

class Contains(db.Edge[Chapter, SubChapter]):
    pass

class ArgChapter(helix.Struct):
    id: helix.I64
    subchapters: helix.List[ArgSubchapter]

class ArgSubchapter(helix.Struct):
    title: helix.String
    content: helix.String
    chunk: helix.Vector

@db.query
def loaddocs_rag(chapters: helix.List[ArgChapter]) -> str:
    for c in chapters:
        c_node = db.add_node(Chapter(index=c.id))

        for sc in c.subchapters:
            sc_node = db.add_node(SubChapter(
                title=sc.title,
                content=sc.content,
                embedding=EmbeddingVector(sc.chunk)
            ))
            
            db.add_edge(Contains(from=c_node, to=sc_node))

    return "Success"

@db.query
def searchdocs_rag(query: helix.Vector, k: helix.I32) -> helix.Iterator[dict[str, helix.Value]]:
    # TODO
    vecs = db.search_vector(query, k)
    chapters = vecs.incoming_nodes[Contains]
    return chapters.map(lambda c: {"index": c.index})

@db.query
def edge_node() -> helix.Iterator[Contains]:
    return db.nodes[Chapter].outgoing_edges[Contains]

@db.query
def edge_node_id(id: helix.Id[Chapter]) -> helix.Iterator[Contains]:
    return db.nodes[Chapter](id=id).outgoing_edges[Contains]
