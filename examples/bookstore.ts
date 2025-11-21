import hx from "helix";

const schema = hx.schema();

const Chapter = schema.defineNode("Chapter", {
  index: I64,
});

schema.index(Chapter.index, { unique: true });

const SubChapter = schema.defineNode("SubChapter", {
  title: hx.String,
  content: hx.String,

  embedding: SubChapterEmbedding,
});

const SubChapterEmbedding = schema.defineVector("SubChapterEmbedding", {
  dimensions: 1536,
  hnsw: hx.cosine,
});

const Contains = schema.defineEdge("Contains", {
  from: Chapter,
  to: SubChapter,
});

const ArgChapter = hx.Struct({
  id: hx.I64,
  subchapters: hx.List(ArgSubchapter),
});

const ArgSubChapter = hx.Struct({
  title: hx.String,
  content: hx.String,
  chunk: hx.Vector,
});

const loadDocsRag = schema.query("loaddocs_rag", {
  arguments: [hx.List(ArgChapter)],
  returns: hx.String,
}, (db, [chapters]) => {
  chapters.forEach((c) => {
    const cNode = db.addNode(Chapter({ index: c.id }));

    c.subchapters.forEach((sc) => {
      const scNode = db.addNode(SubChapter({
        title: sc.title,
        content: sc.content,
        embedding: SubChapterEmbedding(sc.chunk),
      }));

      db.addEdge(Contains({ from: cNode, to: scNode }));
    });
  });

  return "Success";
});

const edgeNode = schema.query("edge_node", {
  arguments: [],
  returns: hx.Iterator(Contains),
}, (db, []) => {
  return db.nodes[Chapter].outgoingEdges[Contains];
});

const edgeNodeId = schema.query("edge_node_id", {
  arguments: [hx.Id(Chapter)],
  returns: hx.Iterator(Contains),
}, (db, [id]) => {
  return db.nodes[Chapter]({ id }).outgoingEdges[Contains];
});
