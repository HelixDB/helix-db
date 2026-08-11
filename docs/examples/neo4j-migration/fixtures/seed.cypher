MATCH (n) DETACH DELETE n;

CREATE
  (keanu:Person {personId: "p-keanu", name: "Keanu Reeves", born: 1964}),
  (carrie:Person {personId: "p-carrie", name: "Carrie-Anne Moss", born: 1967}),
  (laurence:Person {personId: "p-laurence", name: "Laurence Fishburne", born: 1961}),
  (lana:Person {personId: "p-lana", name: "Lana Wachowski", born: 1965}),
  (joel:Person {personId: "p-joel", name: "Joel Silver", born: 1952}),
  (matrix:Movie {
    movieId: "m-matrix",
    title: "The Matrix",
    released: 1999,
    tagline: "Welcome to the Real World"
  }),
  (reloaded:Movie {
    movieId: "m-matrix-reloaded",
    title: "The Matrix Reloaded",
    released: 2003,
    tagline: "Free your mind"
  }),
  (keanu)-[:ACTED_IN {relationshipId: "r-acted-keanu-matrix", roles: ["Neo"]}]->(matrix),
  (carrie)-[:ACTED_IN {relationshipId: "r-acted-carrie-matrix", roles: ["Trinity"]}]->(matrix),
  (laurence)-[:ACTED_IN {relationshipId: "r-acted-laurence-matrix", roles: ["Morpheus"]}]->(matrix),
  (keanu)-[:ACTED_IN {relationshipId: "r-acted-keanu-reloaded", roles: ["Neo"]}]->(reloaded),
  (carrie)-[:ACTED_IN {relationshipId: "r-acted-carrie-reloaded", roles: ["Trinity"]}]->(reloaded),
  (laurence)-[:ACTED_IN {relationshipId: "r-acted-laurence-reloaded", roles: ["Morpheus"]}]->(reloaded),
  (lana)-[:DIRECTED {relationshipId: "r-directed-lana-matrix"}]->(matrix),
  (lana)-[:DIRECTED {relationshipId: "r-directed-lana-reloaded"}]->(reloaded),
  (joel)-[:PRODUCED {relationshipId: "r-produced-joel-matrix"}]->(matrix),
  (joel)-[:PRODUCED {relationshipId: "r-produced-joel-reloaded"}]->(reloaded);
