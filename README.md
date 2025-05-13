<div align="center">

<picture>
  <img src="./docs/icon-1.png" alt="HelixDB Logo" width="200" height="200">
</picture>

<h3>
[Homepage](https://helix-db.com) | [Docs](https://docs.helix-db.com) | [Discord](https://discord.gg/2stgMPr5BD) | [X](https://x.com/hlx_db)
</h3>

[![GitHub Repo stars](https://img.shields.io/github/stars/HelixDB/helix-db)](https://github.com/HelixDB/helix-db/stargazers)

<b>HelixDB</b>: an open-source graph-vector database written in Rust built for RAG and AI applications.

</div>

--

HelixDB is a high-performance graph-vector database  designed with a focus on developer experience and performance. Built in Rust and powered by LMDB as its storage engine, it combines the reliability of a proven storage layer with modern features tailored for AI and vector-based applications.

We are currently using LMDB via Heed3, a rust wrapper built by the amazing team over at [Meilisearch](https://github.com/meilisearch/heed).

## Key Features

- **Fast & Efficient**: Built for performance we're currently 1000x faster than Neo4j, 100x faster than TigerGraph and on par with Qdrant for vectors.
- **RAG-First**: Native support for graph and vector data types, making it ideal for RAG (Retrieval Augmented Generation) and AI applications
- **Graph-Vector**: Easiest database for storing relationships between nodes, vectors, or nodes AND vectors.
- **Reliable Storage**: Powered by LMDB (Lightning Memory-Mapped Database) for robust and efficient data persistence
- **ACID Compliant**: Ensures data integrity and consistency

## Getting Started

#### Helix CLI

The Helix CLI tool can be used to check, compile and deploy Helix locally.

1. Install CLI

   ```bash
   curl -sSL "https://install.helix-db.com" | bash
   ```

2. Install Helix

   ```bash
   helix install
   ```

3. Setup

   ```bash
   helix init --path <path-to-project>
   ```

4. Write queries

   Open your newly created `.hx` files and start writing your schema and queries.
   Head over to [our docs](https://docs.helix-db.com/introduction/cookbook/basic) for more information about writing queries
   ```js
   QUERY addUser(name: String, age: Integer) =>
      user <- AddN<User({name: name, age: age})
      RETURN user

   QUERY getUser(user_name: String) =>
      user <- N<User::WHERE(_::{name}::EQ(user_name))
      RETURN user
   ```

6. Check your queries compile before building them into API endpoints (optional)

   ```bash
   # in ./<path-to-project>
   helix check
   ```

7. Deploy your queries

   ```bash
   # in ./<path-to-project>
   helix deploy --local
   ```
8. Start calling them using our [TypeScript SDK](https://github.com/HelixDB/helix-ts) or [Python SDK](https://github.com/HelixDB/helix-py). For example:
   ```typescript
   import HelixDB from "helix-ts";

   // Create a new HelixDB client
   // The default port is 6969
   const client = new HelixDB();

   // Query the database
   await client.query("addUser", {
      name: "John",
      age: 20
   });

   // Get the created user
   const user = await client.query("getUser", {
      user_name: "John"
   });

   console.log(user);
   ```


Other commands:

- `helix instances` to see all your local instances.
- `helix stop <instance-id>` to stop your local instance with specified id.
- `helix stop --all` to stop all your local instances.
- `helix start <instance-id>` to start your local instance with specific id.

## Roadmap

Our current focus areas include:

- Expanding vector data type capabilities for RAG applications
- Enhancing the query language with more robust type checking
- Implementing a test suite to enable end-to-end testing of queries before deployment
- Building a Deterministic Simulation Testing engine enabling us to robustly iterate faster
- Binary quantisation for even better performance

Long term projects:
- In-house graph-vector storage engine (to replace LMDB)
- In-house network protocol & serdes libraries (similar to protobufs/gRPC)

## License

HelixDB is licensed under the The AGPL (Affero General Public License).

## Commercial Support

HelixDB is available as a managed service for selected users, if you're interested in using Helix's managed service or want enterprise support contact us for more information and deployment options.
