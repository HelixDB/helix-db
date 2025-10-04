<div align="center">

<picture>
  <img src="/assets/full_logo.png" alt="HelixDB Logo">
</picture>

<b>HelixDB</b>: a database built from scratch to be the storage backend for any AI application.

<h3>
  <a href="https://helix-db.com">Homepage</a> |
  <a href="https://docs.helix-db.com">Docs</a> |
  <a href="https://discord.gg/2stgMPr5BD">Discord</a> |
  <a href="https://x.com/hlx_db">X</a>
</h3>

[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://docs.helix-db.com)
[![Change Log](https://img.shields.io/badge/changelog-latest-blue)](https://docs.helix-db.com/change-log/helixdb)
[![GitHub Repo stars](https://img.shields.io/github/stars/HelixDB/helix-db)](https://github.com/HelixDB/helix-db/stargazers)
[![Discord](https://img.shields.io/discord/1354148209005559819)](https://discord.gg/2stgMPr5BD)
[![LOC](https://img.shields.io/endpoint?url=https://ghloc.vercel.app/api/HelixDB/helix-db/badge?filter=.rs$,.sh$&style=flat&logoColor=white&label=Lines%20of%20Code)](https://github.com/HelixDB/helix-db)

<a href="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai" target="_blank"><img src="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai/upvote_embed.svg" alt="Launch YC: HelixDB - The Database for Intelligence" style="margin-left: 12px;"/></a>

</div>

<hr>

HelixDB was built on the thesis that current database infrastructure is built for how humans think about data, not AI. So we've built a database that makes it easy to build all the components needed for an AI application in a single platform.

You no longer need a separate application DB, vector DB, graph DB, or application layers to manage the multiple storage locations. All you need to build any application that uses AI, agents or RAG, is a single HelixDB cluster and HelixQL; we take care of the rest.

HelixDB primarily operates with a graph + vector data model, but it can also support support KV, documents, and relational data.

## Key Features

|                                  |                                                                                                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Built-in MCP tools**           | Helix has built-in MCP support to allow your agents to discover data and walk the graph rather than having to generate human readable queries, letting agents actually think. |
| **Built-in Embeddings**          | Don't worry about needing to embed your data before sending it to Helix, just use the `Embed` function to vectorize text.                                                     |
| **Tooling for Knowledge Graphs** | It is super easy to ingest your unstructured data into a knowledge graph, with our integrations for Zep-AI's Graphiti, and our own implementation of OpenAI's KG tool.        |
| **Tooling for RAG**              | HelixDB has a built-in vector search, keyword search, and hybrid search that can be used to power your RAG applications.                                                      |
| **Secure by Default**            | HelixDB is private by default. You can only access your data through your compiled HelixQL queries.                                                                           |
| **Logical Isolation**            | Each Helix cluster is logically isolated in its own VPC meaning only you can ever see your data.                                                                              |
| **Ultra-Low Latency**            | Helix is built in Rust and uses LMDB as its storage engine to provide extremely low latencies.                                                                                |

## Getting Started

#### Helix CLI

The Helix CLI tool can be used to check, compile and deploy Helix locally.

1. Install CLI

   ```bash
   curl -sSL "https://install.helix-db.com" | bash
   ```

2. Setup

   ```bash
   mkdir <path-to-project> && cd <path-to-project>
   helix init
   ```

3. Write queries

   Open your newly created `.hx` files and start writing your schema and queries.
   Head over to [our docs](https://docs.helix-db.com/documentation/getting-started/intro) for more information about writing queries



   ```js
   QUERY addUser(name: String, age: I64) =>
      user <- AddN<User>({name: name, age: age})
      RETURN user

   QUERY getUser(user_name: String) =>
      user <- N<User>::WHERE(_::{name}::EQ(user_name))
      RETURN user
   ```

4. Check your queries compile before building them into API endpoints (optional)

   ```bash
   helix check
   ```

5. Deploy your queries

   ```bash
   helix push dev
   ```

6. Start calling them using our [TypeScript SDK](https://github.com/HelixDB/helix-ts) or [Python SDK](https://github.com/HelixDB/helix-py). For example:

   ```typescript
   import HelixDB from "helix-ts";

   // Create a new HelixDB client
   // The default port is 6969
   const client = new HelixDB();

   // Query the database
   await client.query("addUser", {
     name: "John",
     age: 20,
   });

   // Get the created user
   const user = await client.query("getUser", {
     user_name: "John",
   });

   console.log(user);
   ```

## Roadmap

Our current focus areas include:

- Organizational auth to manage teams, and Helix clusters.
- Improvements to our server code to massively improve network IO performance and scalability.
- Guides and educational content to help you get started with Helix.
- Binary quantisation for even better performance.

Long term projects:

- In-house SOTA knowledge graph ingestion tool for any data source.
- In-house graph-vector storage engine (to replace LMDB)
- In-house network protocol & serdes libraries (similar to protobufs/gRPC)

## License

HelixDB is licensed under the The AGPL (Affero General Public License).

## Commercial Support

HelixDB is available as a managed service for selected users, if you're interested in using Helix's managed service or want enterprise support, [contact](mailto:founders@helix-db.com) us for more information and deployment options.
