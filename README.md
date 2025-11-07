<div align="center">

<picture>
  <img src="/assets/full_logo.png" alt="HelixDB Logo">
</picture>

<b>HelixDB</b>: an open-source graph-vector database built from scratch in Rust.

<h3>
  <a href="https://helix-db.com">Website</a> |
  <a href="https://docs.helix-db.com">Docs</a> |
  <a href="https://discord.gg/2stgMPr5BD">Discord</a> |
  <a href="https://x.com/hlx_db">X/Twitter</a>
</h3>

[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://docs.helix-db.com)
[![Change Log](https://img.shields.io/badge/changelog-latest-blue)](https://docs.helix-db.com/change-log/helixdb)
[![GitHub Repo stars](https://img.shields.io/github/stars/HelixDB/helix-db)](https://github.com/HelixDB/helix-db/stargazers)
[![Discord](https://img.shields.io/discord/1354148209005559819?logo=discord)](https://discord.gg/2stgMPr5BD)
[![LOC](https://img.shields.io/endpoint?url=https://ghloc.vercel.app/api/HelixDB/helix-db/badge?filter=.rs$,.sh$&style=flat&logoColor=white&label=Lines%20of%20Code)](https://github.com/HelixDB/helix-db)
[![Manta Graph](https://getmanta.ai/api/badges?text=Manta%20Graph&link=helixdb)](https://getmanta.ai/helixdb)

<a href="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai" target="_blank"><img src="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai/upvote_embed.svg" alt="Launch YC: HelixDB - The Database for Intelligence" style="margin-left: 12px;"/></a>

</div>

<hr>

HelixDB is a database that makes it easy to build all the components needed for an AI application in a single platform.

You no longer need a separate application DB, vector DB, graph DB, or application layers to manage the multiple storage locations to build the backend of any application that uses AI, agents or RAG. Just use Helix.

HelixDB primarily operates with a graph + vector data model, but it can also support KV, documents, and relational data.

## Key Features

|                                  |                                                                                                                                                                               |
| -------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Built-in MCP tools**           | Helix has built-in MCP support to allow your agents to discover data and walk the graph rather than generating human readable queries. |
| **Built-in Embeddings**          | No need to embed your data before sending it to Helix, just use the `Embed` function to vectorize text.                                                     |
| **Tooling for RAG**              | HelixDB has a built-in vector search, keyword search, and graph traversals that can be used to power any type of RAG applications.                                                      |
| **Secure by Default**            | HelixDB is private by default. You can only access your data through your compiled HelixQL queries.                                                                           |
| **Ultra-Low Latency**            | Helix is built in Rust and uses LMDB as its storage engine to provide extremely low latencies.                                                                                |
| **Type-Safe Queries**            | HelixQL is 100% type-safe, which lets you develop and deploy with the confidence that your queries will execute in production                                                                                |

## Getting Started

#### Helix CLI

Start by installing the Helix CLI tool to deploy Helix locally.

1. Install CLI

   ```bash
   curl -sSL "https://install.helix-db.com" | bash
   ```

2. Initialize a project

   ```bash
   mkdir <path-to-project> && cd <path-to-project>
   helix init
   ```

3. Write queries

   Open your newly created `.hx` files and start writing your schema and queries.
   Head over to [our docs](https://docs.helix-db.com/documentation/hql/hql) for more information about writing queries.



   ```js
   N::User {
      INDEX name: String,
      age: U32
   }

   QUERY getUser(user_name: String) =>
      user <- N<User>({name: user_name})
      RETURN user
   ```

4. (Optional) Check your queries compile

   ```bash
   helix check
   ```

5. Deploy your queries to their API endpoints

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

## License

HelixDB is licensed under the The AGPL (Affero General Public License).

## Commercial Support

HelixDB is available as a managed service for selected users, if you're interested in using Helix's managed service or want enterprise support, [contact](mailto:founders@helix-db.com) us for more information and deployment options.

---

Just Use Helix