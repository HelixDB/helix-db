<div align="center">

<img src="./assets/full_logo_dark.png#gh-dark-mode-only" alt="HelixDB Logo">
<img src="./assets/full_logo_light.png#gh-light-mode-only" alt="HelixDB Logo">

<b>HelixDB</b>: a graph-vector database for knowledge graphs and AI memory. Built from scratch in Rust.
<br/><br/>
<a href="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai" target="_blank"><img src="https://www.ycombinator.com/launches/Naz-helixdb-the-database-for-rag-ai/upvote_embed.svg" alt="Launch YC: HelixDB - The Database for Intelligence" style="margin-left: 12px;"/></a>
<h3>
  <a href="https://helix-db.com">website</a> |
  <a href="https://docs.helix-db.com">docs</a> |
  <a href="https://discord.gg/2stgMPr5BD">discord</a> |
  <a href="https://x.com/helixdb">X/twitter</a>
</h3>

[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://docs.helix-db.com)
[![Change Log](https://img.shields.io/badge/changelog-latest-blue)](https://docs.helix-db.com/change-log/helixdb)
[![GitHub Repo stars](https://img.shields.io/github/stars/HelixDB/helix-db)](https://github.com/HelixDB/helix-db/stargazers)
[![Discord](https://img.shields.io/discord/1354148209005559819?logo=discord)](https://discord.gg/2stgMPr5BD)
[![LOC](https://img.shields.io/endpoint?url=https://ghloc.vercel.app/api/HelixDB/helix-db/badge?filter=.rs$,.sh$&style=flat&logoColor=white&label=Lines%20of%20Code)](https://github.com/HelixDB/helix-db)



</div>

<hr>

HelixDB is a database that makes it easy to build all the components needed for AI applications in a single platform.

You don't need a separate application DB, relational DB, vector DB, graph DB, or application layers to manage the multiple storage locations. HelixDB gives your agents federated access to company data, for memory, company brains, and applications.

Helix primarily operates with a graph + vector data model, but it also supports KV, documents, and relational data.

## Getting Started

### 1. Install the CLI

The Helix CLI runs and manages local instances and talks to Helix Cloud.

```bash
curl -sSL "https://install.helix-db.com" | bash
```

Already installed? Update to the latest version with `helix update`.

### 2. The quickest path — `helix chef`

`helix chef` is an interactive, one-shot bootstrapper. It installs the HelixDB query skills and docs MCP, scaffolds a project, starts a local instance, seeds some example data, and writes a `HELIX_CHEF_PROMPT.md`. If a coding agent is available (Claude Code, Codex, or OpenCode), it can hand off and build a working app — frontend and all — from a one-line description of what you want.

```bash
helix chef
```

That's it — no flags. Answer "what do you want to build?" and follow the prompts.

### 3. Manual local setup

If you'd rather wire things up yourself:

1. **Initialize a project.** This scaffolds `helix.toml`, a `.helix/` workspace dir, and a ready-to-run `examples/request.json`.
  ```bash
   mkdir my-helix-app && cd my-helix-app
   helix init
  ```
2. **Start a local instance.** Runs a background container on port `6969` and waits until it accepts queries.
  ```bash
   helix start dev
  ```
  > ⚠️ The default storage mode is **in-memory** — stopping the instance wipes its data. Use `helix start dev --disk` to persist data across restarts, or `--foreground` to stream logs.
3. **Send a query.**
  ```bash
   helix query dev --file examples/request.json
  ```
4. **Stop the instance when you're done.**
  ```bash
   helix stop dev
  ```

## Writing queries with the SDKs

Queries are authored with the Rust, TypeScript, Go, or Python DSL and sent straight to a running instance as dynamic requests against `POST /v1/query` — no build or deploy step. The SDKs produce the same JSON AST. The examples below talk to a local instance on `http://localhost:6969` (the default `helix start dev` port). See the [Querying Guide](https://docs.helix-db.com/database/querying-guide/overview) for the full builder catalog and the dynamic-query wire format.

### Rust

Install the crate (published as `helix-db`, imported as `helix_db`):

```bash
cargo init && cargo add helix-db tokio sonic-rs
```

Define your queries as `#[register]` functions, then run them directly through the client:

```rust
use helix_db::Client;
use helix_db::dsl::prelude::*;

#[register]
pub fn add_user(name: String) {
    write_batch()
        .var_as(
            "user",
            g().add_n("User", vec![("name", name)])
                .value_map(None::<Vec<String>>),
        )
        .returning(["user"])
}

#[register]
pub fn get_user(name: String) {
    read_batch()
        .var_as(
            "user",
            g().n_with_label("User")
                .where_(Predicate::eq("name", name))
                .value_map(None::<Vec<String>>),
        )
        .returning(["user"])
}

#[tokio::main]
async fn main() {
    let client = Client::new(None).unwrap(); // defaults to http://localhost:6969

    // add user
    let new_user = client
        .query::<sonic_rs::Value>()
        .dynamic(add_user("John Doe".to_string()))
        .send()
        .await
        .unwrap();
    println!("new user: {:#}", sonic_rs::to_string_pretty(&new_user).unwrap());

    // get user
    let user = client
        .query::<sonic_rs::Value>()
        .dynamic(get_user("John Doe".to_string()))
        .send()
        .await
        .unwrap();
    println!("user: {:#}", sonic_rs::to_string_pretty(&user).unwrap());
}
```

### TypeScript

Install the package (Node.js 20+):

```bash
npm init -y && npm install @helix-db/helix-db
```

Define your queries as functions, then `POST` them to the running instance:

```ts
import {
  Predicate, PropertyInput, PropertyProjection,
  defineParams, g, param, readBatch, writeBatch,
} from "@helix-db/helix-db";

const addUserParams = defineParams({ name: param.string() });
function addUser(p = addUserParams) {
  return writeBatch()
    .varAs("user",
      g().addN("User", { name: PropertyInput.param("name") })
        .project([PropertyProjection.new("name")]),
    )
    .returning(["user"]);
}

const getUserParams = defineParams({ name: param.string() });
function getUser(p = getUserParams) {
  return readBatch()
    .varAs("user",
      g().nWithLabel("User")
        .where(Predicate.eqParam("name", "name"))
        .project([PropertyProjection.new("name")]),
    )
    .returning(["user"]);
}

const HELIX_URL = "http://localhost:6969/v1/query";

// add user
const newUser = await fetch(HELIX_URL, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: addUser().toDynamicJson(addUserParams, { name: "John Doe" }),
}).then((r) => r.json());
console.log("new user:", newUser);

// get user
const user = await fetch(HELIX_URL, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: getUser().toDynamicJson(getUserParams, { name: "John Doe" }),
}).then((r) => r.json());
console.log("user:", user);
```

### Python

Install the package from this repository:

```bash
pip install -e sdks/python
```

Build dynamic requests with snake_case builders, then send them with the client:

```python
from helixdb import Client, Predicate, g, param, define_params, read_batch, write_batch

add_user_params = define_params({"name": param.string()})
add_user = (
    write_batch()
    .var_as("user", g().add_n("User", {"name": add_user_params.name}))
    .returning(["user"])
)

get_user_params = define_params({"name": param.string()})
get_user = (
    read_batch()
    .var_as(
        "user",
        g()
        .n_with_label("User")
        .where(Predicate.eq("name", get_user_params.name))
        .value_map(["name"]),
    )
    .returning(["user"])
)

client = Client("http://localhost:6969")

new_user = client.query().dynamic(
    add_user.to_dynamic_request(add_user_params, {"name": "John Doe"})
).send()
print("new user:", new_user)

user = client.query().dynamic(
    get_user.to_dynamic_request(get_user_params, {"name": "John Doe"})
).send()
print("user:", user)
```

## HelixDB Cloud

HelixDB Cloud is an object-storage-backed deployment with integrated vector and full-text search, full ACID transactions, a single writer with auto-scaling reader nodes, and high availability (3+ gateways and DB nodes). Cloud clusters use a separate deploy path from local instances:

```bash
helix auth login                                  # authenticate
helix workspace switch <workspace>                # select workspace + project
helix project switch <project>
helix init cloud --cluster-id <cluster-id>        # or: helix add cloud --name production --cluster-id <id>
helix sync production                             # pull gateway URL + auth contract into helix.toml
helix query production --file examples/request.json
```

## Commercial Support

### HelixDB Cloud

HelixDB is available as a distributed, high-availability, managed service. If you're interested in using Helix's managed service, go to [our website](https://helix-db.com/login) to get started or [contact us](mailto:founders@helix-db.com) to talk with a founder.

## Docs & Community

- 📚 [Documentation](https://docs.helix-db.com) · [Querying Guide](https://docs.helix-db.com/database/querying-guide/overview)
- 💬 [Discord](https://discord.gg/2stgMPr5BD)
- 🐦 [X / Twitter](https://x.com/helixdb)

---

Just Use Helix.
