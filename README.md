<div align="center">

<img src="./assets/full_logo_dark.png#gh-dark-mode-only" alt="HelixDB Logo">
<img src="./assets/full_logo_light.png#gh-light-mode-only" alt="HelixDB Logo">

<h3>
  <a href="https://helix-db.com">Website</a> |
  <a href="https://docs.helix-db.com">Docs</a> |
  <a href="https://discord.gg/2stgMPr5BD">Discord</a> |
  <a href="https://x.com/helixdb">X</a>
</h3>

[![Docs](https://img.shields.io/badge/docs-latest-blue)](https://docs.helix-db.com)
[![Release notes](https://img.shields.io/badge/changelog-latest-blue)](https://docs.helix-db.com/database/helix-db/start-here/release-notes)
[![GitHub Repo stars](https://img.shields.io/github/stars/HelixDB/helix-db)](https://github.com/HelixDB/helix-db)
[![Discord](https://img.shields.io/discord/1354148209005559819?logo=discord)](https://discord.gg/2stgMPr5BD)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

<hr>

HelixDB is an open-source (Apache 2.0) graph database with native vector search and BM25
full-text search, built in Rust on object storage. Store entities, relationships, embeddings,
and text in one transactional engine and query them together from Rust, TypeScript, Go, or
Python.

| Graph | Vector | Full-text |
| --- | --- | --- |
| Model entities, relationships, and typed properties as a labeled property graph. | Approximate nearest-neighbor search, prefiltered by graph traversal. | BM25 keyword search over node and edge properties. |

## Getting started

Install the CLI. On macOS and Linux:

```bash
curl -sSL "https://install.helix-db.com" | bash
```

On Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/HelixDB/helix-db/main/crates/cli/install.ps1 | iex
```

Create a project, start a local instance (requires Docker or Podman), and run the generated query:

```bash
mkdir my-helix-app && cd my-helix-app
helix init local                               # writes helix.toml and examples/request.json
helix start dev                                # serves http://localhost:6969
helix query dev --file examples/request.json
```

Local data lives in memory by default; `helix start dev --disk` persists it across restarts.
To run HelixDB inside your own process without a server, use
[embedded mode](https://docs.helix-db.com/database/helix-db/start-here/local-development/embedded-database).
The full walkthrough is in the [quickstart](https://docs.helix-db.com/database/helix-db/start-here/quickstart).
Already installed? Run `helix update`.

### Or let an agent build it

`helix chef` installs the HelixDB query skills and docs MCP, scaffolds a project, starts a local
instance, seeds example data, and hands off to the first coding agent it finds, in this order:
Claude Code → OpenAI Codex → OpenCode → Cursor Agent. Describe what you want to build and it
builds a working app, frontend included.

```bash
helix chef
```

## Query from your app

Write queries with an SDK and send them to a running instance through `POST /v2/query`. There is
no build or deploy step, and every SDK produces the same JSON request. (`/v2/` is the wire
endpoint version; the current HelixDB and SDK generation is v3.) The examples below target the
local instance on `http://localhost:6969`. New to the query model? Start with the
[Query walkthrough](https://docs.helix-db.com/database/helix-db/core-concepts/overview).

| SDK | Package | Current release | Setup guide |
|-----|---------|-----------------|-------------|
| Rust | [`helix-db`](https://crates.io/crates/helix-db) | `3.0.0` | [Rust setup](https://docs.helix-db.com/database/helix-db/start-here/sdk-setup/rust-project-setup) |
| TypeScript | [`@helix-db/helix-db`](https://www.npmjs.com/package/@helix-db/helix-db) | `3.1.0` | [TypeScript setup](https://docs.helix-db.com/database/helix-db/start-here/sdk-setup/typescript-project-setup) |
| Python | [`helix-db`](https://pypi.org/project/helix-db/) | `0.3.4` | [Python setup](https://docs.helix-db.com/database/helix-db/start-here/sdk-setup/python-project-setup) |
| Go | [`github.com/helixdb/helix-db/sdks/go`](https://pkg.go.dev/github.com/helixdb/helix-db/sdks/go) | `v0.3.1` | [Go setup](https://docs.helix-db.com/database/helix-db/start-here/sdk-setup/go-project-setup) |

<details>
<summary><b>Rust</b></summary>

The crate is published as `helix-db` and imported as `helix_db`:

```bash
cargo init && cargo add helix-db@3.0.0 tokio sonic-rs
```

```rust
use helix_db::Client;
use helix_db::dsl::prelude::*;

#[query]
pub fn add_user(name: String) -> WriteBatch {
    write_batch()
        .var_as(
            "user",
            g().add_n("User", vec![("name", name)])
                .value_map(None::<Vec<String>>),
        )
        .returning(["user"])
}

#[query]
pub fn get_user(name: String) -> ReadBatch {
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
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = Client::new(None)?; // defaults to http://localhost:6969

    // #[query] helpers return Result<QueryRequest, QueryError>
    let new_user: sonic_rs::Value = client
        .query(add_user("John Doe".to_string())?)
        .send()
        .await?;
    println!("new user: {:#}", sonic_rs::to_string_pretty(&new_user)?);

    let user: sonic_rs::Value = client
        .query(get_user("John Doe".to_string())?)
        .send()
        .await?;
    println!("user: {:#}", sonic_rs::to_string_pretty(&user)?);
    Ok(())
}
```

</details>

<details>
<summary><b>TypeScript</b></summary>

Requires Node.js 20+:

```bash
npm init -y && npm install @helix-db/helix-db@3.1.0
```

```ts
import {
  Predicate, PropertyProjection,
  defineParams, g, param, readBatch, writeBatch,
} from "@helix-db/helix-db";

const addUserParams = defineParams({ name: param.string() });
function addUser(p = addUserParams) {
  return writeBatch()
    .varAs("user",
      g().addN("User", { name: p.name })
        .project([PropertyProjection.new("name")]),
    )
    .returning(["user"]);
}

const getUserParams = defineParams({ name: param.string() });
function getUser(p = getUserParams) {
  return readBatch()
    .varAs("user",
      g().nWithLabel("User")
        .where(Predicate.eq("name", p.name))
        .project([PropertyProjection.new("name")]),
    )
    .returning(["user"]);
}

const HELIX_URL = "http://localhost:6969/v2/query";

const newUser = await fetch(HELIX_URL, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: addUser().toQueryJson(addUserParams, { name: "John Doe" }),
}).then((r) => r.json());
console.log("new user:", newUser);

const user = await fetch(HELIX_URL, {
  method: "POST",
  headers: { "content-type": "application/json" },
  body: getUser().toQueryJson(getUserParams, { name: "John Doe" }),
}).then((r) => r.json());
console.log("user:", user);
```

</details>

<details>
<summary><b>Python</b></summary>

```bash
python -m pip install helix-db==0.3.4
```

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

new_user = client.query(
    add_user.to_query_request(add_user_params, {"name": "John Doe"})
)
print("new user:", new_user)

user = client.query(
    get_user.to_query_request(get_user_params, {"name": "John Doe"})
)
print("user:", user)
```

</details>

<details>
<summary><b>Go</b></summary>

```bash
go mod init example.com/my-helix-app
go get github.com/helixdb/helix-db/sdks/go@v0.3.1
```

```go
package main

import (
    "context"
    "fmt"
    "log"

    helix "github.com/helixdb/helix-db/sdks/go"
)

func getUsers() helix.Request {
    return helix.ReadQuery("get_users").
        VarAs("users", helix.G().NWithLabel("User").ValueMap("$id", "name")).
        Returning("users")
}

func main() {
    client, err := helix.NewClient("http://localhost:6969")
    if err != nil {
        log.Fatal(err)
    }

    var response map[string]any
    if err := client.Exec(context.Background(), getUsers(), &response); err != nil {
        log.Fatal(err)
    }
    fmt.Println(response)
}
```

</details>

## Helix Cloud

Helix Cloud is the managed, high-availability deployment. Object storage is the durable system of
record, a single writer commits every transaction with full ACID guarantees, and reader nodes
auto-scale with query load. [Sign up](https://helix-db.com/login) or
[talk to a founder](mailto:founders@helix-db.com).

```bash
helix auth login
helix init cloud   # pick a workspace, project, and database; links them in helix.toml
helix query production --file request.json
```

See the [Cloud CLI workflow](https://docs.helix-db.com/cli/workflows/helix_cloud) for
authentication and resource commands, and the
[architecture](https://docs.helix-db.com/database/helix-cloud/start-here/architecture) for how it
scales.

## Docs and community

- [Documentation](https://docs.helix-db.com) · [Query walkthrough](https://docs.helix-db.com/database/helix-db/core-concepts/overview) · [Learn center](https://docs.helix-db.com/learn)
- [Discord](https://discord.gg/2stgMPr5BD) · [X / Twitter](https://x.com/helixdb)

## License

HelixDB is licensed under the [Apache License 2.0](LICENSE).

---

Just Use Helix.
