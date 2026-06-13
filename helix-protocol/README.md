# helix-protocol

`helix-protocol` defines the versioned Protobuf and gRPC contract for HelixDB.

The crate generates Rust client and server bindings for `helix.v1.Helix` with Tonic. Other SDKs can generate their native clients from `proto/helix/v1/helix.proto`.

## API Surface

- `Query`: unary query execution for dynamic and stored queries.
- `QueryStream`: server-side streaming for large results and long-running graph traversals.
- `Insert`: document/record insertion with mutation metadata.
- `Search`: server-side streaming vector search results.
- `Health`: readiness/status probe for clients and load balancers.

JSON fields are represented as `bytes` so the gRPC API remains lossless with the existing `/v1/query` HTTP contract while SDKs get generated, type-safe transport code.

## Rust Usage

```rust,no_run
use helix_protocol::v1::helix_client::HelixClient;

# async fn run() -> Result<(), Box<dyn std::error::Error>> {
let mut client = HelixClient::connect("http://localhost:6970").await?;
let health = client.health(helix_protocol::v1::HealthRequest {}).await?;
println!("{:?}", health.into_inner());
# Ok(())
# }
```
