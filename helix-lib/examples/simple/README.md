# simple

Working example of using helix-lib SDK.

## What This Is

A complete example project showing how to:

- Set up build-time compilation
- Define schemas and queries
- Execute queries with type-safe responses
- Use all response deserialization patterns

## File Structure

```
simple/
├── Cargo.toml           # Dependencies
├── build.rs             # Compiles queries at build time
├── queries/
│   ├── schema.hx        # Graph schema definition
│   └── queries.hx       # Query definitions
└── src/
    ├── main.rs          # Example code
    └── queries.rs       # Generated (auto-created by build.rs)
```

## The Schema

`queries/schema.hx` defines the graph structure:

```hql
NODE User {
  name: String
  email: String
}

NODE Memory {
  user_id: ID
  content: String
  title: String
}

NODE Space {
  user_id: ID
  name: String
  description: String
}

# Edges define relationships
EDGE Owns: User -> Memory
EDGE HasSpace: User -> Space
EDGE BelongsTo: Memory -> Space
```

## The Queries

`queries/queries.hx` defines 5 queries:

### 1. GetUsers - Read all users

```hql
QUERY GetUsers() =>
  users <- N<User>
  RETURN users
```

### 2. GetMemories - Read all memories

```hql
QUERY GetMemories() =>
  memories <- N<Memory>
  RETURN memories
```

### 3. CreateUser - Create a new user

```hql
QUERY CreateUser(name: String, email: String) =>
  user <- CREATE N<User> {
    name: name,
    email: email
  }
  RETURN user
```

### 4. CreateMemory - Create a memory for a user

```hql
QUERY CreateMemory(user_id: ID, content: String, title: String) =>
  memory <- CREATE N<Memory> {
    user_id: user_id,
    content: content,
    title: title
  }
  RETURN memory
```

### 5. GetUserMemories - Get memories owned by a user

```hql
QUERY GetUserMemories(user_id: ID) =>
  user <- N<User> WHERE id == user_id
  memories <- user -> Owns -> N<Memory>
  RETURN memories
```

## Build Process

### 1. build.rs

```rust
fn main() {
    helix_lib::build::compile_queries_default()
        .expect("Failed to compile Helix queries");
}
```

What it does:

1. Reads `queries/*.hx` files
2. Parses schema and queries
3. Generates `src/queries.rs` with:
   - Handler functions for each query
   - `config()` function with embedded schema
   - Type definitions for nodes/edges

### 2. Generated Code

After `cargo build`, `src/queries.rs` contains:

```rust
// Node type definitions
pub struct User { name: String, email: String, ... }
pub struct Memory { user_id: ID, content: String, ... }

// Handler functions
#[handler]
pub fn GetUsers(input: HandlerInput) -> Result<Response, GraphError> {
    // Generated query execution code
}

#[handler]
pub fn CreateUser(input: HandlerInput) -> Result<Response, GraphError> {
    // Generated mutation code
}

// Config with embedded schema
pub fn config() -> Option<Config> {
    Some(Config {
        schema: Some("...embedded schema JSON..."),
        // ...
    })
}
```

## The Example Code

`src/main.rs` demonstrates 4 response patterns:

### Pattern 1: Two-Step

```rust
let response = db.execute("GetUsers", json!({}))?;
let data: GetUsersResponse = response.deserialize()?;
```

### Pattern 2: Chained

```rust
let data: GetUsersResponse = db.execute("GetUsers", json!({}))?.deserialize()?;
```

### Pattern 3: Dynamic JSON

```rust
let json_value = db.execute("GetUsers", json!({}))?.json()?;
println!("{}", json_value["users"]);
```

### Pattern 4: Field Extraction

```rust
let users: Vec<UserData> = db.execute("GetUsers", json!({}))?.get_field("users")?;
```

## Running It

```bash
cd helix-lib/tests/test_consumer
cargo run
```

Output:

```
Test Consumer: Simplified execute() API Demo

✓ Config loaded from generated queries.rs
  Schema embedded: true

✓ Database initialized successfully
  Path: /tmp/test_consumer_db

--- Demo 1: execute().deserialize() pattern ---
✓ Got 0 users

--- Demo 2: Chained pattern ---
✓ Got 0 users (chained)

--- Demo 3: Dynamic JSON with .json() ---
✓ JSON value: {"users":[]}

--- Demo 4: Field extraction with .get_field() ---
✓ Extracted 'users' field: 0 items

🎉 Simplified execute() API works!
```

## Dependencies

```toml
[dependencies]
helix-lib = { path = "../.." }
helix-db = { path = "../../../helix-db" }
helix-macros = { path = "../../../helix-macros" }
serde = { version = "1.0", features = ["derive"] }
sonic-rs = "0.3"
inventory = "0.3"
# ... other deps needed by generated code
```

Why these dependencies:

- `helix-lib` - SDK for building/running
- `helix-db` - Core database (re-exported by helix-lib)
- `helix-macros` - `#[handler]` macro (re-exported by helix-lib)
- `serde` - Deserialization of responses
- `sonic-rs` - JSON handling
- `inventory` - Handler registration

## Key Takeaways

1. **Build-time compilation** - Queries become Rust code during `cargo build`
2. **Type safety** - Define your response types, get compile-time checks
3. **Handler-based** - Each query becomes a handler function
4. **Flexible responses** - 4 ways to access data depending on your needs
5. **No runtime overhead** - Schema embedded, queries pre-compiled

## Use This As Template

Copy this structure for your own projects:

1. Copy file structure
2. Replace schema with your graph
3. Replace queries with your operations
4. Update response types in main.rs
5. Run it

That's all you need to know.
