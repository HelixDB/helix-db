//! # HelixDB Query Guide
//!
//! This module is the query-builder DSL of the `helix-db` crate (imported as `helix_db`),
//! centered on two entry points:
//! - [`read_batch()`] for read-only transactions
//! - [`write_batch()`] for write-capable transactions
//!
//! Everything in this crate is designed to be composed inside those batch chains.
//! You write one or more named traversals with `.var_as(...)` / `.var_as_if(...)`, then
//! choose the final payload with `.returning(...)`.
//!
//! For shorter query code, import the curated builder API:
//! ```
//! use helix_db::dsl::prelude::*;
//! ```
//!
//! ## Core Shape
//!
//! Read chain:
//! `read_batch() -> var_as / var_as_if -> returning`
//!
//! Write chain:
//! `write_batch() -> var_as / var_as_if -> returning`
//!
//! Each `var_as` call accepts a traversal expression, usually starting with `g()`.
//! Traversals can read, traverse, filter, aggregate, or mutate depending on whether
//! they are used in a read or write batch.
//!
//! ## Read Batches
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "user",
//!         g().n_where(SourcePredicate::eq("username", "alice")),
//!     )
//!     .var_as(
//!         "friends",
//!         g()
//!             .n(NodeRef::var("user"))
//!             .out(Some("FOLLOWS"))
//!             .dedup()
//!             .limit(100),
//!     )
//!     .returning(["user", "friends"]);
//! ```
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "active_users",
//!         g()
//!             .n_with_label_where("User", SourcePredicate::eq("status", "active"))
//!             .where_(Predicate::gt("score", 100i64))
//!             .order_by("score", Order::Desc)
//!             .limit(25)
//!             .value_map(Some(vec!["$id", "name", "score"])),
//!     )
//!     .returning(["active_users"]);
//! ```
//!
//! ## Conditional Queries
//!
//! Use [`BatchCondition`] with `var_as_if` to run later queries only when earlier
//! variables satisfy runtime conditions.
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "user",
//!         g().n_where(SourcePredicate::eq("username", "alice")),
//!     )
//!     .var_as_if(
//!         "posts",
//!         BatchCondition::VarNotEmpty("user".to_string()),
//!         g().n(NodeRef::var("user")).out(Some("POSTED")),
//!     )
//!     .returning(["user", "posts"]);
//! ```
//!
//! ## Write Batches
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! write_batch()
//!     .var_as(
//!         "alice",
//!         g().add_n("User", vec![("name", "Alice"), ("tier", "pro")]),
//!     )
//!     .var_as("bob", g().add_n("User", vec![("name", "Bob")]))
//!     .var_as(
//!         "linked",
//!         g()
//!             .n(NodeRef::var("alice"))
//!             .add_e(
//!                 "FOLLOWS",
//!                 NodeRef::var("bob"),
//!                 vec![("since", "2026-01-01")],
//!             )
//!             .count(),
//!     )
//!     .returning(["alice", "bob", "linked"]);
//! ```
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! write_batch()
//!     .var_as(
//!         "inactive_users",
//!         g().n_with_label_where(
//!             "User",
//!             SourcePredicate::eq("status", "inactive"),
//!         ),
//!     )
//!     .var_as_if(
//!         "deactivated_count",
//!         BatchCondition::VarNotEmpty("inactive_users".to_string()),
//!         g()
//!             .n(NodeRef::var("inactive_users"))
//!             .set_property("deactivated", true)
//!             .count(),
//!     )
//!     .returning(["deactivated_count"]);
//! ```
//!
//! ## Vector Search Operations (End-to-End)
//!
//! The current Helix interpreter executes vector search as top-k nearest-neighbor
//! lookup with these runtime semantics:
//! - returns up to `k` hits (top-k behavior)
//! - hit order is ascending by `$distance` (smaller is closer)
//! - hit metadata can be read through virtual fields in projections:
//!   - node hits: `$id`, `$distance`
//!   - edge hits: `$id`, `$from`, `$to`, `$distance`
//!
//! ### Result field contract
//!
//! | Field | Type | Node hits | Edge hits | Meaning |
//! |---|---|---:|---:|---|
//! | `$id` | integer | yes | yes* | Node ID (for node hits) or edge ID (for edge hits) |
//! | `$distance` | floating-point | yes | yes | Vector distance from query (`lower` = closer) |
//! | `$from` | integer | no | yes | Edge source node ID |
//! | `$to` | integer | no | yes | Edge target node ID |
//!
//! `*` For edge hits, `$id` is present when an edge ID is available in storage.
//!
//! Contract scope in the current Helix interpreter:
//! - available on direct vector-hit streams and projection terminals
//! - available in `value_map`, `values`, `project`, and (for edges) `edge_properties`
//! - once a traversal step leaves the hit stream (`out`, `in_`, `both`, etc.),
//!   downstream traversers no longer carry distance metadata
//!
//! ### 1) Create indexes and insert vectors
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! write_batch()
//!     .var_as(
//!         "create_doc_index",
//!         g().create_vector_index_nodes(
//!             "Doc",
//!             "embedding",
//!             None::<&str>,
//!         ),
//!     )
//!     .var_as(
//!         "create_similar_index",
//!         g().create_vector_index_edges(
//!             "SIMILAR",
//!             "embedding",
//!             None::<&str>,
//!         ),
//!     )
//!     .var_as(
//!         "doc_a",
//!         g().add_n(
//!             "Doc",
//!             vec![
//!                 ("title", PropertyValue::from("A")),
//!                 ("embedding", PropertyValue::from(vec![1.0f32, 0.0, 0.0])),
//!             ],
//!         ),
//!     )
//!     .var_as(
//!         "doc_b",
//!         g().add_n(
//!             "Doc",
//!             vec![
//!                 ("title", PropertyValue::from("B")),
//!                 ("embedding", PropertyValue::from(vec![0.9f32, 0.1, 0.0])),
//!             ],
//!         ),
//!     )
//!     .returning(["create_doc_index", "doc_a", "doc_b"]);
//! ```
//!
//! ### 2) Node vector search: get ranked hits and fetch node properties
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "doc_hits",
//!         g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 5, None)
//!             .value_map(Some(vec!["$id", "$distance", "title"])),
//!     )
//!     .returning(["doc_hits"]);
//! ```
//!
//! ```text
//! doc_hits rows (example shape):
//! [
//!   { "$id": 42, "$distance": 0.0031, "title": "A" },
//!   { "$id": 77, "$distance": 0.0198, "title": "B" }
//! ]
//! ```
//!
//! ### 3) Use `project(...)` on vector hits (including distance)
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "ranked_docs",
//!         g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 10, None)
//!             .project(vec![
//!                 PropertyProjection::renamed("$id", "doc_id"),
//!                 PropertyProjection::renamed("$distance", "score"),
//!                 PropertyProjection::new("title"),
//!             ]),
//!     )
//!     .returning(["ranked_docs"]);
//! ```
//!
//! ### 4) Traverse from hit IDs to related entities
//!
//! Store hit rows (with `$id` + `$distance`) and then use `NodeRef::var(...)` to
//! continue graph traversal from those hit IDs.
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "doc_hit_rows",
//!         g().vector_search_nodes("Doc", "embedding", vec![1.0f32, 0.0, 0.0], 5, None)
//!             .value_map(Some(vec!["$id", "$distance", "title"])),
//!     )
//!     .var_as(
//!         "authors",
//!         g().n(NodeRef::var("doc_hit_rows"))
//!             .out(Some("AUTHORED_BY"))
//!             .value_map(Some(vec!["$id", "name"])),
//!     )
//!     .returning(["doc_hit_rows", "authors"]);
//! ```
//!
//! ### 5) Edge vector search and endpoint/property extraction
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "edge_hits",
//!         g().vector_search_edges("SIMILAR", "embedding", vec![1.0f32, 0.0, 0.0], 10, None)
//!             .edge_properties(),
//!     )
//!     .var_as(
//!         "targets",
//!         g().e(EdgeRef::var("edge_hits"))
//!             .out_n()
//!             .value_map(Some(vec!["$id", "title"])),
//!     )
//!     .returning(["edge_hits", "targets"]);
//! ```
//!
//! `edge_hits` rows include `$from`, `$to`, and `$distance` (and `$id` when available),
//! so you can inspect ranking metadata and still traverse from those edges.
//!
//! ### 6) Optional multitenancy
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! write_batch()
//!     .var_as(
//!         "create_mt_index",
//!         g().create_vector_index_nodes(
//!             "Doc",
//!             "embedding",
//!             Some("tenant_id"),
//!         ),
//!     )
//!     .var_as(
//!         "insert_acme",
//!         g().add_n(
//!             "Doc",
//!             vec![
//!                 ("tenant_id", PropertyValue::from("acme")),
//!                 ("title", PropertyValue::from("Acme doc")),
//!                 ("embedding", PropertyValue::from(vec![1.0f32, 0.0, 0.0])),
//!             ],
//!         ),
//!     )
//!     .returning(["create_mt_index", "insert_acme"]);
//! ```
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "acme_hits",
//!         g().vector_search_nodes(
//!             "Doc",
//!             "embedding",
//!             vec![1.0f32, 0.0, 0.0],
//!             5,
//!             Some(PropertyValue::from("acme")),
//!         )
//!         .value_map(Some(vec!["$id", "$distance", "title"])),
//!     )
//!     .returning(["acme_hits"]);
//! ```
//!
//! Multitenant behavior in the current Helix interpreter:
//! - multitenant index + missing `tenant_value` on search => query error
//! - multitenant index + unknown tenant => empty result set
//! - write with vector present but missing tenant property => write error
//!
//! ## Edge-First Reads
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "heavy_edges",
//!         g()
//!             .e_where(SourcePredicate::gt("weight", 0.8f64))
//!             .edge_has_label("FOLLOWS")
//!             .order_by("weight", Order::Desc)
//!             .limit(50),
//!     )
//!     .var_as(
//!         "targets",
//!         g()
//!             .e(EdgeRef::var("heavy_edges"))
//!             .out_n()
//!             .dedup(),
//!     )
//!     .returning(["heavy_edges", "targets"]);
//! ```
//!
//! ## Branching and Repetition
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "recommendations",
//!         g()
//!             .n(1u64)
//!             .store("seed")
//!             .repeat(RepeatConfig::new(sub().out(Some("FOLLOWS"))).times(2))
//!             .without("seed")
//!             .union(vec![sub().out(Some("LIKES"))])
//!             .dedup()
//!             .limit(30),
//!     )
//!     .returning(["recommendations"]);
//! ```
//!
//! ## Complete Function Coverage
//!
//! The examples below are a catalog-style reference showing every public query-builder
//! function in a `read_batch()` / `write_batch()` flow.
//!
//! ### Sources, NodeRef, EdgeRef, and Vector Search
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as("n_id", g().n(NodeRef::id(1)))
//!     .var_as("n_ids", g().n(NodeRef::ids([1u64, 2, 3])))
//!     .var_as("n_var", g().n(NodeRef::var("n_ids")))
//!     .var_as(
//!         "n_where_all",
//!         g().n_where(SourcePredicate::and(vec![
//!             SourcePredicate::eq("kind", "user"),
//!             SourcePredicate::neq("status", "deleted"),
//!             SourcePredicate::gt("score", 10i64),
//!             SourcePredicate::gte("score", 10i64),
//!             SourcePredicate::lt("score", 100i64),
//!             SourcePredicate::lte("score", 100i64),
//!             SourcePredicate::between("age", 18i64, 65i64),
//!             SourcePredicate::has_key("email"),
//!             SourcePredicate::starts_with("name", "a"),
//!             SourcePredicate::or(vec![
//!                 SourcePredicate::eq("tier", "pro"),
//!                 SourcePredicate::eq("tier", "team"),
//!             ]),
//!         ])),
//!     )
//!     .var_as("n_label", g().n_with_label("User"))
//!     .var_as(
//!         "n_label_where",
//!         g().n_with_label_where("User", SourcePredicate::eq("active", true)),
//!     )
//!     .var_as("e_id", g().e(EdgeRef::id(10)))
//!     .var_as("e_ids", g().e(EdgeRef::ids([10u64, 11, 12])))
//!     .var_as("e_var", g().e(EdgeRef::var("e_ids")))
//!     .var_as("e_where", g().e_where(SourcePredicate::gte("weight", 0.5f64)))
//!     .var_as("e_label", g().e_with_label("FOLLOWS"))
//!     .var_as(
//!         "e_label_where",
//!         g().e_with_label_where("FOLLOWS", SourcePredicate::lt("weight", 2.0f64)),
//!     )
//!     .var_as(
//!         "vector_nodes",
//!         g().vector_search_nodes("Doc", "embedding", vec![0.1f32; 4], 5, None),
//!     )
//!     .var_as(
//!         "vector_edges",
//!         g().vector_search_edges("SIMILAR", "embedding", vec![0.2f32; 4], 4, None),
//!     )
//!     .var_as(
//!         "vector_nodes_tenant",
//!         g().vector_search_nodes(
//!             "Doc",
//!             "embedding",
//!             vec![0.1f32; 4],
//!             5,
//!             Some(PropertyValue::from("acme")),
//!         ),
//!     )
//!     .var_as(
//!         "vector_edges_tenant",
//!         g().vector_search_edges(
//!             "SIMILAR",
//!             "embedding",
//!             vec![0.2f32; 4],
//!             4,
//!             Some(PropertyValue::from("acme")),
//!         ),
//!     )
//!     .returning(["n_id", "e_id", "vector_nodes"]);
//! ```
//!
//! ### Node Traversal, Filters, Predicates, Expressions, and Projections
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "filtered",
//!         g()
//!             .n(1u64)
//!             .out(Some("FOLLOWS"))
//!             .in_(Some("MENTIONS"))
//!             .both(None::<&str>)
//!             .has(
//!                 "name",
//!                 PropertyValue::from("alice").as_str().unwrap_or("alice"),
//!             )
//!             .has("visits", PropertyValue::from(42i64).as_i64().unwrap_or(0i64))
//!             .has("ratio", PropertyValue::from(1.5f64).as_f64().unwrap_or(0.0f64))
//!             .has("active", PropertyValue::from(true).as_bool().unwrap_or(false))
//!             .has_label("User")
//!             .has_key("email")
//!             .where_(Predicate::and(vec![
//!                 Predicate::eq("status", "active"),
//!                 Predicate::neq("tier", "banned"),
//!                 Predicate::gt("score", 10i64),
//!                 Predicate::gte("score", 10i64),
//!                 Predicate::lt("score", 100i64),
//!                 Predicate::lte("score", 100i64),
//!                 Predicate::between("age", 18i64, 65i64),
//!                 Predicate::has_key("email"),
//!                 Predicate::starts_with("name", "a"),
//!                 Predicate::ends_with("email", ".com"),
//!                 Predicate::contains("bio", "graph"),
//!                 Predicate::not(Predicate::or(vec![
//!                     Predicate::eq("role", "bot"),
//!                     Predicate::eq("role", "system"),
//!                 ])),
//!                 Predicate::compare(
//!                     Expr::prop("price")
//!                         .mul(Expr::prop("qty"))
//!                         .add(Expr::val(10i64))
//!                         .sub(Expr::param("discount"))
//!                         .div(Expr::val(2i64))
//!                         .modulo(Expr::val(3i64))
//!                         .add(Expr::id().neg()),
//!                     CompareOp::Gt,
//!                     Expr::val(100i64),
//!                 ),
//!                 Predicate::is_in(
//!                     "status",
//!                     vec!["active".to_string(), "pending".to_string()],
//!                 ),
//!                 Predicate::eq_param("region", "target_region"),
//!                 Predicate::is_in_param("country", "allowed_countries"),
//!                 Predicate::neq_param("status", "blocked_status"),
//!                 Predicate::gt_param("score", "min_score"),
//!                 Predicate::gte_param("score", "min_score_inclusive"),
//!                 Predicate::lt_param("score", "max_score"),
//!                 Predicate::lte_param("score", "max_score_inclusive"),
//!             ]))
//!             .as_("seed")
//!             .store("seed_store")
//!             .select("seed")
//!             .inject("seed_store")
//!             .within("seed_store")
//!             .without("seed")
//!             .dedup()
//!             .order_by("score", Order::Desc)
//!             .order_by_multiple(vec![("age", Order::Asc), ("score", Order::Desc)])
//!             .limit(100)
//!             .skip(5)
//!             .range(0, 20),
//!     )
//!     .var_as("counted", g().n(NodeRef::var("filtered")).count())
//!     .var_as("exists", g().n(NodeRef::var("filtered")).exists())
//!     .var_as("ids", g().n(NodeRef::var("filtered")).id())
//!     .var_as("labels", g().n(NodeRef::var("filtered")).label())
//!     .var_as("values", g().n(NodeRef::var("filtered")).values(vec!["name", "email"]))
//!     .var_as(
//!         "value_map_some",
//!         g().n(NodeRef::var("filtered"))
//!             .value_map(Some(vec!["$id", "name", "email"])),
//!     )
//!     .var_as(
//!         "value_map_all",
//!         g().n(NodeRef::var("filtered")).value_map(None::<Vec<&str>>),
//!     )
//!     .var_as(
//!         "projected",
//!         g().n(NodeRef::var("filtered")).project(vec![
//!             PropertyProjection::new("name"),
//!             PropertyProjection::renamed("email", "contact"),
//!         ]),
//!     )
//!     .returning(["filtered", "projected"]);
//! ```
//!
//! ### Edge Traversal and Edge Terminals
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "edge_ops",
//!         g()
//!             .e_where(SourcePredicate::gt("weight", 0.1f64))
//!             .edge_has("weight", 1i64)
//!             .edge_has_label("FOLLOWS")
//!             .as_("edges_a")
//!             .store("edges_b")
//!             .dedup()
//!             .order_by("weight", Order::Desc)
//!             .limit(50)
//!             .skip(2)
//!             .range(0, 20),
//!     )
//!     .var_as("to_out_n", g().e(EdgeRef::var("edge_ops")).out_n())
//!     .var_as("to_in_n", g().e(EdgeRef::var("edge_ops")).in_n())
//!     .var_as("to_other_n", g().e(EdgeRef::var("edge_ops")).other_n())
//!     .var_as("edge_count", g().e(EdgeRef::var("edge_ops")).count())
//!     .var_as("edge_exists", g().e(EdgeRef::var("edge_ops")).exists())
//!     .var_as("edge_ids", g().e(EdgeRef::var("edge_ops")).id())
//!     .var_as("edge_labels", g().e(EdgeRef::var("edge_ops")).label())
//!     .var_as("edge_props", g().e(EdgeRef::var("edge_ops")).edge_properties())
//!     .returning(["edge_ops", "edge_props"]);
//! ```
//!
//! ### Branching, Sub-Traversals, Repeat, Grouping, Paths, and Sack
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as(
//!         "advanced",
//!         g()
//!             .n(1u64)
//!             .out_e(Some("FOLLOWS"))
//!             .in_n()
//!             .in_e(Some("MENTIONS"))
//!             .out_n()
//!             .both_e(None::<&str>)
//!             .other_n()
//!             .repeat(
//!                 RepeatConfig::new(sub().out(Some("FOLLOWS")))
//!                     .times(2)
//!                     .until(Predicate::has_key("stop"))
//!                     .emit_all()
//!                     .emit_before()
//!                     .emit_after()
//!                     .emit_if(Predicate::gt("score", 0i64))
//!                     .max_depth(8),
//!             )
//!             .union(vec![
//!                 sub().out(Some("LIKES")),
//!                 SubTraversal::new()
//!                     .out(Some("FOLLOWS"))
//!                     .in_(Some("MENTIONS"))
//!                     .both(None::<&str>)
//!                     .out_e(Some("REL"))
//!                     .in_e(Some("REL"))
//!                     .both_e(None::<&str>)
//!                     .out_n()
//!                     .in_n()
//!                     .other_n()
//!                     .has("active", true)
//!                     .has_label("User")
//!                     .has_key("email")
//!                     .where_(Predicate::eq("state", "ok"))
//!                     .dedup()
//!                     .within("allow")
//!                     .without("deny")
//!                     .edge_has("weight", 1i64)
//!                     .edge_has_label("REL")
//!                     .limit(10)
//!                     .skip(1)
//!                     .range(0, 5)
//!                     .as_("s1")
//!                     .store("s2")
//!                     .select("s1")
//!                     .order_by("score", Order::Desc)
//!                     .order_by_multiple(vec![("age", Order::Asc)])
//!                     .path()
//!                     .simple_path(),
//!             ])
//!             .choose(
//!                 Predicate::eq("vip", true),
//!                 sub().out(Some("PREMIUM")),
//!                 Some(sub().out(Some("STANDARD"))),
//!             )
//!             .coalesce(vec![sub().out(Some("POSTED")), sub().out(Some("COMMENTED"))])
//!             .optional(sub().out(Some("MENTIONED")))
//!             .fold()
//!             .unfold()
//!             .path()
//!             .simple_path()
//!             .with_sack(PropertyValue::I64(0))
//!             .sack_set("weight")
//!             .sack_add("weight")
//!             .sack_get()
//!             .dedup(),
//!     )
//!     .var_as("grouped", g().n_with_label("User").group("team"))
//!     .var_as("grouped_count", g().n_with_label("User").group_count("team"))
//!     .var_as(
//!         "aggregate_count",
//!         g().n_with_label("User")
//!             .aggregate_by(AggregateFunction::Count, "score"),
//!     )
//!     .var_as(
//!         "aggregate_sum",
//!         g().n_with_label("User").aggregate_by(AggregateFunction::Sum, "score"),
//!     )
//!     .var_as(
//!         "aggregate_min",
//!         g().n_with_label("User").aggregate_by(AggregateFunction::Min, "score"),
//!     )
//!     .var_as(
//!         "aggregate_max",
//!         g().n_with_label("User").aggregate_by(AggregateFunction::Max, "score"),
//!     )
//!     .var_as(
//!         "aggregate_mean",
//!         g().n_with_label("User")
//!             .aggregate_by(AggregateFunction::Mean, "score"),
//!     )
//!     .returning(["advanced", "grouped", "grouped_count", "aggregate_count"]);
//! ```
//!
//! ### Read-Batch Conditions
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! read_batch()
//!     .var_as("base", g().n_with_label("User"))
//!     .var_as_if(
//!         "if_not_empty",
//!         BatchCondition::VarNotEmpty("base".to_string()),
//!         g().n(NodeRef::var("base")).limit(10),
//!     )
//!     .var_as_if(
//!         "if_empty",
//!         BatchCondition::VarEmpty("base".to_string()),
//!         g().n_with_label("FallbackUser"),
//!     )
//!     .var_as_if(
//!         "if_min_size",
//!         BatchCondition::VarMinSize("base".to_string(), 5),
//!         g().n(NodeRef::var("base")).order_by("score", Order::Desc),
//!     )
//!     .var_as_if(
//!         "if_prev_not_empty",
//!         BatchCondition::PrevNotEmpty,
//!         g().n(NodeRef::var("base")).count(),
//!     )
//!     .returning(["base", "if_not_empty", "if_empty", "if_min_size", "if_prev_not_empty"]);
//! ```
//!
//! ### Write Sources, Mutations, and Vector Index Configuration
//!
//! ```
//! # use helix_db::dsl::prelude::*;
//! write_batch()
//!     .var_as("created_user", g().add_n("User", vec![("name", "Alice")]))
//!     .var_as(
//!         "created_team",
//!         g().n(NodeRef::var("created_user"))
//!             .add_n("Team", vec![("name", "Graph")]),
//!     )
//!     .var_as(
//!         "connected",
//!         g().n(NodeRef::var("created_user")).add_e(
//!             "MEMBER_OF",
//!             NodeRef::var("created_team"),
//!             vec![("since", "2026-01-01")],
//!         ),
//!     )
//!     .var_as(
//!         "updated",
//!         g().n(NodeRef::var("created_user"))
//!             .set_property("active", true)
//!             .remove_property("old_field"),
//!     )
//!     .var_as(
//!         "drop_some_edges",
//!         g().n(NodeRef::var("created_user"))
//!             .drop_edge(NodeRef::ids([2u64, 3]))
//!             .drop_edge_by_id(EdgeRef::ids([40u64, 41])),
//!     )
//!     .var_as("drop_nodes", g().n(NodeRef::var("created_team")).drop())
//!     .var_as("inject_from_empty", g().inject("created_user").has_label("User"))
//!     .var_as("drop_edge_by_id_from_empty", g().drop_edge_by_id([90u64, 91]))
//!     .var_as(
//!         "create_vector_index_nodes",
//!         g().create_vector_index_nodes(
//!             "Doc",
//!             "embedding",
//!             Some("tenant_id"),
//!         ),
//!     )
//!     .var_as(
//!         "create_vector_index_edges",
//!         g().create_vector_index_edges(
//!             "SIMILAR",
//!             "embedding",
//!             None::<&str>,
//!         ),
//!     )
//!     .var_as(
//!         "create_vector_index_edges_alt",
//!         g().create_vector_index_edges(
//!             "RELATED",
//!             "embedding",
//!             None::<&str>,
//!         ),
//!     )
//!     .var_as_if(
//!         "write_if_not_empty",
//!         BatchCondition::VarNotEmpty("created_user".to_string()),
//!         g().n(NodeRef::var("created_user")).set_property("verified", true),
//!     )
//!     .returning([
//!         "created_user",
//!         "created_team",
//!         "connected",
//!         "updated",
//!         "drop_some_edges",
//!         "drop_nodes",
//!         "inject_from_empty",
//!         "drop_edge_by_id_from_empty",
//!         "create_vector_index_nodes",
//!         "create_vector_index_edges",
//!         "create_vector_index_edges_alt",
//!         "write_if_not_empty",
//!     ]);
//! ```
//!
//! ## Traversal Building Inside `var_as(...)`
//!
//! Common source steps:
//! - `n(...)`, `n_where(...)`, `n_with_label(...)`
//! - `e(...)`, `e_where(...)`, `e_with_label(...)`
//! - `vector_search_nodes(...)`, `vector_search_edges(...)`
//!   - current Helix runtime exposes vector hit metadata via virtual fields
//!     (`$id`, `$distance`, `$from`, `$to`) in terminal projections
//!
//! Common navigation and filtering:
//! - `out/in_/both`, `out_e/in_e/both_e`, `out_n/in_n/other_n`
//! - `has`, `has_label`, `has_key`, `where_`, `within`, `without`, `dedup`
//! - `limit`, `skip`, `range`, `order_by`, `order_by_multiple`
//!
//! Common terminal projections:
//! - `count`, `exists`, `id`, `label`
//! - `values`, `value_map`, `project`, `edge_properties`
//!
//! Write-only operations (usable in [`write_batch()`] traversals):
//! - `add_n`, `add_e`, `set_property`, `remove_property`, `drop`, `drop_edge`, `drop_edge_by_id`
//! - `create_vector_index_nodes`, `create_vector_index_edges`

#![warn(missing_docs)]
#![warn(clippy::all)]
#![deny(unsafe_code)]

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;

use chrono::{SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

pub use crate::query_generator::*;
pub use helix_dsl_macros::register;

#[doc(hidden)]
pub mod __private {
    use std::collections::BTreeMap;

    pub use inventory;

    pub fn dynamic_query_value_from_property_value(
        value: crate::PropertyValue,
        path: impl Into<String>,
    ) -> Result<crate::DynamicQueryValue, crate::DynamicQueryError> {
        fn convert(
            value: crate::PropertyValue,
            path: String,
        ) -> Result<crate::DynamicQueryValue, crate::DynamicQueryError> {
            Ok(match value {
                crate::PropertyValue::Null => crate::DynamicQueryValue::Null,
                crate::PropertyValue::Bool(value) => crate::DynamicQueryValue::Bool(value),
                crate::PropertyValue::I64(value) => crate::DynamicQueryValue::I64(value),
                crate::PropertyValue::DateTime(value) => crate::DynamicQueryValue::String(
                    crate::DateTime::from_millis(value)
                        .to_rfc3339()
                        .ok_or_else(|| crate::DynamicQueryError::invalid_datetime(path, value))?,
                ),
                crate::PropertyValue::F64(value) => crate::DynamicQueryValue::F64(value),
                crate::PropertyValue::F32(value) => crate::DynamicQueryValue::F32(value),
                crate::PropertyValue::String(value) => crate::DynamicQueryValue::String(value),
                crate::PropertyValue::Bytes(_) => {
                    return Err(crate::DynamicQueryError::unsupported_bytes(path));
                }
                crate::PropertyValue::I64Array(values) => crate::DynamicQueryValue::Array(
                    values
                        .into_iter()
                        .map(crate::DynamicQueryValue::I64)
                        .collect(),
                ),
                crate::PropertyValue::F64Array(values) => crate::DynamicQueryValue::Array(
                    values
                        .into_iter()
                        .map(crate::DynamicQueryValue::F64)
                        .collect(),
                ),
                crate::PropertyValue::F32Array(values) => crate::DynamicQueryValue::Array(
                    values
                        .into_iter()
                        .map(crate::DynamicQueryValue::F32)
                        .collect(),
                ),
                crate::PropertyValue::StringArray(values) => crate::DynamicQueryValue::Array(
                    values
                        .into_iter()
                        .map(crate::DynamicQueryValue::String)
                        .collect(),
                ),
                crate::PropertyValue::Array(values) => crate::DynamicQueryValue::Array(
                    values
                        .into_iter()
                        .enumerate()
                        .map(|(index, value)| convert(value, format!("{}[{}]", path, index)))
                        .collect::<Result<Vec<_>, _>>()?,
                ),
                crate::PropertyValue::Object(values) => crate::DynamicQueryValue::Object(
                    values
                        .into_iter()
                        .map(|(key, value)| {
                            let entry_path = format!("{}.{}", path, key);
                            Ok((key, convert(value, entry_path)?))
                        })
                        .collect::<Result<BTreeMap<_, _>, crate::DynamicQueryError>>()?,
                ),
            })
        }

        convert(value, path.into())
    }
}

/// Type alias for node IDs
pub type NodeId = u64;

/// Type alias for edge IDs (separate namespace from node IDs)
pub type EdgeId = u64;

/// Arbitrary nested parameter value.
pub type ParamValue = PropertyValue;

/// Object-shaped parameter payload.
pub type ParamObject = BTreeMap<String, PropertyValue>;

// Typestate Markers

/// Marker trait for all traversal states
#[doc(hidden)]
pub trait TraversalState: private::Sealed {}

mod private {
    /// Seal the TraversalState trait to prevent external implementation
    pub trait Sealed {}
    impl Sealed for super::Empty {}
    impl Sealed for super::OnNodes {}
    impl Sealed for super::OnEdges {}
    impl Sealed for super::Terminal {}
    impl Sealed for super::ReadOnly {}
    impl Sealed for super::WriteEnabled {}
}

/// Initial state - no source step yet
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Empty;

/// Traversal is currently operating on a node stream
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnNodes;

/// Traversal is currently operating on an edge stream
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OnEdges;

/// Traversal has terminated - no more chaining allowed
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Terminal;

impl TraversalState for Empty {}
impl TraversalState for OnNodes {}
impl TraversalState for OnEdges {}
impl TraversalState for Terminal {}

// MutationMode Markers

/// Marker trait for mutation capability - tracks whether a traversal contains mutations
#[doc(hidden)]
pub trait MutationMode: private::Sealed {}

/// Read-only traversal - no mutation steps
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadOnly;

/// Write-enabled traversal - contains mutation steps
#[doc(hidden)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteEnabled;

impl MutationMode for ReadOnly {}
impl MutationMode for WriteEnabled {}

// Property Value Types

/// A property value that can be stored on nodes or edges
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyValue {
    /// Null value
    Null,
    /// Boolean value
    Bool(bool),
    /// 64-bit signed integer
    I64(i64),
    /// UTC datetime stored as epoch milliseconds
    DateTime(i64),
    /// 64-bit floating point
    F64(f64),
    /// 32-bit floating point
    F32(f32),
    /// UTF-8 string
    String(String),
    /// Raw bytes
    Bytes(Vec<u8>),
    /// Array of i64 values
    I64Array(Vec<i64>),
    /// Array of f64 values
    F64Array(Vec<f64>),
    /// Array of f32 values
    F32Array(Vec<f32>),
    /// Array of strings
    StringArray(Vec<String>),
    /// Heterogeneous array value for parameter payloads
    Array(Vec<PropertyValue>),
    /// Object/map value for parameter payloads
    Object(BTreeMap<String, PropertyValue>),
}

impl PropertyValue {
    /// Get value as string reference if it is a String
    pub fn as_str(&self) -> Option<&str> {
        match self {
            PropertyValue::String(s) => Some(s),
            _ => None,
        }
    }

    /// Get value as i64 if it is an I64
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            PropertyValue::I64(n) => Some(*n),
            _ => None,
        }
    }

    /// Create a typed datetime value from UTC epoch milliseconds
    pub fn datetime_millis(millis: i64) -> Self {
        Self::DateTime(millis)
    }

    /// Get the datetime as UTC epoch milliseconds if it is a DateTime
    pub fn as_datetime_millis(&self) -> Option<i64> {
        match self {
            PropertyValue::DateTime(millis) => Some(*millis),
            _ => None,
        }
    }

    /// Get value as f64 if it is an F64
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            PropertyValue::F64(n) => Some(*n),
            PropertyValue::F32(n) => Some(*n as f64),
            _ => None,
        }
    }

    /// Get value as bool if it is a Bool
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            PropertyValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Get value as array reference if it is an Array
    pub fn as_array(&self) -> Option<&[PropertyValue]> {
        match self {
            PropertyValue::Array(values) => Some(values),
            _ => None,
        }
    }

    /// Get value as object reference if it is an Object
    pub fn as_object(&self) -> Option<&BTreeMap<String, PropertyValue>> {
        match self {
            PropertyValue::Object(values) => Some(values),
            _ => None,
        }
    }
}

impl From<&str> for PropertyValue {
    fn from(s: &str) -> Self {
        PropertyValue::String(s.to_string())
    }
}

impl From<String> for PropertyValue {
    fn from(s: String) -> Self {
        PropertyValue::String(s)
    }
}

impl From<i64> for PropertyValue {
    fn from(n: i64) -> Self {
        PropertyValue::I64(n)
    }
}

/// UTC datetime represented internally as epoch milliseconds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DateTime(i64);

impl DateTime {
    /// Create a datetime from UTC epoch milliseconds.
    pub fn from_millis(millis: i64) -> Self {
        Self(millis)
    }

    /// Parse an RFC3339 datetime string and normalize it to UTC.
    pub fn parse_rfc3339(input: &str) -> Result<Self, chrono::ParseError> {
        Ok(Self(
            chrono::DateTime::parse_from_rfc3339(input)?
                .with_timezone(&Utc)
                .timestamp_millis(),
        ))
    }

    /// Return the UTC epoch milliseconds.
    pub fn millis(self) -> i64 {
        self.0
    }

    /// Format this datetime as a canonical RFC3339 UTC string.
    pub fn to_rfc3339(self) -> Option<String> {
        chrono::DateTime::<Utc>::from_timestamp_millis(self.0)
            .map(|dt| dt.to_rfc3339_opts(SecondsFormat::Millis, true))
    }
}

impl From<DateTime> for PropertyValue {
    fn from(value: DateTime) -> Self {
        PropertyValue::DateTime(value.millis())
    }
}

impl From<i32> for PropertyValue {
    fn from(n: i32) -> Self {
        PropertyValue::I64(n as i64)
    }
}

impl From<f64> for PropertyValue {
    fn from(n: f64) -> Self {
        PropertyValue::F64(n)
    }
}

impl From<f32> for PropertyValue {
    fn from(n: f32) -> Self {
        PropertyValue::F32(n)
    }
}

impl From<bool> for PropertyValue {
    fn from(b: bool) -> Self {
        PropertyValue::Bool(b)
    }
}

impl From<Vec<u8>> for PropertyValue {
    fn from(bytes: Vec<u8>) -> Self {
        PropertyValue::Bytes(bytes)
    }
}

impl From<Vec<i64>> for PropertyValue {
    fn from(values: Vec<i64>) -> Self {
        PropertyValue::I64Array(values)
    }
}

impl From<Vec<f64>> for PropertyValue {
    fn from(values: Vec<f64>) -> Self {
        PropertyValue::F64Array(values)
    }
}

impl From<Vec<f32>> for PropertyValue {
    fn from(values: Vec<f32>) -> Self {
        PropertyValue::F32Array(values)
    }
}

impl From<Vec<String>> for PropertyValue {
    fn from(values: Vec<String>) -> Self {
        PropertyValue::StringArray(values)
    }
}

impl From<Vec<PropertyValue>> for PropertyValue {
    fn from(values: Vec<PropertyValue>) -> Self {
        PropertyValue::Array(values)
    }
}

impl From<BTreeMap<String, PropertyValue>> for PropertyValue {
    fn from(values: BTreeMap<String, PropertyValue>) -> Self {
        PropertyValue::Object(values)
    }
}

impl From<HashMap<String, PropertyValue>> for PropertyValue {
    fn from(values: HashMap<String, PropertyValue>) -> Self {
        PropertyValue::Object(values.into_iter().collect())
    }
}

/// Mutation input value for add/set property operations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PropertyInput {
    /// Store a literal property value.
    Value(PropertyValue),
    /// Resolve the value from an expression at execution time.
    Expr(Expr),
}

impl PropertyInput {
    /// Create an input from a query parameter.
    pub fn param(name: impl Into<String>) -> Self {
        Self::Expr(Expr::param(name))
    }

    /// Convert into an `Expr`, promoting a literal value to `Expr::Constant`.
    #[doc(hidden)]
    pub fn into_expr(self) -> Expr {
        match self {
            PropertyInput::Value(v) => Expr::Constant(v),
            PropertyInput::Expr(e) => e,
        }
    }
}

impl<T> From<T> for PropertyInput
where
    PropertyValue: From<T>,
{
    fn from(value: T) -> Self {
        Self::Value(value.into())
    }
}

impl From<Expr> for PropertyInput {
    fn from(value: Expr) -> Self {
        Self::Expr(value)
    }
}

// Reference Types

/// A reference to nodes - can be concrete IDs or a variable name
///
/// This allows the AST to express operations without knowing actual IDs at build time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum NodeRef {
    /// All nodes in storage
    All,
    /// One or more concrete node IDs
    Ids(Vec<NodeId>),
    /// Reference nodes stored in a named variable
    Var(String),
    /// Reference node IDs from a runtime parameter
    Param(String),
}

impl NodeRef {
    /// Create a reference to all nodes
    pub fn all() -> Self {
        NodeRef::All
    }

    /// Create a reference to a single node ID
    pub fn id(id: NodeId) -> Self {
        NodeRef::Ids(vec![id])
    }

    /// Create a reference to multiple node IDs
    pub fn ids(ids: impl IntoIterator<Item = NodeId>) -> Self {
        NodeRef::Ids(ids.into_iter().collect())
    }

    /// Create a reference to a variable
    pub fn var(name: impl Into<String>) -> Self {
        NodeRef::Var(name.into())
    }

    /// Create a reference to node IDs stored in a runtime parameter
    pub fn param(name: impl Into<String>) -> Self {
        NodeRef::Param(name.into())
    }
}

impl From<NodeId> for NodeRef {
    fn from(id: NodeId) -> Self {
        NodeRef::Ids(vec![id])
    }
}

impl From<Vec<NodeId>> for NodeRef {
    fn from(ids: Vec<NodeId>) -> Self {
        NodeRef::Ids(ids)
    }
}

impl<const N: usize> From<[NodeId; N]> for NodeRef {
    fn from(ids: [NodeId; N]) -> Self {
        NodeRef::Ids(ids.to_vec())
    }
}

impl From<&str> for NodeRef {
    fn from(var_name: &str) -> Self {
        NodeRef::Var(var_name.to_string())
    }
}

/// A reference to edges - can be concrete IDs or a variable name
///
/// This allows the AST to express operations without knowing actual IDs at build time.
/// Edge IDs are separate from node IDs in the graph.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum EdgeRef {
    /// One or more concrete edge IDs
    Ids(Vec<EdgeId>),
    /// Reference edges stored in a named variable
    Var(String),
    /// Reference edge IDs from a runtime parameter
    Param(String),
}

impl EdgeRef {
    /// Create a reference to a single edge ID
    pub fn id(id: EdgeId) -> Self {
        EdgeRef::Ids(vec![id])
    }

    /// Create a reference to multiple edge IDs
    pub fn ids(ids: impl IntoIterator<Item = EdgeId>) -> Self {
        EdgeRef::Ids(ids.into_iter().collect())
    }

    /// Create a reference to a variable containing edges
    pub fn var(name: impl Into<String>) -> Self {
        EdgeRef::Var(name.into())
    }

    /// Create a reference to edge IDs stored in a runtime parameter
    pub fn param(name: impl Into<String>) -> Self {
        EdgeRef::Param(name.into())
    }
}

impl From<EdgeId> for EdgeRef {
    fn from(id: EdgeId) -> Self {
        EdgeRef::Ids(vec![id])
    }
}

impl From<Vec<EdgeId>> for EdgeRef {
    fn from(ids: Vec<EdgeId>) -> Self {
        EdgeRef::Ids(ids)
    }
}

impl<const N: usize> From<[EdgeId; N]> for EdgeRef {
    fn from(ids: [EdgeId; N]) -> Self {
        EdgeRef::Ids(ids.to_vec())
    }
}

// Expression Types

/// An expression for computed values, math operations, and property references
///
/// Expressions can be used in predicates for property-to-property comparisons,
/// computed values, and math operations.
///
/// Note: support for some expression variants is engine-dependent. In particular,
/// `Expr::Id` may be reserved or unsupported by some runtimes.
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    /// Reference a property by name
    Property(String),
    /// The current element ID (engine-defined).
    Id,
    /// Server-side current timestamp in UTC epoch milliseconds.
    Timestamp,
    /// Server-side current datetime as a typed `DateTime` value.
    DateTimeNow,
    /// A constant value
    Constant(PropertyValue),
    /// Reference a query parameter by name
    Param(String),
    /// Addition: left + right
    Add(Box<Expr>, Box<Expr>),
    /// Subtraction: left - right
    Sub(Box<Expr>, Box<Expr>),
    /// Multiplication: left * right
    Mul(Box<Expr>, Box<Expr>),
    /// Division: left / right
    Div(Box<Expr>, Box<Expr>),
    /// Modulo: left % right
    Mod(Box<Expr>, Box<Expr>),
    /// Negation: -expr
    Neg(Box<Expr>),
    /// Conditional expression that evaluates the first matching branch.
    Case {
        /// Ordered predicate/expression branches.
        when_then: Vec<(Predicate, Expr)>,
        /// Fallback expression. When omitted, the result is explicit `Null`.
        else_expr: Option<Box<Expr>>,
    },
}

impl Expr {
    /// Create a property reference expression
    pub fn prop(name: impl Into<String>) -> Self {
        Expr::Property(name.into())
    }

    /// Create a constant value expression
    pub fn val(value: impl Into<PropertyValue>) -> Self {
        Expr::Constant(value.into())
    }

    /// Create an ID reference expression
    pub fn id() -> Self {
        Expr::Id
    }

    /// Create a server-side timestamp expression (UTC epoch milliseconds).
    pub fn timestamp() -> Self {
        Expr::Timestamp
    }

    /// Create a server-side datetime expression.
    pub fn datetime() -> Self {
        Expr::DateTimeNow
    }

    /// Create a parameter reference expression
    pub fn param(name: impl Into<String>) -> Self {
        Expr::Param(name.into())
    }

    /// Addition: self + other
    #[allow(clippy::should_implement_trait)]
    pub fn add(self, other: Expr) -> Self {
        Expr::Add(Box::new(self), Box::new(other))
    }

    /// Subtraction: self - other
    #[allow(clippy::should_implement_trait)]
    pub fn sub(self, other: Expr) -> Self {
        Expr::Sub(Box::new(self), Box::new(other))
    }

    /// Multiplication: self * other
    #[allow(clippy::should_implement_trait)]
    pub fn mul(self, other: Expr) -> Self {
        Expr::Mul(Box::new(self), Box::new(other))
    }

    /// Division: self / other
    #[allow(clippy::should_implement_trait)]
    pub fn div(self, other: Expr) -> Self {
        Expr::Div(Box::new(self), Box::new(other))
    }

    /// Modulo: self % other
    pub fn modulo(self, other: Expr) -> Self {
        Expr::Mod(Box::new(self), Box::new(other))
    }

    /// Negation: -self
    #[allow(clippy::should_implement_trait)]
    pub fn neg(self) -> Self {
        Expr::Neg(Box::new(self))
    }

    /// Create a conditional expression.
    pub fn case(when_then: Vec<(Predicate, Expr)>, else_expr: Option<Expr>) -> Self {
        Expr::Case {
            when_then,
            else_expr: else_expr.map(Box::new),
        }
    }
}

/// A non-negative integer input used by stream-shaping steps like `limit`, `skip`, and `range`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamBound {
    /// A literal bound known at query-build time.
    Literal(usize),
    /// A computed or parameterized bound resolved by the runtime.
    Expr(Expr),
}

impl StreamBound {
    /// Create a literal bound.
    pub fn literal(value: usize) -> Self {
        Self::Literal(value)
    }

    /// Create an expression-backed bound.
    pub fn expr(expr: Expr) -> Self {
        Self::Expr(expr)
    }
}

impl From<usize> for StreamBound {
    fn from(value: usize) -> Self {
        Self::Literal(value)
    }
}

impl From<u32> for StreamBound {
    fn from(value: u32) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<u16> for StreamBound {
    fn from(value: u16) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<u8> for StreamBound {
    fn from(value: u8) -> Self {
        Self::Literal(value as usize)
    }
}

impl From<i64> for StreamBound {
    fn from(value: i64) -> Self {
        if value >= 0 {
            Self::Literal(value as usize)
        } else {
            Self::Expr(Expr::val(value))
        }
    }
}

impl From<i32> for StreamBound {
    fn from(value: i32) -> Self {
        if value >= 0 {
            Self::Literal(value as usize)
        } else {
            Self::Expr(Expr::val(value))
        }
    }
}

impl From<Expr> for StreamBound {
    fn from(value: Expr) -> Self {
        Self::Expr(value)
    }
}

/// Comparison operators for expression-based predicates
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CompareOp {
    /// Equal
    Eq,
    /// Not equal
    Neq,
    /// Greater than
    Gt,
    /// Greater than or equal
    Gte,
    /// Less than
    Lt,
    /// Less than or equal
    Lte,
}

// Predicate Types

/// A predicate for filtering nodes by properties
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Predicate {
    /// Equals: property == value
    Eq(String, PropertyValue),
    /// Not equals: property != value
    Neq(String, PropertyValue),
    /// Greater than: property > value (for numeric/string)
    Gt(String, PropertyValue),
    /// Greater than or equal: property >= value
    Gte(String, PropertyValue),
    /// Less than: property < value
    Lt(String, PropertyValue),
    /// Less than or equal: property <= value
    Lte(String, PropertyValue),
    /// Between (inclusive): min <= property <= max
    Between(String, PropertyValue, PropertyValue),
    /// Equals against an expression/parameter: property == <expr>
    EqExpr(String, Expr),
    /// Not equals against an expression/parameter
    NeqExpr(String, Expr),
    /// Greater than an expression/parameter
    GtExpr(String, Expr),
    /// Greater than or equal to an expression/parameter
    GteExpr(String, Expr),
    /// Less than an expression/parameter
    LtExpr(String, Expr),
    /// Less than or equal to an expression/parameter
    LteExpr(String, Expr),
    /// Between two expressions/parameters (inclusive)
    BetweenExpr(String, Expr, Expr),
    /// Property exists
    HasKey(String),
    /// Property is missing or explicitly null.
    IsNull(String),
    /// Property exists and is not null.
    IsNotNull(String),
    /// String starts with prefix
    StartsWith(String, String),
    /// String ends with suffix
    EndsWith(String, String),
    /// String contains substring
    Contains(String, String),
    /// String contains a runtime expression result
    ContainsExpr(String, Expr),
    /// Property value is equal to one of the provided values
    IsIn(String, PropertyValue),
    /// Property value is equal to one of the values produced by a runtime expression
    IsInExpr(String, Expr),
    /// Logical AND of predicates
    And(Vec<Predicate>),
    /// Logical OR of predicates
    Or(Vec<Predicate>),
    /// Logical NOT of predicate
    Not(Box<Predicate>),
    /// Expression-based comparison (supports property-to-property, math, etc.)
    Compare {
        /// Left side of comparison
        left: Expr,
        /// Comparison operator
        op: CompareOp,
        /// Right side of comparison
        right: Expr,
    },
}

/// A predicate that can be used in source steps (`n_where` / `e_where`).
///
/// This is a restricted subset of [`Predicate`] intended to be index- and
/// planner-friendly for "source" selection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SourcePredicate {
    /// Equals: property == value
    Eq(String, PropertyValue),
    /// Not equals: property != value
    Neq(String, PropertyValue),
    /// Greater than: property > value (for numeric/string)
    Gt(String, PropertyValue),
    /// Greater than or equal: property >= value
    Gte(String, PropertyValue),
    /// Less than: property < value
    Lt(String, PropertyValue),
    /// Less than or equal: property <= value
    Lte(String, PropertyValue),
    /// Between (inclusive): min <= property <= max
    Between(String, PropertyValue, PropertyValue),
    /// Property exists
    HasKey(String),
    /// String starts with prefix
    StartsWith(String, String),
    /// Logical AND of predicates
    And(Vec<SourcePredicate>),
    /// Logical OR of predicates
    Or(Vec<SourcePredicate>),
    /// Equals against an expression/parameter: property == <expr>
    EqExpr(String, Expr),
    /// Not equals against an expression/parameter
    NeqExpr(String, Expr),
    /// Greater than an expression/parameter
    GtExpr(String, Expr),
    /// Greater than or equal to an expression/parameter
    GteExpr(String, Expr),
    /// Less than an expression/parameter
    LtExpr(String, Expr),
    /// Less than or equal to an expression/parameter
    LteExpr(String, Expr),
    /// Between two expressions/parameters (inclusive)
    BetweenExpr(String, Expr, Expr),
}

impl SourcePredicate {
    /// Create an equality predicate.
    ///
    /// Accepts a literal value or an `Expr`/query parameter. Literals keep the `Eq` variant;
    /// expressions route to `EqExpr`.
    pub fn eq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Eq(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::EqExpr(property.into(), e),
        }
    }

    /// Create a not-equals predicate (literal or `Expr`/parameter).
    pub fn neq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Neq(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::NeqExpr(property.into(), e),
        }
    }

    /// Create a greater-than predicate (literal or `Expr`/parameter).
    pub fn gt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Gt(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::GtExpr(property.into(), e),
        }
    }

    /// Create a greater-than-or-equal predicate (literal or `Expr`/parameter).
    pub fn gte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Gte(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::GteExpr(property.into(), e),
        }
    }

    /// Create a less-than predicate (literal or `Expr`/parameter).
    pub fn lt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Lt(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::LtExpr(property.into(), e),
        }
    }

    /// Create a less-than-or-equal predicate (literal or `Expr`/parameter).
    pub fn lte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => SourcePredicate::Lte(property.into(), v),
            PropertyInput::Expr(e) => SourcePredicate::LteExpr(property.into(), e),
        }
    }

    /// Create a between predicate (inclusive). Accepts literals or `Expr`/parameters; if either
    /// bound is an expression, both are promoted to `BetweenExpr`.
    pub fn between(
        property: impl Into<String>,
        min: impl Into<PropertyInput>,
        max: impl Into<PropertyInput>,
    ) -> Self {
        let prop = property.into();
        match (min.into(), max.into()) {
            (PropertyInput::Value(a), PropertyInput::Value(b)) => {
                SourcePredicate::Between(prop, a, b)
            }
            (min, max) => SourcePredicate::BetweenExpr(prop, min.into_expr(), max.into_expr()),
        }
    }

    /// Create a has-key predicate
    pub fn has_key(property: impl Into<String>) -> Self {
        SourcePredicate::HasKey(property.into())
    }

    /// Create a starts-with predicate
    pub fn starts_with(property: impl Into<String>, prefix: impl Into<String>) -> Self {
        SourcePredicate::StartsWith(property.into(), prefix.into())
    }

    /// Combine predicates with AND
    pub fn and(predicates: Vec<SourcePredicate>) -> Self {
        SourcePredicate::And(predicates)
    }

    /// Combine predicates with OR
    pub fn or(predicates: Vec<SourcePredicate>) -> Self {
        SourcePredicate::Or(predicates)
    }
}

impl From<SourcePredicate> for Predicate {
    fn from(predicate: SourcePredicate) -> Self {
        match predicate {
            SourcePredicate::Eq(prop, val) => Predicate::Eq(prop, val),
            SourcePredicate::Neq(prop, val) => Predicate::Neq(prop, val),
            SourcePredicate::Gt(prop, val) => Predicate::Gt(prop, val),
            SourcePredicate::Gte(prop, val) => Predicate::Gte(prop, val),
            SourcePredicate::Lt(prop, val) => Predicate::Lt(prop, val),
            SourcePredicate::Lte(prop, val) => Predicate::Lte(prop, val),
            SourcePredicate::Between(prop, min, max) => Predicate::Between(prop, min, max),
            SourcePredicate::HasKey(prop) => Predicate::HasKey(prop),
            SourcePredicate::StartsWith(prop, prefix) => Predicate::StartsWith(prop, prefix),
            SourcePredicate::And(predicates) => {
                Predicate::And(predicates.into_iter().map(Predicate::from).collect())
            }
            SourcePredicate::Or(predicates) => {
                Predicate::Or(predicates.into_iter().map(Predicate::from).collect())
            }
            SourcePredicate::EqExpr(prop, e) => Predicate::EqExpr(prop, e),
            SourcePredicate::NeqExpr(prop, e) => Predicate::NeqExpr(prop, e),
            SourcePredicate::GtExpr(prop, e) => Predicate::GtExpr(prop, e),
            SourcePredicate::GteExpr(prop, e) => Predicate::GteExpr(prop, e),
            SourcePredicate::LtExpr(prop, e) => Predicate::LtExpr(prop, e),
            SourcePredicate::LteExpr(prop, e) => Predicate::LteExpr(prop, e),
            SourcePredicate::BetweenExpr(prop, min, max) => Predicate::BetweenExpr(prop, min, max),
        }
    }
}

impl Predicate {
    /// Create an equality predicate.
    ///
    /// Accepts a literal value or an `Expr`/query parameter. Literals keep the `Eq` variant;
    /// expressions route to `EqExpr`.
    pub fn eq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Eq(property.into(), v),
            PropertyInput::Expr(e) => Predicate::EqExpr(property.into(), e),
        }
    }

    /// Create a not-equals predicate (literal or `Expr`/parameter).
    pub fn neq(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Neq(property.into(), v),
            PropertyInput::Expr(e) => Predicate::NeqExpr(property.into(), e),
        }
    }

    /// Create a greater-than predicate (literal or `Expr`/parameter).
    pub fn gt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Gt(property.into(), v),
            PropertyInput::Expr(e) => Predicate::GtExpr(property.into(), e),
        }
    }

    /// Create a greater-than-or-equal predicate (literal or `Expr`/parameter).
    pub fn gte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Gte(property.into(), v),
            PropertyInput::Expr(e) => Predicate::GteExpr(property.into(), e),
        }
    }

    /// Create a less-than predicate (literal or `Expr`/parameter).
    pub fn lt(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Lt(property.into(), v),
            PropertyInput::Expr(e) => Predicate::LtExpr(property.into(), e),
        }
    }

    /// Create a less-than-or-equal predicate (literal or `Expr`/parameter).
    pub fn lte(property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        match value.into() {
            PropertyInput::Value(v) => Predicate::Lte(property.into(), v),
            PropertyInput::Expr(e) => Predicate::LteExpr(property.into(), e),
        }
    }

    /// Create a between predicate (inclusive). Accepts literals or `Expr`/parameters; if either
    /// bound is an expression, both are promoted to `BetweenExpr`.
    pub fn between(
        property: impl Into<String>,
        min: impl Into<PropertyInput>,
        max: impl Into<PropertyInput>,
    ) -> Self {
        let prop = property.into();
        match (min.into(), max.into()) {
            (PropertyInput::Value(a), PropertyInput::Value(b)) => Predicate::Between(prop, a, b),
            (min, max) => Predicate::BetweenExpr(prop, min.into_expr(), max.into_expr()),
        }
    }

    /// Create a has-key predicate
    pub fn has_key(property: impl Into<String>) -> Self {
        Predicate::HasKey(property.into())
    }

    /// Create an `IS NULL` predicate.
    pub fn is_null(property: impl Into<String>) -> Self {
        Predicate::IsNull(property.into())
    }

    /// Create an `IS NOT NULL` predicate.
    pub fn is_not_null(property: impl Into<String>) -> Self {
        Predicate::IsNotNull(property.into())
    }

    /// Create a starts-with predicate
    pub fn starts_with(property: impl Into<String>, prefix: impl Into<String>) -> Self {
        Predicate::StartsWith(property.into(), prefix.into())
    }

    /// Create an ends-with predicate
    pub fn ends_with(property: impl Into<String>, suffix: impl Into<String>) -> Self {
        Predicate::EndsWith(property.into(), suffix.into())
    }

    /// Create a contains predicate
    pub fn contains(property: impl Into<String>, substring: impl Into<String>) -> Self {
        Predicate::Contains(property.into(), substring.into())
    }

    /// Create a parameterized contains predicate: property contains param string
    pub fn contains_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::ContainsExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create an `IN` predicate with a literal array value.
    pub fn is_in(property: impl Into<String>, values: impl Into<PropertyValue>) -> Self {
        Predicate::IsIn(property.into(), values.into())
    }

    /// Create an `IN` predicate whose values are resolved from an expression.
    pub fn is_in_expr(property: impl Into<String>, values: Expr) -> Self {
        Predicate::IsInExpr(property.into(), values)
    }

    /// Create a parameterized `IN` predicate: property IN param_array.
    pub fn is_in_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::IsInExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Combine predicates with AND
    pub fn and(predicates: Vec<Predicate>) -> Self {
        Predicate::And(predicates)
    }

    /// Combine predicates with OR
    pub fn or(predicates: Vec<Predicate>) -> Self {
        Predicate::Or(predicates)
    }

    /// Negate a predicate
    #[allow(clippy::should_implement_trait)]
    pub fn not(predicate: Predicate) -> Self {
        Predicate::Not(Box::new(predicate))
    }

    /// Create an expression-based comparison predicate
    ///
    /// This supports property-to-property comparisons, math expressions, and more.
    ///
    pub fn compare(left: Expr, op: CompareOp, right: Expr) -> Self {
        Predicate::Compare { left, op, right }
    }

    // Parameterized predicate constructors

    /// Create a parameterized equality predicate: property == param
    ///
    /// The parameter value is provided at query execution time.
    ///
    pub fn eq_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::EqExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create a parameterized not-equals predicate: property != param
    pub fn neq_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::NeqExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create a parameterized greater-than predicate: property > param
    pub fn gt_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::GtExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create a parameterized greater-than-or-equal predicate: property >= param
    pub fn gte_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::GteExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create a parameterized less-than predicate: property < param
    pub fn lt_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::LtExpr(property.into(), Expr::Param(param_name.into()))
    }

    /// Create a parameterized less-than-or-equal predicate: property <= param
    pub fn lte_param(property: impl Into<String>, param_name: impl Into<String>) -> Self {
        Predicate::LteExpr(property.into(), Expr::Param(param_name.into()))
    }
}

// Supporting Types

/// A property projection with optional renaming
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PropertyProjection {
    /// Original property name in the data
    pub source: String,
    /// Name to use in the output (alias)
    pub alias: String,
}

impl PropertyProjection {
    /// Create a projection without renaming
    pub fn new(name: impl Into<String>) -> Self {
        let n = name.into();
        Self {
            source: n.clone(),
            alias: n,
        }
    }

    /// Create a projection with renaming
    pub fn renamed(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self {
            source: source.into(),
            alias: alias.into(),
        }
    }
}

/// An expression-backed projection.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExprProjection {
    /// Name to use in the output.
    pub alias: String,
    /// Expression to evaluate.
    pub expr: Expr,
}

impl ExprProjection {
    /// Create a projection from an expression.
    pub fn new(alias: impl Into<String>, expr: Expr) -> Self {
        Self {
            alias: alias.into(),
            expr,
        }
    }
}

/// A terminal projection entry.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Projection {
    /// Project a property with optional renaming.
    Property(PropertyProjection),
    /// Project a computed expression.
    Expr(ExprProjection),
}

impl Projection {
    /// Project a property with optional renaming.
    pub fn property(source: impl Into<String>, alias: impl Into<String>) -> Self {
        Self::Property(PropertyProjection::renamed(source, alias))
    }

    /// Project a computed expression.
    pub fn expr(alias: impl Into<String>, expr: Expr) -> Self {
        Self::Expr(ExprProjection::new(alias, expr))
    }
}

impl From<PropertyProjection> for Projection {
    fn from(value: PropertyProjection) -> Self {
        Self::Property(value)
    }
}

impl From<ExprProjection> for Projection {
    fn from(value: ExprProjection) -> Self {
        Self::Expr(value)
    }
}

/// Sort order for ordering steps
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum Order {
    /// Ascending order (smallest first)
    #[default]
    Asc,
    /// Descending order (largest first)
    Desc,
}

/// Emit behavior for repeat steps
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EmitBehavior {
    /// Don't emit intermediate results.
    #[default]
    None,
    /// Emit the current node stream before each repeat iteration.
    Before,
    /// Emit the node stream produced by each repeat iteration.
    After,
    /// Emit both before and after each repeat iteration.
    All,
}

/// Aggregation function for reduce operations
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AggregateFunction {
    /// Count items
    Count,
    /// Sum numeric values
    Sum,
    /// Find minimum value
    Min,
    /// Find maximum value
    Max,
    /// Calculate mean/average
    Mean,
}

// Sub-Traversal (for branching operations without typestate)

/// A sub-traversal for use in branching operations (union, choose, coalesce, optional, repeat).
///
/// Sub-traversals don't track typestate because they start from an implicit context
/// provided by the parent traversal. This allows maximum flexibility in branching
/// while the parent traversal maintains compile-time safety.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SubTraversal {
    /// The steps in this sub-traversal
    pub steps: Vec<Step>,
}

impl SubTraversal {
    /// Create a new empty sub-traversal
    pub fn new() -> Self {
        Self { steps: Vec::new() }
    }

    // Navigation Steps (node -> node)

    /// Traverse outgoing edges, optionally filtered by label
    pub fn out(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::Out(label.map(|l| l.into())));
        self
    }

    /// Traverse incoming edges, optionally filtered by label
    pub fn in_(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::In(label.map(|l| l.into())));
        self
    }

    /// Traverse edges in both directions, optionally filtered by label
    pub fn both(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::Both(label.map(|l| l.into())));
        self
    }

    // Edge Traversal Steps

    /// Traverse to outgoing edges
    pub fn out_e(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::OutE(label.map(|l| l.into())));
        self
    }

    /// Traverse to incoming edges
    pub fn in_e(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::InE(label.map(|l| l.into())));
        self
    }

    /// Traverse to edges in both directions
    pub fn both_e(mut self, label: Option<impl Into<String>>) -> Self {
        self.steps.push(Step::BothE(label.map(|l| l.into())));
        self
    }

    /// From edge, get the target node
    pub fn out_n(mut self) -> Self {
        self.steps.push(Step::OutN);
        self
    }

    /// From edge, get the source node
    pub fn in_n(mut self) -> Self {
        self.steps.push(Step::InN);
        self
    }

    /// From edge, get the "other" node (not the one we came from)
    pub fn other_n(mut self) -> Self {
        self.steps.push(Step::OtherN);
        self
    }

    // Filter Steps

    /// Filter by property value
    pub fn has(mut self, property: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        self.steps.push(Step::Has(property.into(), value.into()));
        self
    }

    /// Filter by label (shorthand for has("$label", value))
    pub fn has_label(mut self, label: impl Into<String>) -> Self {
        self.steps.push(Step::HasLabel(label.into()));
        self
    }

    /// Filter by property existence
    pub fn has_key(mut self, property: impl Into<String>) -> Self {
        self.steps.push(Step::HasKey(property.into()));
        self
    }

    /// Filter by a complex predicate
    pub fn where_(mut self, predicate: Predicate) -> Self {
        self.steps.push(Step::Where(predicate));
        self
    }

    /// Remove duplicates from the stream
    pub fn dedup(mut self) -> Self {
        self.steps.push(Step::Dedup);
        self
    }

    /// Filter to nodes that exist in a variable
    pub fn within(mut self, var_name: impl Into<String>) -> Self {
        self.steps.push(Step::Within(var_name.into()));
        self
    }

    /// Filter to nodes that do NOT exist in a variable
    pub fn without(mut self, var_name: impl Into<String>) -> Self {
        self.steps.push(Step::Without(var_name.into()));
        self
    }

    // Edge Filter Steps

    /// Filter edges by property value
    pub fn edge_has(
        mut self,
        property: impl Into<String>,
        value: impl Into<PropertyInput>,
    ) -> Self {
        self.steps
            .push(Step::EdgeHas(property.into(), value.into()));
        self
    }

    /// Filter edges by label
    pub fn edge_has_label(mut self, label: impl Into<String>) -> Self {
        self.steps.push(Step::EdgeHasLabel(label.into()));
        self
    }

    // Limit Steps

    /// Take at most N items.
    pub fn limit(mut self, n: impl Into<StreamBound>) -> Self {
        self.steps.push(limit_step(n));
        self
    }

    /// Skip the first N items.
    pub fn skip(mut self, n: impl Into<StreamBound>) -> Self {
        self.steps.push(skip_step(n));
        self
    }

    /// Get items in a range [start, end).
    pub fn range(mut self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.steps.push(range_step(start, end));
        self
    }

    // Variable Steps

    /// Store current nodes with a name for later reference
    pub fn as_(mut self, name: impl Into<String>) -> Self {
        self.steps.push(Step::As(name.into()));
        self
    }

    /// Store current nodes to a variable (same as `as_`)
    pub fn store(mut self, name: impl Into<String>) -> Self {
        self.steps.push(Step::Store(name.into()));
        self
    }

    /// Replace current traversal with nodes from a variable
    pub fn select(mut self, name: impl Into<String>) -> Self {
        self.steps.push(Step::Select(name.into()));
        self
    }

    // Ordering Steps

    /// Order results by a property.
    ///
    /// Note: some interpreters represent intermediate streams as sets. In those
    /// engines, ordering may not be preserved in the returned node set.
    pub fn order_by(mut self, property: impl Into<String>, order: Order) -> Self {
        self.steps.push(Step::OrderBy(property.into(), order));
        self
    }

    /// Order results by multiple properties with priorities.
    ///
    /// Note: some interpreters represent intermediate streams as sets. In those
    /// engines, ordering may not be preserved in the returned node set.
    pub fn order_by_multiple(mut self, orderings: Vec<(impl Into<String>, Order)>) -> Self {
        let orderings: Vec<(String, Order)> =
            orderings.into_iter().map(|(p, o)| (p.into(), o)).collect();
        self.steps.push(Step::OrderByMultiple(orderings));
        self
    }

    // Path Steps

    /// Include the full traversal path in results.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn path(mut self) -> Self {
        self.steps.push(Step::Path);
        self
    }

    /// Filter to only simple paths (no repeated nodes).
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn simple_path(mut self) -> Self {
        self.steps.push(Step::SimplePath);
        self
    }
}

/// Create a new sub-traversal for use in branching operations
///
/// Use this instead of `g()` when building traversals for `union()`, `choose()`,
/// `coalesce()`, `optional()`, or `repeat()`.
///
pub fn sub() -> SubTraversal {
    SubTraversal::new()
}

// Repeat Configuration

/// Configuration for repeat steps
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RepeatConfig {
    /// The sub-traversal to repeat
    pub traversal: SubTraversal,
    /// Maximum number of iterations (None = unlimited)
    pub times: Option<usize>,
    /// Condition to stop repeating (checked each iteration)
    pub until: Option<Predicate>,
    /// Whether to emit intermediate results
    pub emit: EmitBehavior,
    /// Optional predicate for conditional emit
    pub emit_predicate: Option<Predicate>,
    /// Maximum depth to prevent infinite loops (default: 100)
    pub max_depth: usize,
}

impl RepeatConfig {
    /// Create a new repeat configuration
    pub fn new(traversal: SubTraversal) -> Self {
        Self {
            traversal,
            times: None,
            until: None,
            emit: EmitBehavior::None,
            emit_predicate: None,
            max_depth: 100,
        }
    }

    /// Set the number of times to repeat
    pub fn times(mut self, n: usize) -> Self {
        self.times = Some(n);
        self
    }

    /// Set the until condition
    pub fn until(mut self, predicate: Predicate) -> Self {
        self.until = Some(predicate);
        self
    }

    /// Emit intermediate results before and after each iteration.
    pub fn emit_all(mut self) -> Self {
        self.emit = EmitBehavior::All;
        self
    }

    /// Emit intermediate results before each iteration
    pub fn emit_before(mut self) -> Self {
        self.emit = EmitBehavior::Before;
        self
    }

    /// Emit intermediate results after each iteration
    pub fn emit_after(mut self) -> Self {
        self.emit = EmitBehavior::After;
        self
    }

    /// Emit intermediate results that match a predicate.
    ///
    /// This enables post-iteration emission (equivalent to [`EmitBehavior::After`])
    /// and applies `predicate` to decide which vertices to emit.
    pub fn emit_if(mut self, predicate: Predicate) -> Self {
        self.emit = EmitBehavior::After;
        self.emit_predicate = Some(predicate);
        self
    }

    /// Set maximum depth to prevent infinite loops
    pub fn max_depth(mut self, depth: usize) -> Self {
        self.max_depth = depth;
        self
    }
}

/// Dynamic index declaration used by runtime index-management steps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum IndexSpec {
    /// Equality index over node properties.
    NodeEquality {
        /// Node label to scope the index.
        label: String,
        /// Indexed property name.
        property: String,
        /// Whether the index enforces uniqueness for supported non-null values.
        #[serde(default)]
        unique: bool,
    },
    /// Range index over node properties.
    NodeRange {
        /// Node label to scope the index.
        label: String,
        /// Indexed property name.
        property: String,
    },
    /// Equality index over edge properties.
    EdgeEquality {
        /// Edge label to scope the index.
        label: String,
        /// Indexed property name.
        property: String,
    },
    /// Range index over edge properties.
    EdgeRange {
        /// Edge label to scope the index.
        label: String,
        /// Indexed property name.
        property: String,
    },
    /// Vector index over node properties.
    NodeVector {
        /// Node label to scope the index.
        label: String,
        /// Property name containing vectors.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Text index over node properties.
    NodeText {
        /// Node label to scope the index.
        label: String,
        /// Property name containing text.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Vector index over edge properties.
    EdgeVector {
        /// Edge label to scope the index.
        label: String,
        /// Property name containing vectors.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
    /// Text index over edge properties.
    EdgeText {
        /// Edge label to scope the index.
        label: String,
        /// Property name containing text.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },
}

impl IndexSpec {
    /// Build a node equality index declaration.
    pub fn node_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::NodeEquality {
            label: label.into(),
            property: property.into(),
            unique: false,
        }
    }

    /// Build a unique node equality index declaration.
    pub fn node_unique_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::NodeEquality {
            label: label.into(),
            property: property.into(),
            unique: true,
        }
    }

    /// Build a node range index declaration.
    pub fn node_range(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::NodeRange {
            label: label.into(),
            property: property.into(),
        }
    }

    /// Build an edge equality index declaration.
    pub fn edge_equality(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::EdgeEquality {
            label: label.into(),
            property: property.into(),
        }
    }

    /// Build an edge range index declaration.
    pub fn edge_range(label: impl Into<String>, property: impl Into<String>) -> Self {
        Self::EdgeRange {
            label: label.into(),
            property: property.into(),
        }
    }

    /// Build a node vector index declaration.
    pub fn node_vector(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::NodeVector {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(|value| value.into()),
        }
    }

    /// Build a node text index declaration.
    pub fn node_text(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::NodeText {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(|value| value.into()),
        }
    }

    /// Build an edge vector index declaration.
    pub fn edge_vector(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::EdgeVector {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(|value| value.into()),
        }
    }

    /// Build an edge text index declaration.
    pub fn edge_text(
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Self {
        Self::EdgeText {
            label: label.into(),
            property: property.into(),
            tenant_property: tenant_property.map(|value| value.into()),
        }
    }
}

// Step Enum (AST Nodes)

/// A single step in a traversal AST.
///
/// Most users should build traversals via [`g()`] and the [`Traversal`] builder.
/// This enum exists so the traversal can be inspected, serialized, transported,
/// and reconstructed with [`Traversal::from_steps`].
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Step {
    // Source Steps - Start a traversal or switch context
    /// Start (or switch) on nodes.
    ///
    /// Typical usage is as the first traversal source via [`Traversal::n`].
    N(NodeRef),

    /// Start from nodes matching a [`SourcePredicate`].
    NWhere(SourcePredicate),

    /// Start (or switch) on edges.
    ///
    /// Typical usage is as the first traversal source via [`Traversal::e`].
    E(EdgeRef),

    /// Start from edges matching a [`SourcePredicate`].
    EWhere(SourcePredicate),

    /// Vector similarity search on nodes
    ///
    /// Start traversal from nodes with vectors similar to the query vector.
    /// Uses the HNSW index for the given (label, property) combination.
    ///
    /// Note: this step encodes the nearest-neighbor search inputs.
    /// Implementations may expose ranking and distance metadata at runtime.
    VectorSearchNodes {
        /// The node label to search
        label: String,
        /// The property name containing vectors
        property: String,
        /// Optional multitenant partition value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// The query vector input.
        query_vector: PropertyInput,
        /// Number of nearest neighbors to return.
        k: StreamBound,
    },

    /// BM25 text search on nodes.
    TextSearchNodes {
        /// The node label to search.
        label: String,
        /// The property name containing indexed text.
        property: String,
        /// Optional multitenant partition value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// The query text input.
        query_text: PropertyInput,
        /// Number of ranked results to return.
        k: StreamBound,
    },

    /// Vector similarity search on edges
    ///
    /// Start traversal from edges with vectors similar to the query vector.
    /// Uses the HNSW index for the given (label, property) combination.
    ///
    /// Note: this step encodes the nearest-neighbor search inputs.
    /// Implementations may expose ranking and distance metadata at runtime.
    VectorSearchEdges {
        /// The edge label to search
        label: String,
        /// The property name containing vectors
        property: String,
        /// Optional multitenant partition value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// The query vector input.
        query_vector: PropertyInput,
        /// Number of nearest neighbors to return.
        k: StreamBound,
    },

    /// BM25 text search on edges.
    TextSearchEdges {
        /// The edge label to search.
        label: String,
        /// The property name containing indexed text.
        property: String,
        /// Optional multitenant partition value.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_value: Option<PropertyInput>,
        /// The query text input.
        query_text: PropertyInput,
        /// Number of ranked results to return.
        k: StreamBound,
    },

    // Traversal Steps - Navigate the graph
    /// Traverse outgoing edges, optionally filtered by label.
    ///
    /// Builder forms:
    /// - `.out(Some("KNOWS"))`
    /// - `.out(None::<&str>)` (no label filter)
    Out(Option<String>),

    /// Traverse incoming edges, optionally filtered by label.
    ///
    /// Builder forms:
    /// - `.in_(Some("KNOWS"))`
    /// - `.in_(None::<&str>)`
    In(Option<String>),

    /// Traverse edges in both directions, optionally filtered by label.
    ///
    /// Builder forms:
    /// - `.both(Some("KNOWS"))`
    /// - `.both(None::<&str>)`
    Both(Option<String>),

    // Edge Traversal Steps - Navigate to/from edges
    /// Traverse from nodes to outgoing edges.
    ///
    /// Builder forms:
    /// - `.out_e(Some("KNOWS"))`
    /// - `.out_e(None::<&str>)`
    OutE(Option<String>),

    /// Traverse from nodes to incoming edges.
    ///
    /// Builder forms:
    /// - `.in_e(Some("KNOWS"))`
    /// - `.in_e(None::<&str>)`
    InE(Option<String>),

    /// Traverse from nodes to edges in both directions.
    ///
    /// Builder forms:
    /// - `.both_e(Some("KNOWS"))`
    /// - `.both_e(None::<&str>)`
    BothE(Option<String>),

    /// From an edge stream, switch back to nodes by selecting the edge target.
    ///
    /// Builder form: `.out_n()`
    OutN,

    /// From an edge stream, switch back to nodes by selecting the edge source.
    ///
    /// Builder form: `.in_n()`
    InN,

    /// From an edge stream, switch back to nodes by selecting the "other" endpoint.
    ///
    /// Builder form: `.other_n()`
    OtherN,

    // Filter Steps - Reduce the stream
    /// Filter nodes by property equality: `.has("name", "Alice")`
    Has(String, PropertyValue),

    /// Filter nodes by label: `.has_label("User")`.
    ///
    /// This is shorthand for filtering on the reserved `$label` property.
    HasLabel(String),

    /// Filter nodes by property existence: `.has_key("email")`
    HasKey(String),

    /// Filter nodes by a [`Predicate`]: `.where_(Predicate::gt("age", 18i64))`
    /// or `.where_(Predicate::is_in("status", vec!["active".to_string()]))`
    Where(Predicate),

    /// Remove duplicates: `dedup()`
    Dedup,

    /// Filter to nodes that exist in a variable: `within("x")`
    Within(String),

    /// Filter to nodes that do NOT exist in a variable: `without("x")`
    Without(String),

    // Edge Filter Steps - Filter edges
    /// Filter edges by property equality: `.edge_has("weight", 1i64)`
    EdgeHas(String, PropertyInput),

    /// Filter edges by label: `.edge_has_label("KNOWS")`
    EdgeHasLabel(String),

    // Limit Steps - Control stream size
    /// Take first N items: `limit(10)`
    Limit(usize),

    /// Take first N items using a runtime-resolved expression.
    LimitBy(Expr),

    /// Skip first N items: `skip(5)`
    Skip(usize),

    /// Skip first N items using a runtime-resolved expression.
    SkipBy(Expr),

    /// Get items in range [start, end): equivalent to skip(start).limit(end - start)
    Range(usize, usize),

    /// Get items in range [start, end) using literal and/or runtime-resolved bounds.
    RangeBy(StreamBound, StreamBound),

    // Variable Steps - Store and reference results
    /// Store the current stream in the traversal context under a name.
    ///
    /// Builder form: `.as_("x")`
    As(String),

    /// Store the current stream in the traversal context under a name.
    ///
    /// Builder form: `.store("x")`
    Store(String),

    /// Replace the current node stream with nodes referenced by a stored variable.
    ///
    /// Builder form: `.select("x")`
    Select(String),

    // Terminal Steps - End the traversal
    /// Count results (returns single value)
    Count,

    /// Check if any results exist (returns bool)
    Exists,

    /// Get the ID of current nodes/edges (returns the ID as a value)
    Id,

    /// Get the label of current nodes/edges (returns the $label property)
    Label,

    // Property Projection Steps - Return property data
    /// Return specific node properties.
    ///
    /// Builder form: `.values(vec!["name", "age"])`
    Values(Vec<String>),

    /// Return node properties as maps.
    ///
    /// Builder forms:
    /// - `.value_map(None::<Vec<&str>>)` (all properties)
    /// - `.value_map(Some(vec!["name", "age"]))`
    ValueMap(Option<Vec<String>>),

    /// Project properties and expressions with optional renaming.
    Project(Vec<Projection>),

    /// Return edge properties for the current edge stream.
    ///
    /// Builder form: `.edge_properties()`
    EdgeProperties,

    /// Create a runtime index, treating existing matching definitions as a no-op
    /// when `if_not_exists` is true.
    CreateIndex {
        /// Index specification to create.
        spec: IndexSpec,
        /// Whether duplicate creates should be ignored.
        if_not_exists: bool,
    },

    /// Drop a runtime index.
    DropIndex {
        /// Index specification to drop.
        spec: IndexSpec,
    },

    // Mutation Steps - Modify the graph (write transactions only)
    /// Create a vector index for nodes with the given label and property
    CreateVectorIndexNodes {
        /// Node label to scope the index
        label: String,
        /// Property name containing vectors
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },

    /// Create a vector index for edges with the given label and property
    CreateVectorIndexEdges {
        /// Edge label to scope the index
        label: String,
        /// Property name containing vectors
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },

    /// Create a text index for nodes with the given label and property.
    CreateTextIndexNodes {
        /// Node label to scope the index.
        label: String,
        /// Property name containing text.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },

    /// Create a text index for edges with the given label and property.
    CreateTextIndexEdges {
        /// Edge label to scope the index.
        label: String,
        /// Property name containing text.
        property: String,
        /// Optional multitenant partition property.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tenant_property: Option<String>,
    },

    /// Add a node with a label and properties.
    ///
    /// Builder form: `.add_n("User", vec![("name", "Alice")])`
    /// The node ID is allocated automatically.
    /// The new node becomes the current traversal context.
    AddN {
        /// The node label (required)
        label: String,
        /// Optional properties
        properties: Vec<(String, PropertyInput)>,
    },

    /// Add edges from the current nodes to `to`.
    ///
    /// Builder form: `.add_e("FOLLOWS", to, vec![("weight", 1i64)])`
    AddE {
        /// The edge label (required)
        label: String,
        /// Target nodes (by ID or variable)
        to: NodeRef,
        /// Optional edge properties
        properties: Vec<(String, PropertyInput)>,
    },

    /// Set/update a property on the current nodes: `.set_property(name, value)`
    SetProperty(String, PropertyInput),

    /// Remove a property from the current nodes: `.remove_property(name)`
    RemoveProperty(String),

    /// Delete current nodes (and their edges): `drop()`
    Drop,

    /// Delete edges from the current nodes to a target set: `.drop_edge(target)`
    ///
    /// **Note**: In multigraph scenarios, this removes ALL edges between the current
    /// nodes and the target nodes. Use `DropEdgeById` for precise edge removal.
    DropEdge(NodeRef),

    /// Delete only edges with a specific label from the current nodes to a target set.
    DropEdgeLabeled {
        /// Target nodes to disconnect from.
        to: NodeRef,
        /// Edge label to remove.
        label: String,
    },

    /// Delete specific edges by their IDs: `.drop_edge_by_id(edge_ref)`
    ///
    /// This is the multigraph-safe way to remove edges, as it removes specific
    /// edges rather than all edges between a pair of nodes.
    DropEdgeById(EdgeRef),

    // Ordering Steps - Sort the stream
    /// Order the node stream by a property: `.order_by("age", Order::Desc)`
    OrderBy(String, Order),

    /// Order by multiple properties with priorities
    OrderByMultiple(Vec<(String, Order)>),

    // Loop/Repeat Steps - Iterative traversal
    /// Repeat a traversal body.
    ///
    /// Builder form: `.repeat(RepeatConfig::new(sub().out(None::<&str>)).times(3))`
    Repeat(RepeatConfig),

    // Branching Steps - Conditional execution
    /// Execute multiple sub-traversals and merge their results: `.union(vec![...])`
    Union(Vec<SubTraversal>),

    /// Conditional branching: `choose(predicate, then_traversal, else_traversal)`
    Choose {
        /// Condition to check
        condition: Predicate,
        /// Traversal if condition is true
        then_traversal: SubTraversal,
        /// Traversal if condition is false (optional)
        else_traversal: Option<SubTraversal>,
    },

    /// Try sub-traversals in order until one produces results: `.coalesce(vec![...])`
    Coalesce(Vec<SubTraversal>),

    /// Execute a sub-traversal if it produces results, otherwise pass through: `.optional(t)`
    Optional(SubTraversal),

    // Aggregation Steps - Group and reduce
    /// Group by a property.
    Group(String),

    /// Count occurrences grouped by a property.
    GroupCount(String),

    /// Apply an aggregation function to a property.
    ///
    /// Builder form: `.aggregate_by(AggregateFunction::Sum, "price")`
    AggregateBy(AggregateFunction, String),

    /// Barrier step.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    Fold,

    /// Expand a collected list back into individual items.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    Unfold,

    // Path Steps - Track traversal history
    /// Include the full traversal path in results.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    Path,

    /// Filter to paths without repeated nodes (cycle detection).
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    SimplePath,

    // Sack Steps - Carry state through traversal
    /// Initialize a sack with a value.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    WithSack(PropertyValue),

    /// Update the sack with a property value.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    SackSet(String),

    /// Add to the sack (numeric only).
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    SackAdd(String),

    /// Get the current sack value.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    SackGet,

    // Inject Steps - Add values to the stream
    /// Inject nodes from a variable into the stream.
    ///
    /// As a source step, this starts from the variable's stored node set.
    /// When used mid-traversal, engines may interpret this as a union/merge.
    Inject(String),
}

fn limit_step(bound: impl Into<StreamBound>) -> Step {
    match bound.into() {
        StreamBound::Literal(n) => Step::Limit(n),
        StreamBound::Expr(expr) => Step::LimitBy(expr),
    }
}

fn skip_step(bound: impl Into<StreamBound>) -> Step {
    match bound.into() {
        StreamBound::Literal(n) => Step::Skip(n),
        StreamBound::Expr(expr) => Step::SkipBy(expr),
    }
}

fn range_step(start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Step {
    let start = start.into();
    let end = end.into();
    match (&start, &end) {
        (StreamBound::Literal(start), StreamBound::Literal(end)) => Step::Range(*start, *end),
        _ => Step::RangeBy(start, end),
    }
}

// Traversal with Typestate

/// A complete traversal - a sequence of steps with compile-time state tracking.
///
/// The type parameter `S` tracks what kind of elements the traversal is currently
/// operating on, preventing invalid operation sequences at compile time.
///
/// # State Types
/// - `Empty` - No source step yet, only source operations allowed
/// - `OnNodes` - Currently on node stream, node operations allowed
/// - `OnEdges` - Currently on edge stream, edge operations allowed
/// - `Terminal` - Traversal complete, no more chaining allowed
///
/// The second type parameter `M` tracks mutation capability:
/// - `ReadOnly` - No mutation steps (can be used in read batches)
/// - `WriteEnabled` - Contains mutation steps (requires write batch)
#[derive(Debug, Clone, PartialEq)]
pub struct Traversal<S: TraversalState = OnNodes, M: MutationMode = ReadOnly> {
    /// The steps in this traversal.
    ///
    /// Mutating this vector directly bypasses typestate guarantees enforced by
    /// builder methods. Prefer builder APIs unless you intentionally need to
    /// manipulate raw AST steps.
    #[doc(hidden)]
    pub steps: Vec<Step>,
    /// Phantom data to track the typestate
    _state: PhantomData<S>,
    /// Phantom data to track mutation mode
    _mode: PhantomData<M>,
}

impl<S: TraversalState, M: MutationMode> Default for Traversal<S, M> {
    fn default() -> Self {
        Self {
            steps: Vec::new(),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }
}

impl<S: TraversalState, M: MutationMode> Traversal<S, M> {
    /// Get the steps of this traversal
    #[doc(hidden)]
    pub fn into_steps(self) -> Vec<Step> {
        self.steps
    }

    /// Check if this traversal has a terminal step
    #[doc(hidden)]
    pub fn has_terminal(&self) -> bool {
        self.steps.iter().any(|s| {
            matches!(
                s,
                Step::Count
                    | Step::Exists
                    | Step::Id
                    | Step::Label
                    | Step::Values(_)
                    | Step::ValueMap(_)
                    | Step::Project(_)
                    | Step::EdgeProperties
                    | Step::CreateIndex { .. }
                    | Step::DropIndex { .. }
                    | Step::CreateVectorIndexNodes { .. }
                    | Step::CreateVectorIndexEdges { .. }
                    | Step::CreateTextIndexNodes { .. }
                    | Step::CreateTextIndexEdges { .. }
            )
        })
    }

    /// Create a traversal from steps
    ///
    /// This is useful for batch execution where queries are stored as Vec<Step>
    /// and need to be reconstructed into a Traversal.
    ///
    /// This constructor does not validate that `steps` matches `S`/`M`.
    /// Prefer builder entry points like [`g()`], [`read_batch()`], and [`write_batch()`]
    /// unless you intentionally need a raw reconstruction.
    #[doc(hidden)]
    pub fn from_steps(steps: Vec<Step>) -> Self {
        Self {
            steps,
            _state: PhantomData,
            _mode: PhantomData,
        }
    }

    /// Add a step and transition to a new state (preserving mutation mode)
    fn push_step<T: TraversalState>(mut self, step: Step) -> Traversal<T, M> {
        self.steps.push(step);
        Traversal::from_steps(self.steps)
    }

    /// Add a step and transition to WriteEnabled mode
    fn push_mutation_step<T: TraversalState>(mut self, step: Step) -> Traversal<T, WriteEnabled> {
        self.steps.push(step);
        Traversal::from_steps(self.steps)
    }
}

// Empty State Implementation - Source Steps Only

impl Traversal<Empty, ReadOnly> {
    /// Create a new empty traversal
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            steps: Vec::new(),
            _state: PhantomData,
            _mode: PhantomData,
        }
    }

    // Node Source Steps: Empty -> OnNodes

    /// Start traversal from nodes by IDs or a variable.
    ///
    pub fn n(self, nodes: impl Into<NodeRef>) -> Traversal<OnNodes> {
        self.push_step(Step::N(nodes.into()))
    }

    /// Start traversal from nodes matching a predicate
    ///
    ///
    pub fn n_where(self, predicate: SourcePredicate) -> Traversal<OnNodes> {
        self.push_step(Step::NWhere(predicate))
    }

    /// Start traversal from nodes with a specific label
    ///
    /// This is a convenience method equivalent to `n_where(SourcePredicate::eq("$label", label))`.
    ///
    pub fn n_with_label(self, label: impl Into<String>) -> Traversal<OnNodes> {
        self.n_where(SourcePredicate::Eq(
            "$label".to_string(),
            PropertyValue::String(label.into()),
        ))
    }

    /// Start traversal from nodes with a specific label and additional predicate
    ///
    /// This is a convenience method equivalent to
    /// `n_where(SourcePredicate::and(vec![SourcePredicate::eq("$label", label), predicate]))`.
    ///
    pub fn n_with_label_where(
        self,
        label: impl Into<String>,
        predicate: SourcePredicate,
    ) -> Traversal<OnNodes> {
        self.n_where(SourcePredicate::And(vec![
            SourcePredicate::Eq("$label".to_string(), PropertyValue::String(label.into())),
            predicate,
        ]))
    }

    // Vector Search Source Steps

    /// Start traversal from nodes with vectors similar to the query vector
    ///
    /// Uses the HNSW index for the given (label, property) combination to find
    /// the k nearest neighbors to the query vector.
    ///
    /// Runtime behavior in the current Helix interpreter:
    /// - returns top-k nearest hits (up to `k`) ordered by ascending distance
    /// - `value_map`, `values`, and `project` can read virtual fields `$id` and `$distance`
    /// - after traversing away from the hit stream (for example, `out`/`in_`),
    ///   distance metadata is no longer attached to downstream traversers
    ///
    /// # Arguments
    /// * `label` - The node label to search
    /// * `property` - The property name containing vectors
    /// * `query_vector` - The query vector
    /// * `k` - Number of nearest neighbors to return
    /// * `tenant_value` - Optional multitenant partition value
    ///
    pub fn vector_search_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: Vec<f32>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnNodes> {
        self.vector_search_nodes_with(
            label,
            property,
            query_vector,
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start traversal from nodes with vectors similar to the query vector.
    ///
    /// This variant accepts runtime-resolved inputs for the query vector, result
    /// count, and tenant partition value.
    pub fn vector_search_nodes_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnNodes> {
        self.push_step(Step::VectorSearchNodes {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_vector: query_vector.into(),
            k: k.into(),
        })
    }

    /// Start traversal from nodes matching a text query.
    pub fn text_search_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<String>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnNodes> {
        self.text_search_nodes_with(
            label,
            property,
            PropertyInput::from(query_text.into()),
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start traversal from nodes matching a text query with runtime-resolved inputs.
    pub fn text_search_nodes_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnNodes> {
        self.push_step(Step::TextSearchNodes {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_text: query_text.into(),
            k: k.into(),
        })
    }

    // Edge Source Steps: Empty -> OnEdges

    /// Start traversal from edges by IDs or a variable.
    ///
    pub fn e(self, edges: impl Into<EdgeRef>) -> Traversal<OnEdges> {
        self.push_step(Step::E(edges.into()))
    }

    /// Start traversal from edges matching a predicate
    ///
    ///
    pub fn e_where(self, predicate: SourcePredicate) -> Traversal<OnEdges> {
        self.push_step(Step::EWhere(predicate))
    }

    /// Start traversal from edges with a specific label
    ///
    /// This is a convenience method equivalent to `e_where(SourcePredicate::eq("$label", label))`.
    ///
    pub fn e_with_label(self, label: impl Into<String>) -> Traversal<OnEdges> {
        self.e_where(SourcePredicate::Eq(
            "$label".to_string(),
            PropertyValue::String(label.into()),
        ))
    }

    /// Start traversal from edges with a specific label and additional predicate
    ///
    /// This is a convenience method equivalent to
    /// `e_where(SourcePredicate::and(vec![SourcePredicate::eq("$label", label), predicate]))`.
    ///
    pub fn e_with_label_where(
        self,
        label: impl Into<String>,
        predicate: SourcePredicate,
    ) -> Traversal<OnEdges> {
        self.e_where(SourcePredicate::And(vec![
            SourcePredicate::Eq("$label".to_string(), PropertyValue::String(label.into())),
            predicate,
        ]))
    }

    /// Start traversal from edges with vectors similar to the query vector
    ///
    /// Uses the HNSW index for the given (label, property) combination to find
    /// the k nearest neighbors to the query vector.
    ///
    /// Runtime behavior in the current Helix interpreter:
    /// - returns top-k nearest hits (up to `k`) ordered by ascending distance
    /// - `edge_properties` includes virtual fields `$from`, `$to`, and `$distance`
    ///   (plus `$id` when available)
    ///
    /// # Arguments
    /// * `label` - The edge label to search
    /// * `property` - The property name containing vectors
    /// * `query_vector` - The query vector
    /// * `k` - Number of nearest neighbors to return
    /// * `tenant_value` - Optional multitenant partition value
    ///
    pub fn vector_search_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: Vec<f32>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnEdges> {
        self.vector_search_edges_with(
            label,
            property,
            query_vector,
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start traversal from edges with vectors similar to the query vector.
    ///
    /// This variant accepts runtime-resolved inputs for the query vector, result
    /// count, and tenant partition value.
    pub fn vector_search_edges_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_vector: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnEdges> {
        self.push_step(Step::VectorSearchEdges {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_vector: query_vector.into(),
            k: k.into(),
        })
    }

    /// Start traversal from edges matching a text query.
    pub fn text_search_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<String>,
        k: usize,
        tenant_value: Option<PropertyValue>,
    ) -> Traversal<OnEdges> {
        self.text_search_edges_with(
            label,
            property,
            PropertyInput::from(query_text.into()),
            k,
            tenant_value.map(PropertyInput::from),
        )
    }

    /// Start traversal from edges matching a text query with runtime-resolved inputs.
    pub fn text_search_edges_with(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        query_text: impl Into<PropertyInput>,
        k: impl Into<StreamBound>,
        tenant_value: Option<PropertyInput>,
    ) -> Traversal<OnEdges> {
        self.push_step(Step::TextSearchEdges {
            label: label.into(),
            property: property.into(),
            tenant_value,
            query_text: query_text.into(),
            k: k.into(),
        })
    }

    // Mutation Source Steps: Empty -> OnNodes

    /// Create a runtime index if it does not already exist.
    pub fn create_index_if_not_exists(self, spec: IndexSpec) -> Traversal<Terminal, WriteEnabled> {
        self.push_mutation_step(Step::CreateIndex {
            spec,
            if_not_exists: true,
        })
    }

    /// Drop a runtime index.
    pub fn drop_index(self, spec: IndexSpec) -> Traversal<Terminal, WriteEnabled> {
        self.push_mutation_step(Step::DropIndex { spec })
    }

    /// Create a vector index on nodes.
    ///
    /// This is a write-only source step intended for index management. It does not
    /// produce a useful traversal stream, so the builder marks it as terminal.
    /// Runtime index parameters are selected by the database.
    ///
    pub fn create_vector_index_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::node_vector(label, property, tenant_property))
    }

    /// Create a vector index on edges.
    ///
    /// This is a write-only source step intended for index management. It does not
    /// produce a useful traversal stream, so the builder marks it as terminal.
    /// Runtime index parameters are selected by the database.
    ///
    pub fn create_vector_index_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::edge_vector(label, property, tenant_property))
    }

    /// Create a text index on nodes.
    pub fn create_text_index_nodes(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::node_text(label, property, tenant_property))
    }

    /// Create a text index on edges.
    pub fn create_text_index_edges(
        self,
        label: impl Into<String>,
        property: impl Into<String>,
        tenant_property: Option<impl Into<String>>,
    ) -> Traversal<Terminal, WriteEnabled> {
        self.create_index_if_not_exists(IndexSpec::edge_text(label, property, tenant_property))
    }

    /// Add a new node with a label and optional properties.
    ///
    /// The node ID is automatically allocated.
    ///
    /// In the current Helix interpreter, this step creates exactly one node and
    /// starts the traversal from that node.
    ///
    pub fn add_n<K, V>(
        self,
        label: impl Into<String>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        let props: Vec<(String, PropertyInput)> = properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self.push_mutation_step(Step::AddN {
            label: label.into(),
            properties: props,
        })
    }

    /// Start from nodes stored in a variable (Empty -> OnNodes).
    ///
    /// This is a convenience for starting from a node set previously saved via
    /// `store()` / `as_()` in the same traversal context.
    ///
    /// If you want to start from a variable that yields node IDs via other means
    /// (for example, an `id()` terminal result), prefer `n(NodeRef::var(name))`.
    ///
    pub fn inject(self, var_name: impl Into<String>) -> Traversal<OnNodes, ReadOnly> {
        self.push_step(Step::Inject(var_name.into()))
    }

    /// Delete specific edges by their IDs without needing a source
    ///
    /// This is the multigraph-safe way to remove edges, as it removes specific
    /// edges rather than all edges between a pair of nodes.
    ///
    pub fn drop_edge_by_id(self, edges: impl Into<EdgeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::DropEdgeById(edges.into()))
    }
}

// OnNodes State Implementation

impl<M: MutationMode> Traversal<OnNodes, M> {
    // Navigation Steps: OnNodes -> OnNodes

    /// Traverse outgoing edges, optionally filtered by label
    pub fn out(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push_step(Step::Out(label.map(|l| l.into())))
    }

    /// Traverse incoming edges, optionally filtered by label
    pub fn in_(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push_step(Step::In(label.map(|l| l.into())))
    }

    /// Traverse edges in both directions, optionally filtered by label
    pub fn both(self, label: Option<impl Into<String>>) -> Traversal<OnNodes, M> {
        self.push_step(Step::Both(label.map(|l| l.into())))
    }

    // Edge Traversal Steps: OnNodes -> OnEdges

    /// Traverse to outgoing edges
    pub fn out_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push_step(Step::OutE(label.map(|l| l.into())))
    }

    /// Traverse to incoming edges
    pub fn in_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push_step(Step::InE(label.map(|l| l.into())))
    }

    /// Traverse to edges in both directions
    pub fn both_e(self, label: Option<impl Into<String>>) -> Traversal<OnEdges, M> {
        self.push_step(Step::BothE(label.map(|l| l.into())))
    }

    // Node Filter Steps: OnNodes -> OnNodes

    /// Filter by property value
    pub fn has(self, property: impl Into<String>, value: impl Into<PropertyValue>) -> Self {
        self.push_step(Step::Has(property.into(), value.into()))
    }

    /// Filter by label (shorthand for has("$label", value))
    pub fn has_label(self, label: impl Into<String>) -> Self {
        self.push_step(Step::HasLabel(label.into()))
    }

    /// Filter by property existence
    pub fn has_key(self, property: impl Into<String>) -> Self {
        self.push_step(Step::HasKey(property.into()))
    }

    /// Filter by a complex predicate
    pub fn where_(self, predicate: Predicate) -> Self {
        self.push_step(Step::Where(predicate))
    }

    /// Remove duplicates from the stream
    pub fn dedup(self) -> Self {
        self.push_step(Step::Dedup)
    }

    /// Filter to nodes that exist in a variable
    pub fn within(self, var_name: impl Into<String>) -> Self {
        self.push_step(Step::Within(var_name.into()))
    }

    /// Filter to nodes that do NOT exist in a variable
    pub fn without(self, var_name: impl Into<String>) -> Self {
        self.push_step(Step::Without(var_name.into()))
    }

    // Limit Steps: OnNodes -> OnNodes

    /// Take at most N items.
    pub fn limit(self, n: impl Into<StreamBound>) -> Self {
        self.push_step(limit_step(n))
    }

    /// Skip the first N items.
    pub fn skip(self, n: impl Into<StreamBound>) -> Self {
        self.push_step(skip_step(n))
    }

    /// Get items in a range [start, end)
    ///
    /// Equivalent to `.skip(start).limit(end - start)` but more concise.
    ///
    pub fn range(self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.push_step(range_step(start, end))
    }

    // Variable Steps: OnNodes -> OnNodes

    /// Store the current node stream in the traversal context under `name`.
    ///
    /// This is identical to `store()`; it exists for Gremlin-style naming.
    pub fn as_(self, name: impl Into<String>) -> Self {
        self.push_step(Step::As(name.into()))
    }

    /// Store the current node stream in the traversal context under `name`.
    ///
    /// This does not change the current stream; it only creates/overwrites a
    /// named binding that later steps can reference.
    pub fn store(self, name: impl Into<String>) -> Self {
        self.push_step(Step::Store(name.into()))
    }

    /// Replace the current node stream with nodes referenced by a variable.
    ///
    /// Use this when you want to *switch* streams. If you want to *merge* a stored
    /// node set into the current stream, use `inject()`.
    pub fn select(self, name: impl Into<String>) -> Self {
        self.push_step(Step::Select(name.into()))
    }

    /// Union the current node stream with nodes stored in `var_name`.
    ///
    /// This keeps the current stream and adds any nodes stored in the named
    /// variable. Use `select()` to replace the stream instead.
    ///
    pub fn inject(self, var_name: impl Into<String>) -> Self {
        self.push_step(Step::Inject(var_name.into()))
    }

    // Terminal Steps: OnNodes -> Terminal

    /// Count the number of results
    pub fn count(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Count)
    }

    /// Check if any results exist
    pub fn exists(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Exists)
    }

    /// Get the ID of current nodes
    ///
    /// Returns the node ID as a value. Useful when you need
    /// to extract just the ID without other properties.
    ///
    pub fn id(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Id)
    }

    /// Get the label of current nodes
    ///
    /// Returns the $label property value.
    ///
    pub fn label(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Label)
    }

    /// Get specific property values from current nodes
    pub fn values(self, properties: Vec<impl Into<String>>) -> Traversal<Terminal, M> {
        self.push_step(Step::Values(
            properties.into_iter().map(|p| p.into()).collect(),
        ))
    }

    /// Get properties as a map, optionally filtered to specific properties
    pub fn value_map(self, properties: Option<Vec<impl Into<String>>>) -> Traversal<Terminal, M> {
        self.push_step(Step::ValueMap(
            properties.map(|ps| ps.into_iter().map(|p| p.into()).collect()),
        ))
    }

    /// Project properties and expressions with optional renaming.
    pub fn project<P>(self, projections: Vec<P>) -> Traversal<Terminal, M>
    where
        P: Into<Projection>,
    {
        self.push_step(Step::Project(
            projections.into_iter().map(Into::into).collect(),
        ))
    }

    // Ordering Steps: OnNodes -> OnNodes

    /// Order results by a property.
    ///
    /// Note: some interpreters represent intermediate streams as sets. In those
    /// engines, ordering may not be preserved in the returned node set.
    ///
    pub fn order_by(self, property: impl Into<String>, order: Order) -> Self {
        self.push_step(Step::OrderBy(property.into(), order))
    }

    /// Order results by multiple properties with priorities.
    ///
    /// Note: some interpreters represent intermediate streams as sets. In those
    /// engines, ordering may not be preserved in the returned node set.
    ///
    pub fn order_by_multiple(self, orderings: Vec<(impl Into<String>, Order)>) -> Self {
        let orderings: Vec<(String, Order)> =
            orderings.into_iter().map(|(p, o)| (p.into(), o)).collect();
        self.push_step(Step::OrderByMultiple(orderings))
    }

    // Loop/Repeat Steps: OnNodes -> OnNodes

    /// Repeat a traversal with configuration
    ///
    pub fn repeat(self, config: RepeatConfig) -> Self {
        self.push_step(Step::Repeat(config))
    }

    // Branching Steps: OnNodes -> OnNodes

    /// Execute multiple traversals and merge their results
    ///
    pub fn union(self, traversals: Vec<SubTraversal>) -> Self {
        self.push_step(Step::Union(traversals))
    }

    /// Conditional execution based on a predicate
    ///
    pub fn choose(
        self,
        condition: Predicate,
        then_traversal: SubTraversal,
        else_traversal: Option<SubTraversal>,
    ) -> Self {
        self.push_step(Step::Choose {
            condition,
            then_traversal,
            else_traversal,
        })
    }

    /// Try traversals in order until one produces results
    ///
    pub fn coalesce(self, traversals: Vec<SubTraversal>) -> Self {
        self.push_step(Step::Coalesce(traversals))
    }

    /// Execute a traversal per input item and fall back to the original item when that
    /// input produces no results.
    ///
    /// Note: when the optional branch changes the runtime stream family (for example,
    /// nodes to edges), unmatched inputs drop out of that branch result instead of
    /// producing nullable row bindings.
    ///
    pub fn optional(self, traversal: SubTraversal) -> Self {
        self.push_step(Step::Optional(traversal))
    }

    // Aggregation Steps: OnNodes -> OnNodes (or Terminal for some)

    /// Group nodes by a property value.
    pub fn group(self, property: impl Into<String>) -> Traversal<Terminal, M> {
        self.push_step(Step::Group(property.into()))
    }

    /// Count occurrences grouped by a property.
    pub fn group_count(self, property: impl Into<String>) -> Traversal<Terminal, M> {
        self.push_step(Step::GroupCount(property.into()))
    }

    /// Apply an aggregation function to a property.
    pub fn aggregate_by(
        self,
        function: AggregateFunction,
        property: impl Into<String>,
    ) -> Traversal<Terminal, M> {
        self.push_step(Step::AggregateBy(function, property.into()))
    }

    /// Barrier step.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn fold(self) -> Self {
        self.push_step(Step::Fold)
    }

    /// Expand a collected list back into individual items.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn unfold(self) -> Self {
        self.push_step(Step::Unfold)
    }

    // Path Steps: OnNodes -> OnNodes

    /// Include the full traversal path in results.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    ///
    pub fn path(self) -> Self {
        self.push_step(Step::Path)
    }

    /// Filter to only simple paths (no repeated nodes).
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn simple_path(self) -> Self {
        self.push_step(Step::SimplePath)
    }

    // Sack Steps: OnNodes -> OnNodes

    /// Initialize a sack (traverser-local state) with a value.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    ///
    pub fn with_sack(self, initial: PropertyValue) -> Self {
        self.push_step(Step::WithSack(initial))
    }

    /// Set the sack to a property value from the current node.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn sack_set(self, property: impl Into<String>) -> Self {
        self.push_step(Step::SackSet(property.into()))
    }

    /// Add a property value to the sack (numeric types only).
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn sack_add(self, property: impl Into<String>) -> Self {
        self.push_step(Step::SackAdd(property.into()))
    }

    /// Get the current sack value.
    ///
    /// Note: this step is reserved; the current Helix interpreter treats it as a no-op.
    pub fn sack_get(self) -> Self {
        self.push_step(Step::SackGet)
    }

    // Mutation Steps: OnNodes -> OnNodes (WriteEnabled)

    /// Add a new node with a label and optional properties.
    ///
    /// The node ID is automatically allocated.
    ///
    /// In the current Helix interpreter, this step creates exactly one node and
    /// replaces the current node stream with that new node.
    pub fn add_n<K, V>(
        self,
        label: impl Into<String>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        let props: Vec<(String, PropertyInput)> = properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self.push_mutation_step(Step::AddN {
            label: label.into(),
            properties: props,
        })
    }

    /// Add edges from the current nodes to target nodes.
    ///
    /// In the current Helix interpreter, this creates edges for every pair in the
    /// cartesian product `current_nodes x target_nodes` and leaves the current
    /// node stream unchanged.
    ///
    pub fn add_e<K, V>(
        self,
        label: impl Into<String>,
        to: impl Into<NodeRef>,
        properties: Vec<(K, V)>,
    ) -> Traversal<OnNodes, WriteEnabled>
    where
        K: Into<String>,
        V: Into<PropertyInput>,
    {
        let props: Vec<(String, PropertyInput)> = properties
            .into_iter()
            .map(|(k, v)| (k.into(), v.into()))
            .collect();
        self.push_mutation_step(Step::AddE {
            label: label.into(),
            to: to.into(),
            properties: props,
        })
    }

    /// Set a property on current nodes
    pub fn set_property(
        self,
        name: impl Into<String>,
        value: impl Into<PropertyInput>,
    ) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::SetProperty(name.into(), value.into()))
    }

    /// Remove a property from current nodes
    pub fn remove_property(self, name: impl Into<String>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::RemoveProperty(name.into()))
    }

    /// Delete current nodes and their edges
    pub fn drop(self) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::Drop)
    }

    /// Delete edges from current nodes to target nodes
    ///
    /// **Note**: In multigraph scenarios, this removes ALL edges between the current
    /// nodes and the target nodes. Use `drop_edge_by_id` for precise edge removal.
    pub fn drop_edge(self, to: impl Into<NodeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::DropEdge(to.into()))
    }

    /// Delete only edges with a specific label from current nodes to target nodes.
    pub fn drop_edge_labeled(
        self,
        to: impl Into<NodeRef>,
        label: impl Into<String>,
    ) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::DropEdgeLabeled {
            to: to.into(),
            label: label.into(),
        })
    }

    /// Delete specific edges by their IDs
    ///
    /// This is the multigraph-safe way to remove edges, as it removes specific
    /// edges rather than all edges between a pair of nodes.
    ///
    pub fn drop_edge_by_id(self, edges: impl Into<EdgeRef>) -> Traversal<OnNodes, WriteEnabled> {
        self.push_mutation_step(Step::DropEdgeById(edges.into()))
    }
}

// OnEdges State Implementation

impl<M: MutationMode> Traversal<OnEdges, M> {
    // Node Extraction Steps: OnEdges -> OnNodes

    /// From edge, get the target node
    pub fn out_n(self) -> Traversal<OnNodes, M> {
        self.push_step(Step::OutN)
    }

    /// From edge, get the source node
    pub fn in_n(self) -> Traversal<OnNodes, M> {
        self.push_step(Step::InN)
    }

    /// From edge, get the "other" node (not the one we came from)
    pub fn other_n(self) -> Traversal<OnNodes, M> {
        self.push_step(Step::OtherN)
    }

    // Edge Filter Steps: OnEdges -> OnEdges

    /// Filter edges by property value
    pub fn edge_has(self, property: impl Into<String>, value: impl Into<PropertyInput>) -> Self {
        self.push_step(Step::EdgeHas(property.into(), value.into()))
    }

    /// Filter edges by label
    pub fn edge_has_label(self, label: impl Into<String>) -> Self {
        self.push_step(Step::EdgeHasLabel(label.into()))
    }

    /// Remove duplicates from the stream
    pub fn dedup(self) -> Self {
        self.push_step(Step::Dedup)
    }

    // Limit Steps: OnEdges -> OnEdges

    /// Take at most N items.
    pub fn limit(self, n: impl Into<StreamBound>) -> Self {
        self.push_step(limit_step(n))
    }

    /// Skip the first N items.
    pub fn skip(self, n: impl Into<StreamBound>) -> Self {
        self.push_step(skip_step(n))
    }

    /// Get items in a range [start, end)
    pub fn range(self, start: impl Into<StreamBound>, end: impl Into<StreamBound>) -> Self {
        self.push_step(range_step(start, end))
    }

    // Variable Steps: OnEdges -> OnEdges

    /// Store current edges with a name for later reference
    pub fn as_(self, name: impl Into<String>) -> Self {
        self.push_step(Step::As(name.into()))
    }

    /// Store current edges to a variable (same as `as_`)
    pub fn store(self, name: impl Into<String>) -> Self {
        self.push_step(Step::Store(name.into()))
    }

    // Terminal Steps: OnEdges -> Terminal

    /// Count the number of edges
    pub fn count(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Count)
    }

    /// Check if any edges exist
    pub fn exists(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Exists)
    }

    /// Get the ID of current edges
    pub fn id(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Id)
    }

    /// Get the label of current edges
    pub fn label(self) -> Traversal<Terminal, M> {
        self.push_step(Step::Label)
    }

    /// Get edge properties
    pub fn edge_properties(self) -> Traversal<Terminal, M> {
        self.push_step(Step::EdgeProperties)
    }

    // Ordering Steps: OnEdges -> OnEdges

    /// Order results by a property.
    ///
    /// Note: some interpreters represent intermediate streams as sets. In those
    /// engines, ordering may not be preserved in the returned edge set.
    pub fn order_by(self, property: impl Into<String>, order: Order) -> Self {
        self.push_step(Step::OrderBy(property.into(), order))
    }
}

// Terminal State - No additional methods (traversal is complete)

// Terminal has no additional methods - the traversal is complete

// Entry Point

/// Create a new traversal - the entry point for building queries
///
pub fn g() -> Traversal<Empty> {
    Traversal::new()
}

// Batch Query Types

/// Condition for conditional query execution within a batch
///
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BatchCondition {
    /// Execute only if the named variable is not empty
    VarNotEmpty(String),
    /// Execute only if the named variable is empty
    VarEmpty(String),
    /// Execute only if the named variable has at least N items
    VarMinSize(String, usize),
    /// Execute only if the previous query result was not empty
    PrevNotEmpty,
}

/// A single query within a batch
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NamedQuery {
    /// Variable name to store result (required for var_as)
    pub name: Option<String>,
    /// The traversal steps to execute for this query.
    pub steps: Vec<Step>,
    /// Skip if condition fails
    pub condition: Option<BatchCondition>,
}

/// A batch entry executed in sequence.
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BatchEntry {
    /// Execute a single traversal query.
    Query(NamedQuery),
    /// Execute the enclosed entries once per object in the named array param.
    ForEach {
        /// The top-level parameter containing an array of objects.
        param: String,
        /// Entries to execute for each object.
        body: Vec<BatchEntry>,
    },
}

/// A batch of read-only queries for sequential execution in one transaction
///
/// This allows multiple related read queries to be executed atomically,
/// with results stored in named variables that can be referenced
/// by subsequent queries and returned as a structured result.
///
/// **Important**: ReadBatch only accepts read-only traversals (no mutations).
/// Attempting to add a traversal containing mutation steps will fail at compile time.
///
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct ReadBatch {
    /// Queries to execute in order.
    ///
    /// Most users should build this with [`ReadBatch::var_as`] / [`ReadBatch::var_as_if`]
    /// to preserve type-level read-only guarantees.
    #[doc(hidden)]
    pub queries: Vec<BatchEntry>,
    /// Variables to include in final result (empty = all named variables)
    #[doc(hidden)]
    pub returns: Vec<String>,
}

impl ReadBatch {
    /// Create a new empty read batch
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            queries: Vec::new(),
            returns: Vec::new(),
        }
    }

    /// Add a read-only query that stores result in a named variable
    ///
    /// The traversal is executed and its result is stored in a variable
    /// that can be referenced by subsequent queries using `NodeRef::var()`.
    ///
    /// **Note**: Only accepts read-only traversals. Mutation traversals will fail at compile time.
    ///
    pub fn var_as<S: TraversalState>(
        mut self,
        name: &str,
        traversal: Traversal<S, ReadOnly>,
    ) -> Self {
        self.queries.push(BatchEntry::Query(NamedQuery {
            name: Some(name.to_string()),
            steps: traversal.into_steps(),
            condition: None,
        }));
        self
    }

    /// Add a conditional read-only query that only executes if the condition is met
    ///
    pub fn var_as_if<S: TraversalState>(
        mut self,
        name: &str,
        condition: BatchCondition,
        traversal: Traversal<S, ReadOnly>,
    ) -> Self {
        self.queries.push(BatchEntry::Query(NamedQuery {
            name: Some(name.to_string()),
            steps: traversal.into_steps(),
            condition: Some(condition),
        }));
        self
    }

    /// Execute the provided body once per object in the named array parameter.
    pub fn for_each_param(mut self, param: &str, body: ReadBatch) -> Self {
        self.queries.push(BatchEntry::ForEach {
            param: param.to_string(),
            body: body.queries,
        });
        self
    }

    /// Specify which variables to return (call at end)
    ///
    /// If not called, all named variables are returned.
    ///
    pub fn returning<I, S>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.returns = vars.into_iter().map(|s| s.into()).collect();
        self
    }
}

/// A batch of write queries for sequential execution in one transaction
///
/// This allows multiple related queries (including mutations) to be executed atomically,
/// with results stored in named variables that can be referenced
/// by subsequent queries and returned as a structured result.
///
/// **Note**: WriteBatch accepts both read-only and mutation traversals.
///
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
pub struct WriteBatch {
    /// Queries to execute in order.
    ///
    /// Most users should build this with [`WriteBatch::var_as`] / [`WriteBatch::var_as_if`]
    /// for clearer intent and safer construction.
    #[doc(hidden)]
    pub queries: Vec<BatchEntry>,
    /// Variables to include in final result (empty = all named variables)
    #[doc(hidden)]
    pub returns: Vec<String>,
}

impl WriteBatch {
    /// Create a new empty write batch
    #[doc(hidden)]
    pub fn new() -> Self {
        Self {
            queries: Vec::new(),
            returns: Vec::new(),
        }
    }

    /// Add a query that stores result in a named variable
    ///
    /// The traversal is executed and its result is stored in a variable
    /// that can be referenced by subsequent queries using `NodeRef::var()`.
    ///
    /// Accepts both read-only and mutation traversals.
    ///
    pub fn var_as<S: TraversalState, M: MutationMode>(
        mut self,
        name: &str,
        traversal: Traversal<S, M>,
    ) -> Self {
        self.queries.push(BatchEntry::Query(NamedQuery {
            name: Some(name.to_string()),
            steps: traversal.into_steps(),
            condition: None,
        }));
        self
    }

    /// Add a conditional query that only executes if the condition is met
    ///
    pub fn var_as_if<S: TraversalState, M: MutationMode>(
        mut self,
        name: &str,
        condition: BatchCondition,
        traversal: Traversal<S, M>,
    ) -> Self {
        self.queries.push(BatchEntry::Query(NamedQuery {
            name: Some(name.to_string()),
            steps: traversal.into_steps(),
            condition: Some(condition),
        }));
        self
    }

    /// Execute the provided body once per object in the named array parameter.
    pub fn for_each_param(mut self, param: &str, body: WriteBatch) -> Self {
        self.queries.push(BatchEntry::ForEach {
            param: param.to_string(),
            body: body.queries,
        });
        self
    }

    /// Specify which variables to return (call at end)
    ///
    /// If not called, all named variables are returned.
    ///
    pub fn returning<I, S>(mut self, vars: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.returns = vars.into_iter().map(|s| s.into()).collect();
        self
    }
}

/// A batch query payload for wire transport or storage
#[doc(hidden)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum BatchQuery {
    /// Read-only batch
    Read(ReadBatch),
    /// Write-capable batch
    Write(WriteBatch),
}

/// Errors returned while building or serializing a dynamic query request payload.
#[derive(Debug)]
pub enum DynamicQueryError {
    /// Failed to serialize the request to JSON.
    Serialize(sonic_rs::Error),
    /// Failed to decode serialized JSON bytes as UTF-8.
    Utf8(std::string::FromUtf8Error),
    /// Dynamic query JSON cannot faithfully represent a bytes parameter.
    UnsupportedBytesParameter(String),
    /// A datetime parameter could not be rendered as RFC3339.
    InvalidDateTimeParameter {
        /// Parameter path within the request payload.
        path: String,
        /// Raw UTC epoch milliseconds that failed to render.
        millis: i64,
    },
}

impl DynamicQueryError {
    /// Build an error for a bytes parameter path that cannot be represented safely.
    pub fn unsupported_bytes(path: impl Into<String>) -> Self {
        Self::UnsupportedBytesParameter(path.into())
    }

    /// Build an error for a datetime value that cannot be rendered safely.
    pub fn invalid_datetime(path: impl Into<String>, millis: i64) -> Self {
        Self::InvalidDateTimeParameter {
            path: path.into(),
            millis,
        }
    }
}

impl std::fmt::Display for DynamicQueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Serialize(err) => write!(f, "json serialization error: {err}"),
            Self::Utf8(err) => write!(f, "utf8 conversion error: {err}"),
            Self::UnsupportedBytesParameter(path) => write!(
                f,
                "parameter '{path}' uses bytes, which the dynamic query JSON route cannot represent"
            ),
            Self::InvalidDateTimeParameter { path, millis } => write!(
                f,
                "parameter '{path}' uses datetime millis '{millis}', which cannot be rendered as RFC3339"
            ),
        }
    }
}

impl std::error::Error for DynamicQueryError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Serialize(err) => Some(err),
            Self::Utf8(err) => Some(err),
            Self::UnsupportedBytesParameter(_) => None,
            Self::InvalidDateTimeParameter { .. } => None,
        }
    }
}

impl From<sonic_rs::Error> for DynamicQueryError {
    fn from(value: sonic_rs::Error) -> Self {
        Self::Serialize(value)
    }
}

impl From<std::string::FromUtf8Error> for DynamicQueryError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self::Utf8(value)
    }
}

/// Request type accepted by the gateway dynamic query route.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DynamicQueryRequestType {
    /// Read-only dynamic query request.
    Read,
    /// Write-capable dynamic query request.
    Write,
}

/// JSON-compatible parameter value for a dynamic query request payload.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DynamicQueryValue {
    /// Null JSON value.
    Null,
    /// Boolean JSON value.
    Bool(bool),
    /// 64-bit signed integer JSON value.
    I64(i64),
    /// 64-bit floating-point JSON value.
    F64(f64),
    /// 32-bit floating-point JSON value.
    F32(f32),
    /// UTF-8 string JSON value.
    String(String),
    /// Array JSON value.
    Array(Vec<DynamicQueryValue>),
    /// Object JSON value.
    Object(BTreeMap<String, DynamicQueryValue>),
}

/// Full JSON payload accepted by the gateway dynamic query route.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DynamicQueryRequest {
    /// Whether the inline query should execute as a read or write.
    #[serde(rename = "request_type")]
    pub request_type: DynamicQueryRequestType,
    /// Inline query AST payload.
    pub query: BatchQuery,
    /// Runtime parameters forwarded to the query engine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<BTreeMap<String, DynamicQueryValue>>,
    /// Optional parameter schema used by runtimes to coerce typed inputs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameter_types: Option<BTreeMap<String, QueryParamType>>,
}

impl DynamicQueryRequest {
    fn new(request_type: DynamicQueryRequestType, query: BatchQuery) -> Self {
        Self {
            request_type,
            query,
            parameters: None,
            parameter_types: None,
        }
    }

    /// Create a dynamic request payload for a read batch.
    pub fn read(query: ReadBatch) -> Self {
        Self::new(DynamicQueryRequestType::Read, BatchQuery::Read(query))
    }

    /// Create a dynamic request payload for a write batch.
    pub fn write(query: WriteBatch) -> Self {
        Self::new(DynamicQueryRequestType::Write, BatchQuery::Write(query))
    }

    /// Insert a named parameter value into the request payload.
    pub fn insert_parameter_value(&mut self, name: impl Into<String>, value: DynamicQueryValue) {
        self.parameters
            .get_or_insert_with(BTreeMap::new)
            .insert(name.into(), value);
    }

    /// Insert a named parameter type into the request payload.
    pub fn insert_parameter_type(&mut self, name: impl Into<String>, ty: QueryParamType) {
        self.parameter_types
            .get_or_insert_with(BTreeMap::new)
            .insert(name.into(), ty);
    }

    /// Insert a named parameter value and return the updated request.
    pub fn with_parameter_value(
        mut self,
        name: impl Into<String>,
        value: DynamicQueryValue,
    ) -> Self {
        self.insert_parameter_value(name, value);
        self
    }

    /// Insert a named parameter type and return the updated request.
    pub fn with_parameter_type(mut self, name: impl Into<String>, ty: QueryParamType) -> Self {
        self.insert_parameter_type(name, ty);
        self
    }

    /// Serialize the request payload to JSON bytes.
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, DynamicQueryError> {
        Ok(sonic_rs::to_vec(self)?)
    }

    /// Serialize the request payload to a JSON string.
    pub fn to_json_string(&self) -> Result<String, DynamicQueryError> {
        Ok(String::from_utf8(self.to_json_bytes()?)?)
    }
}

/// Create a new read batch - the entry point for read-only multi-query transactions
///
pub fn read_batch() -> ReadBatch {
    ReadBatch::new()
}

/// Create a new write batch - the entry point for write multi-query transactions
///
pub fn write_batch() -> WriteBatch {
    WriteBatch::new()
}

/// Common query-builder imports.
///
/// This module re-exports the APIs most users reach for when building read and
/// write batches.
///
/// Typical usage in application code: `use helix_db::dsl::prelude::*;`
#[allow(missing_docs)]
pub mod prelude {
    pub use crate::{
        g, read_batch, register, sub, write_batch, AggregateFunction, BatchCondition, BatchEntry,
        CompareOp, DateTime, DynamicQueryError, DynamicQueryRequest, DynamicQueryRequestType,
        DynamicQueryValue, EdgeId, EdgeRef, EmitBehavior, Expr, ExprProjection, IndexSpec, NodeId,
        NodeRef, Order, ParamObject, ParamValue, Predicate, Projection, PropertyInput,
        PropertyProjection, PropertyValue, ReadBatch, RepeatConfig, SourcePredicate, StreamBound,
        SubTraversal, Traversal, WriteBatch,
    };
    // query bundle generation
    pub use crate::{
        generate, generate_to_path, GenerateError, QueryBundle, QueryParamType, QueryParameter,
    };
}

/// Helper type alias for property maps
#[doc(hidden)]
pub type PropertyMap = HashMap<String, PropertyValue>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use crate::query_generator::{
        deserialize_query_bundle, serialize_query_bundle, GenerateError, QueryBundle,
        QueryParamType, QueryParameter, QUERY_BUNDLE_VERSION,
    };

    use super::*;

    fn query_entry(entry: &BatchEntry) -> &NamedQuery {
        match entry {
            BatchEntry::Query(query) => query,
            other => panic!("expected query entry, got {other:?}"),
        }
    }

    #[test]
    fn query_bundle_roundtrip_with_bincode_fixint() {
        let mut bundle = QueryBundle::default();
        bundle.read_routes.insert(
            "read_users".to_string(),
            read_batch().var_as(
                "count",
                g().n_with_label("User")
                    .where_(Predicate::is_in(
                        "status",
                        vec!["active".to_string(), "pending".to_string()],
                    ))
                    .count(),
            ),
        );
        bundle.read_parameters.insert(
            "read_users".to_string(),
            vec![QueryParameter {
                name: "filters".to_string(),
                ty: QueryParamType::Object,
            }],
        );
        bundle.write_routes.insert(
            "create_user".to_string(),
            write_batch().var_as("created", g().add_n("User", vec![("name", "Alice")])),
        );
        bundle.write_parameters.insert(
            "create_user".to_string(),
            vec![QueryParameter {
                name: "data".to_string(),
                ty: QueryParamType::Array(Box::new(QueryParamType::Object)),
            }],
        );

        let bytes = serialize_query_bundle(&bundle).expect("serialize query bundle");
        let decoded = deserialize_query_bundle(&bytes).expect("deserialize query bundle");

        assert_eq!(decoded.version, QUERY_BUNDLE_VERSION);
        assert_eq!(decoded.read_routes.len(), 1);
        assert_eq!(decoded.write_routes.len(), 1);
        assert!(decoded.read_routes.contains_key("read_users"));
        assert!(decoded.write_routes.contains_key("create_user"));
        assert_eq!(
            decoded.read_parameters.get("read_users"),
            Some(&vec![QueryParameter {
                name: "filters".to_string(),
                ty: QueryParamType::Object,
            }])
        );
        assert_eq!(
            decoded.write_parameters.get("create_user"),
            Some(&vec![QueryParameter {
                name: "data".to_string(),
                ty: QueryParamType::Array(Box::new(QueryParamType::Object)),
            }])
        );
    }

    #[test]
    fn query_bundle_rejects_unsupported_version() {
        let mut bundle = QueryBundle::default();
        bundle.version = QUERY_BUNDLE_VERSION + 1;

        let bytes = serialize_query_bundle(&bundle).expect("serialize query bundle");
        let err = deserialize_query_bundle(&bytes).expect_err("version should fail");

        assert!(matches!(
            err,
            GenerateError::UnsupportedVersion {
                found: _,
                expected: QUERY_BUNDLE_VERSION,
            }
        ));
    }

    #[test]
    fn test_traversal_builder() {
        let t = g()
            .n([1u64, 2, 3])
            .out(Some("FOLLOWS"))
            .has("active", "true")
            .limit(10);

        assert_eq!(t.steps.len(), 4);
        assert!(matches!(&t.steps[0], Step::N(NodeRef::Ids(ids)) if ids == &vec![1, 2, 3]));
        assert!(matches!(&t.steps[1], Step::Out(Some(label)) if label == "FOLLOWS"));
    }

    #[test]
    fn test_variable_steps() {
        let t = g()
            .n([1u64])
            .out(None::<String>)
            .as_("neighbors")
            .out(None::<String>)
            .within("neighbors");

        assert_eq!(t.steps.len(), 5);
        assert!(matches!(&t.steps[2], Step::As(name) if name == "neighbors"));
        assert!(matches!(&t.steps[4], Step::Within(name) if name == "neighbors"));
    }

    #[test]
    fn test_terminal_detection() {
        let t1 = g().n([1u64]).out(None::<String>);
        assert!(!t1.has_terminal());

        let t2 = g().n([1u64]).count();
        assert!(t2.has_terminal());

        let t3 = g().n([1u64]).exists();
        assert!(t3.has_terminal());
    }

    #[test]
    fn test_node_ref_from_impls() {
        let all = NodeRef::all();
        assert!(matches!(all, NodeRef::All));

        let r1: NodeRef = 42u64.into();
        assert!(matches!(r1, NodeRef::Ids(ids) if ids == vec![42]));

        let r2: NodeRef = vec![1u64, 2, 3].into();
        assert!(matches!(r2, NodeRef::Ids(ids) if ids == vec![1, 2, 3]));

        let r3: NodeRef = "my_var".into();
        assert!(matches!(r3, NodeRef::Var(name) if name == "my_var"));

        let r4 = NodeRef::param("node_id");
        assert!(matches!(r4, NodeRef::Param(name) if name == "node_id"));
    }

    #[test]
    fn test_edge_ref_param_constructor() {
        let r = EdgeRef::param("edge_id");
        assert!(matches!(r, EdgeRef::Param(name) if name == "edge_id"));
    }

    #[test]
    fn test_add_n_and_add_e() {
        let t = g()
            .add_n("User", vec![("name", "Alice")])
            .as_("alice")
            .add_n("User", vec![("name", "Bob")])
            .add_e("KNOWS", NodeRef::var("alice"), vec![("since", "2024")]);

        assert_eq!(t.steps.len(), 4);
        assert!(
            matches!(&t.steps[0], Step::AddN { label, properties } if label == "User" && properties.len() == 1)
        );
        assert!(
            matches!(&t.steps[3], Step::AddE { label, to: NodeRef::Var(name), .. } if label == "KNOWS" && name == "alice")
        );
    }

    #[test]
    fn test_predicate_builder() {
        let p1 = Predicate::eq("name", "Alice");
        assert!(
            matches!(p1, Predicate::Eq(prop, PropertyValue::String(val)) if prop == "name" && val == "Alice")
        );

        let p_expr = Predicate::eq("name", Expr::param("name"));
        assert!(matches!(
            p_expr,
            Predicate::EqExpr(prop, Expr::Param(param)) if prop == "name" && param == "name"
        ));

        let p_between = Predicate::between("age", Expr::param("min"), 65i64);
        assert!(matches!(
            p_between,
            Predicate::BetweenExpr(prop, Expr::Param(min), Expr::Constant(PropertyValue::I64(65)))
                if prop == "age" && min == "min"
        ));

        let p_in = Predicate::is_in("status", vec!["active".to_string(), "pending".to_string()]);
        assert!(matches!(
            p_in,
            Predicate::IsIn(prop, PropertyValue::StringArray(values))
                if prop == "status" && values == vec!["active".to_string(), "pending".to_string()]
        ));

        let p2 = Predicate::and(vec![
            Predicate::eq("status", "active"),
            Predicate::gt("age", "18"),
        ]);
        assert!(matches!(p2, Predicate::And(preds) if preds.len() == 2));
    }

    #[test]
    fn test_edge_traversal() {
        // This should compile: nodes -> edges -> nodes
        let t = g()
            .n([1u64])
            .out_e(Some("FOLLOWS"))
            .edge_has("weight", 1i64)
            .out_n()
            .has_label("User");

        assert_eq!(t.steps.len(), 5);
    }

    #[test]
    fn test_sub_traversal() {
        let t = g()
            .n([1u64])
            .union(vec![sub().out(Some("FOLLOWS")), sub().out(Some("LIKES"))]);

        assert_eq!(t.steps.len(), 2);
        if let Step::Union(subs) = &t.steps[1] {
            assert_eq!(subs.len(), 2);
        } else {
            panic!("Expected Union step");
        }
    }

    #[test]
    fn test_repeat_with_sub_traversal() {
        let t = g()
            .n([1u64])
            .repeat(RepeatConfig::new(sub().out(None::<&str>)).times(3));

        assert_eq!(t.steps.len(), 2);
    }

    #[test]
    fn test_read_batch_construction() {
        let b = read_batch()
            .var_as(
                "user",
                g().n_where(SourcePredicate::eq("username", "alice")),
            )
            .var_as("friends", g().n(NodeRef::var("user")).out(Some("FOLLOWS")))
            .returning(["user", "friends"]);

        assert_eq!(b.queries.len(), 2);
        assert_eq!(b.returns, vec!["user", "friends"]);

        let first = query_entry(&b.queries[0]);
        let second = query_entry(&b.queries[1]);

        // First query: user
        assert_eq!(first.name, Some("user".to_string()));
        assert!(first.condition.is_none());
        assert_eq!(first.steps.len(), 1); // NWhere

        // Second query: friends
        assert_eq!(second.name, Some("friends".to_string()));
        assert_eq!(second.steps.len(), 2); // N + Out
    }

    #[test]
    fn test_read_batch_conditional() {
        let b = read_batch()
            .var_as("user", g().n_where(SourcePredicate::eq("id", 1i64)))
            .var_as_if(
                "posts",
                BatchCondition::VarNotEmpty("user".to_string()),
                g().n(NodeRef::var("user")).out(Some("POSTED")),
            );

        assert_eq!(b.queries.len(), 2);

        // Second query has condition
        assert!(matches!(
            &query_entry(&b.queries[1]).condition,
            Some(BatchCondition::VarNotEmpty(name)) if name == "user"
        ));
    }

    #[test]
    fn test_read_batch_with_terminal() {
        let b = read_batch()
            .var_as("user", g().n([1u64]).value_map(None::<Vec<&str>>))
            .var_as("friend_count", g().n([1u64]).out(Some("FOLLOWS")).count())
            .returning(["user", "friend_count"]);

        assert_eq!(b.queries.len(), 2);

        // First query ends with ValueMap
        assert!(matches!(
            query_entry(&b.queries[0]).steps.last(),
            Some(Step::ValueMap(_))
        ));

        // Second query ends with Count
        assert!(matches!(
            query_entry(&b.queries[1]).steps.last(),
            Some(Step::Count)
        ));
    }

    #[test]
    fn test_write_batch_construction() {
        let b = write_batch()
            .var_as("user", g().add_n("User", vec![("name", "Alice")]))
            .var_as("post", g().add_n("Post", vec![("title", "Hello")]))
            .returning(["user", "post"]);

        assert_eq!(b.queries.len(), 2);
        assert_eq!(b.returns, vec!["user", "post"]);
    }

    #[test]
    fn test_property_input_from_expr() {
        let traversal = g().add_n(
            "User",
            vec![
                ("name", PropertyInput::param("name")),
                ("age", PropertyInput::param("age")),
            ],
        );

        assert!(matches!(
            &traversal.steps[0],
            Step::AddN { properties, .. }
                if matches!(&properties[0].1, PropertyInput::Expr(Expr::Param(name)) if name == "name")
                && matches!(&properties[1].1, PropertyInput::Expr(Expr::Param(name)) if name == "age")
        ));
    }

    #[test]
    fn test_edge_has_accepts_param_input() {
        let traversal = g()
            .e([1u64])
            .edge_has("targetExternalId", PropertyInput::param("targetExternalId"));

        assert!(matches!(
            &traversal.steps[1],
            Step::EdgeHas(property, PropertyInput::Expr(Expr::Param(name)))
                if property == "targetExternalId" && name == "targetExternalId"
        ));
    }

    #[test]
    fn test_write_batch_for_each_param() {
        let body = write_batch()
            .var_as(
                "existing",
                g().n_where(SourcePredicate::eq("$label", "User")),
            )
            .var_as(
                "created",
                g().add_n("User", vec![("name", PropertyInput::param("name"))]),
            );

        let batch = write_batch().for_each_param("data", body);

        assert_eq!(batch.queries.len(), 1);
        assert!(matches!(
            &batch.queries[0],
            BatchEntry::ForEach { param, body }
                if param == "data" && matches!(&body[1], BatchEntry::Query(NamedQuery { name: Some(name), .. }) if name == "created")
        ));
    }

    #[test]
    fn test_property_value_nested_payload_variants() {
        let mut row = BTreeMap::new();
        row.insert("externalId".to_string(), PropertyValue::from("u-1"));
        row.insert("active".to_string(), PropertyValue::from(true));

        let payload = PropertyValue::from(vec![PropertyValue::from(row.clone())]);

        assert!(matches!(payload.as_array(), Some(values) if values.len() == 1));
        assert_eq!(
            payload
                .as_array()
                .and_then(|values| values[0].as_object())
                .and_then(|map| map.get("externalId"))
                .and_then(PropertyValue::as_str),
            Some("u-1")
        );
    }

    #[test]
    fn test_vector_search_steps() {
        let embedding = vec![0.1f32; 4];
        let t = g().vector_search_nodes("Doc", "embedding", embedding.clone(), 5, None);
        assert!(matches!(
            &t.steps[0],
            Step::VectorSearchNodes {
                label,
                property,
                query_vector: PropertyInput::Value(PropertyValue::F32Array(values)),
                k: StreamBound::Literal(k),
                tenant_value,
            }
                if label == "Doc"
                    && property == "embedding"
                    && values == &embedding
                    && *k == 5
                    && tenant_value.is_none()
        ));

        let t2 = g().vector_search_edges("SIMILAR", "embedding", embedding.clone(), 3, None);
        assert!(matches!(
            &t2.steps[0],
            Step::VectorSearchEdges {
                label,
                property,
                query_vector: PropertyInput::Value(PropertyValue::F32Array(values)),
                k: StreamBound::Literal(k),
                tenant_value,
            }
                if label == "SIMILAR"
                    && property == "embedding"
                    && values == &embedding
                    && *k == 3
                    && tenant_value.is_none()
        ));

        let t3 = g().vector_search_nodes(
            "Doc",
            "embedding",
            vec![0.1f32; 4],
            5,
            Some(PropertyValue::from("tenant-a")),
        );
        assert!(matches!(
            &t3.steps[0],
            Step::VectorSearchNodes {
                label,
                property,
                k: StreamBound::Literal(k),
                tenant_value: Some(PropertyInput::Value(PropertyValue::String(value))),
                ..
            } if label == "Doc" && property == "embedding" && *k == 5 && value == "tenant-a"
        ));
    }

    #[test]
    fn test_parameterized_vector_search_steps() {
        let t = g().vector_search_nodes_with(
            "Doc",
            "embedding",
            PropertyInput::param("queryVector"),
            Expr::param("limit"),
            Some(PropertyInput::param("firmId")),
        );

        assert!(matches!(
            &t.steps[0],
            Step::VectorSearchNodes {
                label,
                property,
                query_vector: PropertyInput::Expr(Expr::Param(query_vector)),
                k: StreamBound::Expr(Expr::Param(limit)),
                tenant_value: Some(PropertyInput::Expr(Expr::Param(firm_id))),
            }
                if label == "Doc"
                    && property == "embedding"
                    && query_vector == "queryVector"
                    && limit == "limit"
                && firm_id == "firmId"
        ));
    }

    #[test]
    fn test_text_search_steps() {
        let t = g().text_search_nodes("Doc", "body", "alice search", 5, None);
        assert!(matches!(
            &t.steps[0],
            Step::TextSearchNodes {
                label,
                property,
                query_text: PropertyInput::Value(PropertyValue::String(query)),
                k: StreamBound::Literal(k),
                tenant_value,
            }
                if label == "Doc"
                    && property == "body"
                    && query == "alice search"
                    && *k == 5
                    && tenant_value.is_none()
        ));

        let t2 = g().text_search_edges(
            "REL",
            "body",
            "alice edge",
            3,
            Some(PropertyValue::from("tenant-a")),
        );
        assert!(matches!(
            &t2.steps[0],
            Step::TextSearchEdges {
                label,
                property,
                query_text: PropertyInput::Value(PropertyValue::String(query)),
                k: StreamBound::Literal(k),
                tenant_value: Some(PropertyInput::Value(PropertyValue::String(tenant))),
            }
                if label == "REL"
                    && property == "body"
                    && query == "alice edge"
                    && *k == 3
                    && tenant == "tenant-a"
        ));
    }

    #[test]
    fn test_parameterized_text_search_steps() {
        let t = g().text_search_nodes_with(
            "Doc",
            "body",
            PropertyInput::param("queryText"),
            Expr::param("limit"),
            Some(PropertyInput::param("tenantId")),
        );

        assert!(matches!(
            &t.steps[0],
            Step::TextSearchNodes {
                label,
                property,
                query_text: PropertyInput::Expr(Expr::Param(query_text)),
                k: StreamBound::Expr(Expr::Param(limit)),
                tenant_value: Some(PropertyInput::Expr(Expr::Param(tenant_id))),
            }
                if label == "Doc"
                    && property == "body"
                    && query_text == "queryText"
                    && limit == "limit"
                    && tenant_id == "tenantId"
        ));
    }

    #[test]
    fn test_parameterized_stream_bounds() {
        let range = g()
            .n_with_label("User")
            .range(Expr::param("start"), Expr::param("end"));
        assert!(matches!(
            range.steps.as_slice(),
            [
                Step::NWhere(_),
                Step::RangeBy(StreamBound::Expr(Expr::Param(start)), StreamBound::Expr(Expr::Param(end))),
            ] if start == "start" && end == "end"
        ));

        let ordered = g()
            .n_with_label("User")
            .order_by("age", Order::Desc)
            .limit(Expr::param("limit"))
            .skip(Expr::param("offset"));
        assert!(matches!(
            ordered.steps.as_slice(),
            [
                Step::NWhere(_),
                Step::OrderBy(property, Order::Desc),
                Step::LimitBy(Expr::Param(limit)),
                Step::SkipBy(Expr::Param(offset)),
            ] if property == "age" && limit == "limit" && offset == "offset"
        ));
    }

    #[test]
    fn test_contains_param_predicate() {
        assert!(matches!(
            Predicate::contains_param("location", "city"),
            Predicate::ContainsExpr(property, Expr::Param(param))
                if property == "location" && param == "city"
        ));
    }

    #[test]
    fn test_is_in_param_predicate() {
        assert!(matches!(
            Predicate::is_in_param("location", "cities"),
            Predicate::IsInExpr(property, Expr::Param(param))
                if property == "location" && param == "cities"
        ));
    }

    #[test]
    fn source_predicate_literal_stays_literal() {
        // A literal value keeps the existing PropertyValue variant (JSON unchanged).
        assert!(matches!(
            SourcePredicate::eq("username", "alice"),
            SourcePredicate::Eq(prop, PropertyValue::String(v))
                if prop == "username" && v == "alice"
        ));
        assert!(matches!(
            SourcePredicate::gt("score", 10i64),
            SourcePredicate::Gt(_, PropertyValue::I64(10))
        ));
    }

    #[test]
    fn source_predicate_param_routes_to_expr_variant() {
        // An Expr/parameter routes to the new *Expr variant.
        assert!(matches!(
            SourcePredicate::eq("username", Expr::param("name")),
            SourcePredicate::EqExpr(prop, Expr::Param(p))
                if prop == "username" && p == "name"
        ));
        assert!(matches!(
            SourcePredicate::lte("score", Expr::param("max")),
            SourcePredicate::LteExpr(_, Expr::Param(p)) if p == "max"
        ));
    }

    #[test]
    fn source_predicate_between_dispatch() {
        // Two literals -> Between; any expr bound -> BetweenExpr.
        assert!(matches!(
            SourcePredicate::between("age", 18i64, 65i64),
            SourcePredicate::Between(_, PropertyValue::I64(18), PropertyValue::I64(65))
        ));
        assert!(matches!(
            SourcePredicate::between("age", Expr::param("lo"), 65i64),
            SourcePredicate::BetweenExpr(_, Expr::Param(lo), Expr::Constant(PropertyValue::I64(65)))
                if lo == "lo"
        ));
    }

    #[test]
    fn source_predicate_expr_converts_to_predicate_expr_variant() {
        // From<SourcePredicate> for Predicate preserves the *Expr variant shape.
        let pred: Predicate = SourcePredicate::eq("username", Expr::param("name")).into();
        assert!(matches!(
            pred,
            Predicate::EqExpr(prop, Expr::Param(p))
                if prop == "username" && p == "name"
        ));
    }

    #[test]
    fn test_create_vector_index_steps() {
        let t = g().create_vector_index_nodes("Doc", "embedding", None::<&str>);
        assert!(t.has_terminal());
        assert!(matches!(
            &t.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::NodeVector {
                    label,
                    property,
                    tenant_property,
                },
                if_not_exists,
            } if label == "Doc"
                && property == "embedding"
                && tenant_property.is_none()
                && *if_not_exists
        ));

        let t2 = g().create_vector_index_edges("REL", "embedding", None::<&str>);
        assert!(matches!(
            &t2.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::EdgeVector {
                    label,
                    property,
                    tenant_property,
                },
                if_not_exists,
            } if label == "REL"
                && property == "embedding"
                && tenant_property.is_none()
                && *if_not_exists
        ));

        let t3 = g().create_vector_index_nodes("Doc", "embedding", Some("tenant_id"));
        assert!(matches!(
            &t3.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::NodeVector {
                    tenant_property: Some(tenant_property),
                    ..
                },
                if_not_exists,
            } if tenant_property == "tenant_id" && *if_not_exists
        ));
    }

    #[test]
    fn test_create_text_index_steps() {
        let t = g().create_text_index_nodes("Doc", "body", None::<&str>);
        assert!(matches!(
            &t.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::NodeText {
                    label,
                    property,
                    tenant_property,
                },
                if_not_exists,
            } if label == "Doc"
                && property == "body"
                && tenant_property.is_none()
                && *if_not_exists
        ));

        let t2 = g().create_text_index_edges("REL", "body", Some("tenant_id"));
        assert!(matches!(
            &t2.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::EdgeText {
                    label,
                    property,
                    tenant_property: Some(tenant_property),
                },
                if_not_exists,
            } if label == "REL"
                && property == "body"
                && tenant_property == "tenant_id"
                && *if_not_exists
        ));
    }

    #[test]
    fn test_generic_index_steps() {
        let create = g().create_index_if_not_exists(IndexSpec::node_equality("User", "status"));
        assert!(create.has_terminal());
        assert!(matches!(
            &create.steps[0],
            Step::CreateIndex {
                spec: IndexSpec::NodeEquality {
                    label,
                    property,
                    unique,
                },
                if_not_exists,
            } if label == "User" && property == "status" && !unique && *if_not_exists
        ));

        let drop = g().drop_index(IndexSpec::edge_range("FOLLOWS", "weight"));
        assert!(drop.has_terminal());
        assert!(matches!(
            &drop.steps[0],
            Step::DropIndex {
                spec: IndexSpec::EdgeRange { label, property },
            } if label == "FOLLOWS" && property == "weight"
        ));
    }

    #[test]
    fn test_unique_node_equality_constructor() {
        assert_eq!(
            IndexSpec::node_unique_equality("User", "email"),
            IndexSpec::NodeEquality {
                label: "User".to_string(),
                property: "email".to_string(),
                unique: true,
            }
        );
    }

    #[test]
    fn test_node_equality_deserializes_unique_default_false() {
        let decoded: IndexSpec =
            sonic_rs::from_str(r#"{"NodeEquality":{"label":"User","property":"status"}}"#)
                .expect("deserialize old node equality payload");

        assert_eq!(
            decoded,
            IndexSpec::NodeEquality {
                label: "User".to_string(),
                property: "status".to_string(),
                unique: false,
            }
        );
    }

    #[test]
    fn test_node_unique_equality_serializes_unique_flag() {
        let encoded = sonic_rs::to_string(&IndexSpec::node_unique_equality("User", "status"))
            .expect("serialize unique node equality");

        assert!(encoded.contains(r#""NodeEquality""#));
        assert!(encoded.contains(r#""unique":true"#));
    }
}
