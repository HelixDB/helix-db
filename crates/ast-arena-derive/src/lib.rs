//! `#[derive(ArenaMirror)]` for helix-ast's owned request types.
//!
//! For an owned type `X` the derive emits, beside it:
//!
//! - `ArenaX<'a>`, a `Copy` mirror whose strings, boxes, vectors and
//!   string-keyed maps are `&'a str`, `&'a T`, `&'a [T]` and
//!   `crate::arena::Map<'a, V>` borrowed from a `bumpalo::Bump`, and whose
//!   other named field types are their own mirrors;
//! - `impl crate::arena::ArenaDeserialize<'a> for ArenaX<'a>`, the visitor
//!   serde_derive would emit for `X`, driven by a seed that allocates in the
//!   arena, so it accepts the same JSON and reports the same errors;
//! - `impl crate::arena::IntoOwned<X> for ArenaX<'_>`, the conversion back.
//!
//! An enum of unit variants only is its own mirror: `type ArenaX<'a> = X`.
//!
//! The derive reads serde's `rename_all` (`snake_case`, `lowercase`) and
//! field `default`, ignores `skip_serializing_if`, and rejects every other
//! serde attribute, so a mirror cannot quietly disagree with the owned type's
//! wire format. `#[arena(copy)]` keeps a field's named types (already `Copy`
//! aliases such as `NodeId`), `#[arena(owned)]` keeps a field owned and
//! deserialized by serde, and `#[arena(manual_deserialize)]` leaves the
//! `ArenaDeserialize` impl to hand-written validation.
//!
//! The generated code names `crate::arena` and `::serde`, so the derive is for
//! helix-ast's own types; `helix_ast::arena` documents and tests the result.
//!
//! ```text
//! #[derive(Deserialize, ArenaMirror)]
//! #[serde(rename_all = "snake_case")]
//! pub enum NodeRef { All, Ids(#[arena(copy)] Vec<NodeId>), Var(String), Param(String) }
//!
//! // emits
//! pub enum ArenaNodeRef<'a> { All, Ids(&'a [NodeId]), Var(&'a str), Param(&'a str) }
//! ```

mod expand;
mod mirror;
mod model;

/// Derive the arena mirror of an owned request type; see the crate docs.
#[proc_macro_derive(ArenaMirror, attributes(arena, serde))]
pub fn derive_arena_mirror(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = syn::parse_macro_input!(input as syn::DeriveInput);
    model::Item::parse(&input)
        .and_then(|item| expand::expand(&item))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
