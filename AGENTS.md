### approach

- ⁠Make invalid states unrepresentable using algebraic data types (e.g. enums).
- ⁠Use code coverage tools to make sure unit test coverage covers every edge case. This should be easily doable if invalid states are unrepresentable.
- Keep things modular.
- Define clear contract boundaries for testability. Document them all, and make sure doc comment examples are ran as tests.
- Use assertions for invariants when encoding within types is not feasible.
- Use clippy and fix code smells reported.
- Prefer iterators, monadic logic, and functional concepts. Where not possible for performance, document why.
- Make invalid state unrepresentable.
- Prefer importing modules over long lists of types. Using `module::Type` at call sites is fine, keeps names unambiguous, and reduces namespace-collision churn.
- Be wary of things that don't scale. If you have reason to believe why it wont, report back to the user explaining why.
- instead of if let { ... } use let Some(…) = … else { }
- avoid over using helper functions where possible. duplicating a single line of code is clearer than polluting with helper functions. if a large piece of functionality is shared or repeated multiple times then it can be abstracted but you should favour inline code where possible. small pieces of functionality should be inlined instead of abstracted.
- Stage and commit code locally in phases as you go. 
- As you make changes, if it is small (e.g. single/couple line addition) or if it is critical to your implementation, make the change.
- Clean up any temporary files or directories you create during your work (including in /private/tmp)
- ⁠match { Example(_) => ExampleVariant } -> use derive macro for Variant and the conversions instead of littering our code with the extra types and functions
⁠- ⁠instead of children structs holding an Arc<Db>, maybe just let them take in a &Db in every method that requires it.
- when that's not feasible, evaluate whether passing in an Arc<Db> is fine, and if not make sure that holding an Arc<Db> won't leak the memory based on how the child is stored in parent.

### Slice access
⁠When accessing or creating slices, for every access or slice don't minify it to start..end constants, always keep the additions of offsets explicit.
Instead of making `start` and `end` variables in the place of `const` values, use the `const` values directly, adding them up instead.

e.g. you should do the following

```rust
const PREFIX_LEN: usize = core::mem::size_of::<u8>();
const INDEX_PREFIX_LEN: usize = core::mem::size_of::<u8>();
const PROPERTY_HASH_MAX_LEN: usize = core::mem::size_of::<PropertyHash>();
const VALUE_HASH_MAX_LEN: usize = core::mem::size_of::<ValueHash>();

let property_hash = slice[INDEX_PREFIX_LEN..INDEX_PREFIX_LEN + PROPERTY_HASH_MAX_LEN]
    .try_into()
    .expect("property hash slice is 4 bytes");
```

### Clippy and formatting

Run clippy and formatting to check for code smells and formatting issues.

```bash
cargo clippy --workspace -- -D warnings
rustfmt --edition 2024 --check
```

you should run formatting and clippy before finishing your work. Rust crates in this workspace use edition 2024 from their `Cargo.toml`; do not pass `--edition 2024` to `cargo clippy`, because Cargo does not accept that flag for Clippy. Use a current stable Rust toolchain with the matching Clippy component installed.

### Database Key/Value Construction, Parsing, and Use

- ALL database key construction, parsing, and serialisation should be done using the structs in crates/db/src/encoding/v1 
    - There are structs here that make building invalid keys impossible and it ensures call sites remain standardised and improves testability
- ALL database value construction, parsing, and serialisation should be done using the structs in crates/db/src/encoding/v1/values
    - There are structs here that make building invalid keys impossible and it ensures call sites remain standardised and improves testability
    - We are in the process of moving more values to this dir. If you notice inline value use, please either add it to crates/db/src/encoding/v1/values if it is small (e.g. single variant addition) or if it is a major change just note it down. 

