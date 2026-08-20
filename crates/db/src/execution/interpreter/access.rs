mod dispatch;
mod expand;
mod indexes;
mod kv;
mod membership;
mod params;
mod range;
mod restricted_text;
mod restricted_vector;
mod rows;
mod search;
mod secondary_set;

pub(in crate::execution::interpreter) use search::SearchReadLimit;

#[cfg(any(test, feature = "production-coverage"))]
#[cfg_attr(
    all(feature = "production-coverage", not(test)),
    allow(dead_code, unused_imports, unused_macros)
)]
pub(in crate::execution::interpreter) mod tests;

#[cfg(all(feature = "production-coverage", not(test)))]
pub(in crate::execution::interpreter) async fn run_production_contracts() {
    dispatch::tests::run_production_contracts().await;
    indexes::tests::run_production_contracts().await;
}
