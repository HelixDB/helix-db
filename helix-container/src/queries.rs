// DEFAULT CODE
// use helix_db::helix_engine::graph_core::config::Config;

// pub fn config() -> Option<Config> {
//     None
// }

use chrono::{DateTime, Utc};
use heed3::RoTxn;
use helix_db::{
    err_bubble,embed, exclude_field, field_addition_from_old_field, field_addition_from_value,
    field_remapping, field_type_cast,
    helix_engine::{
        graph_core::{
            config::{Config, GraphConfig, VectorConfig},
            ops::{
                bm25::search_bm25::SearchBM25Adapter,
                g::G,
                in_::{in_::InAdapter, in_e::InEdgesAdapter, to_n::ToNAdapter, to_v::ToVAdapter},
                out::{
                    from_n::FromNAdapter, from_v::FromVAdapter, out::OutAdapter,
                    out_e::OutEdgesAdapter,
                },
                source::{
                    add_e::{AddEAdapter, EdgeType},
                    add_n::AddNAdapter,
                    e_from_id::EFromIdAdapter,
                    e_from_type::EFromTypeAdapter,
                    n_from_id::NFromIdAdapter,
                    n_from_index::NFromIndexAdapter,
                    n_from_type::NFromTypeAdapter,
                },
                tr_val::{Traversable, TraversalVal},
                util::{
                    dedup::DedupAdapter, drop::Drop, exist::Exist, filter_mut::FilterMut,
                    filter_ref::FilterRefAdapter, map::MapAdapter, order::OrderByAdapter,
                    paths::ShortestPathAdapter, props::PropsAdapter, range::RangeAdapter,
                    update::UpdateAdapter,
                },
                vectors::{
                    brute_force_search::BruteForceSearchVAdapter, insert::InsertVAdapter,
                    search::SearchVAdapter,
                },
            },
        },
        types::GraphError,
        vector_core::vector::HVector,
    },
    helix_gateway::{
        embedding_providers::embedding_providers::{EmbeddingModel, get_embedding_model},
        mcp::mcp::{MCPHandler, MCPHandlerSubmission, MCPToolInput},
        router::router::HandlerInput,
    },
    identifier_remapping, node_matches, props,
    protocol::{
        format::Format,
        remapping::{Remapping, RemappingMap, ResponseRemapping},
        request::RetChan,
        response::Response,
        return_values::ReturnValue,
        value::{
            Value,
            casting::{CastType, cast},
        },
    },
    traversal_remapping,
    utils::{
        count::Count,
        filterable::Filterable,
        id::ID,
        items::{Edge, Node},
    },
    value_remapping,
};
use helix_macros::{handler, mcp_handler, migration, tool_call};
use sonic_rs::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

pub fn config() -> Option<Config> {
    return Some(Config {
        vector_config: Some(VectorConfig {
            m: Some(16),
            ef_construction: Some(128),
            ef_search: Some(768),
        }),
        graph_config: Some(GraphConfig {
            secondary_indices: Some(vec![]),
        }),
        db_max_size_gb: Some(20),
        mcp: Some(true),
        bm25: Some(true),
        schema: Some(
            r#"{
  "schema": {
    "nodes": [],
    "vectors": [
      {
        "name": "UserEmbedding",
        "properties": {
          "lastUpdated": "String",
          "userId": "String",
          "dataType": "String",
          "metadata": "String",
          "id": "ID",
          "createdAt": "Date"
        }
      }
    ],
    "edges": []
  },
  "queries": [
    {
      "name": "SearchSimilarUsers",
      "parameters": {
        "queryText": "String",
        "dataType": "String",
        "k": "I64"
      },
      "returns": [
        "search_results"
      ]
    },
    {
      "name": "CreateUserBioEmbedding",
      "parameters": {
        "userId": "String",
        "bioText": "String",
        "lastUpdated": "String"
      },
      "returns": [
        "embedding"
      ]
    }
  ]
}"#
            .to_string(),
        ),
        embedding_model: None,
        graphvis_node_label: None,
    });
}

pub struct UserEmbedding {
    pub userId: String,
    pub dataType: String,
    pub metadata: String,
    pub lastUpdated: String,
    pub createdAt: DateTime<Utc>,
}

#[derive(Serialize, Deserialize)]
pub struct SearchSimilarUsersInput {
    pub queryText: Option<String>,
    pub k: i64,
    pub dataType: String,
}
#[handler(with_read)]
pub fn SearchSimilarUsers(input: &HandlerInput, ret_chan: RetChan) {
    {
        // FOO!!!!
        let search_results = input.context.io_rt.spawn(async move {
            let __async_embed_value_0 = &embed!(
                db,
                err_bubble!(
                    ret_chan,
                    data.queryText
                        .as_ref()
                        .ok_or_else(|| GraphError::ParamNotFound("queryText"))
                )
            );
            input
                .context
                .cont_tx
                .send(Box::new(move || {
                    let search_results = G::new(Arc::clone(&db), &txn)
                        .search_v::<fn(&HVector, &RoTxn) -> bool, _>(
                            &__async_embed_value_0,
                            data.k.clone(),
                            "UserEmbedding",
                            None,
                        )
                        .collect_to::<Vec<_>>();
                    txn.commit().unwrap();
                    let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
                    return_vals.insert(
                        "search_results".to_string(),
                        ReturnValue::from_traversal_value_array_with_mixin(
                            search_results.clone().clone(),
                            remapping_vals.borrow_mut(),
                        ),
                    );

                    ret_chan
                        .send(Ok(input.request.out_fmt.create_response(&return_vals)))
                        .expect("Return channel should suceed")
                }))
                .expect("Continuation channel should not be closed")
        });
    }
}

#[derive(Serialize, Deserialize)]
pub struct CreateUserBioEmbeddingInput {
    pub userId: String,
    pub bioText: String,
    pub lastUpdated: String,
}
#[handler(with_write)]
pub fn CreateUserBioEmbedding(input: &HandlerInput, ret_chan: RetChan) {
    {
        // FOO!!!!
        let embedding = input.context.io_rt.spawn(async move{
let __async_embed_value_0 = &embed!(db, &data.bioText);
input.context.cont_tx.send(move || {
G::new_mut(Arc::clone(&db), &mut txn)
.insert_v::<fn(&HVector, &RoTxn) -> bool>(&__async_embed_value_1, "UserEmbedding", Some(props! { "lastUpdated" => data.lastUpdated.clone(), "userId" => data.userId.clone(), "metadata" => "{}", "dataType" => "bio" })).collect_to_obj();
    txn.commit().unwrap();
let mut return_vals: HashMap<String, ReturnValue> = HashMap::new();
        return_vals.insert("embedding".to_string(), ReturnValue::from_traversal_value_with_mixin(embedding.clone().clone(), remapping_vals.borrow_mut()));

    ret_chan.send(Ok(input.request.out_fmt.create_response(&return_vals))).expect("Return channel should suceed")
}).expect("Continuation channel should not be closed")});
    }
}
