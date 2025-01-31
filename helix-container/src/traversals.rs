use std::collections::HashMap;
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use get_routes::handler;
use helix_engine::graph_core::traversal::TraversalBuilder;
use helix_engine::graph_core::traversal_steps::{
    SourceTraversalSteps, TraversalMethods, TraversalSearchMethods, TraversalSteps,
};
use helix_engine::props;
use helix_engine::types::GraphError;
use helix_gateway::router::router::HandlerInput;
use protocol::count::Count;
use protocol::response::Response;
use protocol::traversal_value::TraversalValue;
use protocol::{filterable::Filterable, value::Value, ReturnValue};
use sonic_rs::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Data {
    user_id: String,
    username: String,
    to_id: String,
}

#[handler]
pub fn get_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: Data = sonic_rs::from_slice(&input.request.body).unwrap();
    let mut return_vals = HashMap::with_capacity(4);
    let db = input.graph.storage.clone();
    let mut user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    let now = Instant::now();
    user //.v_from_id("a965cc92-16d4-41ef-acf5-cafe2fc063e5");
        .v()
        .filter_nodes(|node| {
            if let Some(val) = node.check_property("screen_name") {
                match val {
                    Value::String(s) => Ok(*s == data.username),
                    _ => unreachable!(),
                }
            } else {
                Err(GraphError::from("Invalid node".to_string()))
            }
        });

    let mut follower_edges = TraversalBuilder::new(Arc::clone(&db), user.current_step.clone());
    follower_edges.in_e("follows");
    let mut followers = TraversalBuilder::new(Arc::clone(&db), follower_edges.current_step.clone());
    followers.out_v();
    let end = now.elapsed();

    return_vals.insert(
        "user".to_string(),
        ReturnValue::TraversalValues(user.current_step),
    );
    return_vals.insert(
        "follower_edges".to_string(),
        ReturnValue::TraversalValues(follower_edges.current_step),
    );
    return_vals.insert(
        "followers".to_string(),
        ReturnValue::TraversalValues(followers.current_step),
    );
    return_vals.insert(
        "time".to_string(),
        ReturnValue::Count(Count::new(end.as_millis() as usize)),
    );
    response.body = match sonic_rs::to_vec(&return_vals) {
        Ok(body) => body,
        Err(e) => {
            return Err(GraphError::ConversionError(e.to_string()));
        }
    };
    Ok(())
}

#[derive(Serialize)]
struct StreamingReturnValues<'a> {
    users: &'a TraversalValue,
    edges: &'a TraversalValue,
    time: Count,
}

#[handler]
pub fn get_all_users(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let now = Instant::now();
    let db = input.graph.storage.clone();

    let edges_result: Result<(TraversalValue, TraversalValue), GraphError> = thread::scope(|s| {
        let edges_handle = s.spawn(|| {
            let now = Instant::now();
            let mut edges = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
            edges.e();
            let end = now.elapsed();
            println!("TIME E: {:?}", end);
            edges.current_step
        });

        let now = Instant::now();
        let mut users = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
        users.v();
        let end = now.elapsed();
        println!("TIME N: {:?}", end);
        let users_result = users.current_step;

        let edges_result = edges_handle.join().map_err(|_| GraphError::Default)?;
        Ok((users_result, edges_result))
    })
    .map_err(|e: GraphError| e);

    let edges_result = edges_result?;
    let total_time = now.elapsed();

    let estimated_size = estimate_response_size(&edges_result.0, &edges_result.1);
    response.body = Vec::with_capacity(estimated_size);

    let now = Instant::now();
    // {
    //     let writer = std::io::BufWriter::new(&mut response.body);
    //     let mut serializer = sonic_rs::Serializer::new(writer);

    //     .serialize(&mut serializer)
    //     .unwrap();

    // }

    let p = StreamingReturnValues {
        users: &edges_result.0,
        edges: &edges_result.1,
        time: Count::new(total_time.as_millis() as usize),
    };
    response.body = sonic_rs::to_vec(&p).unwrap();

    let end = now.elapsed();
    println!("TIME SERDE: {:?}", end);
    Ok(())
}

fn estimate_response_size(users: &TraversalValue, edges: &TraversalValue) -> usize {
    const AVERAGE_USER_SIZE: usize = 256;
    const AVERAGE_EDGE_SIZE: usize = 384;

    let user_count = match users {
        TraversalValue::NodeArray(nodes) => nodes.len(),
        _ => 0,
    };

    let edge_count = match edges {
        TraversalValue::EdgeArray(edges) => edges.len(),
        _ => 0,
    };

    let base_size = 100;
    base_size + (user_count * AVERAGE_USER_SIZE) + (edge_count * AVERAGE_EDGE_SIZE)
}

#[handler]
pub fn get_shortest_path_to_user(
    input: &HandlerInput,
    response: &mut Response,
) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);
    let data: Data = sonic_rs::from_slice(&input.request.body).unwrap();
    let now = Instant::now();
    let mut tr = TraversalBuilder::new(Arc::clone(&input.graph.storage), TraversalValue::Empty);
    tr.v_from_id(data.user_id.as_str())
        .shortest_path_to(data.to_id.as_str());
    let end = now.elapsed();
    return_vals.insert(
        "time".to_string(),
        ReturnValue::Count(Count::new(end.as_millis() as usize)),
    );
    return_vals.insert(
        "shortest_path".to_string(),
        ReturnValue::TraversalValues(tr.current_step),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[derive(Deserialize)]
struct AddUser {
    x_id: String,
    username: String,
    url: String,
    graph_image_url: String,
    created_at: String,
    updated_at: String,
    following_ids: Vec<String>,
}

#[handler]
pub fn add_user(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let mut return_vals: HashMap<String, ReturnValue> = HashMap::with_capacity(2);
    let data: AddUser = sonic_rs::from_slice(&input.request.body).unwrap();
    let now = Instant::now();
    let mut tr: TraversalBuilder =
        TraversalBuilder::new(Arc::clone(&input.graph.storage), TraversalValue::Empty);
    tr.add_v(
        "user",
        props! {
            "x_id" => data.x_id,
            "username" => data.username,
            "url" => data.url,
            "graph_image_url" => data.graph_image_url,
            "created_at" => data.created_at,
            "updated_at" => data.updated_at,
        },
    );
    if let TraversalValue::NodeArray(new_user) = tr.current_step.clone() {
        let user = new_user.first().unwrap();
        let mut following =
            TraversalBuilder::new(Arc::clone(&input.graph.storage), TraversalValue::Empty);
        following.v().for_each_node(|nodes| {
            if let Some(val) = nodes.check_property("username") {
                if let Value::String(val) = val {
                    match data.following_ids.iter().any(|x| x == val) {
                        true => {
                            let mut edge = TraversalBuilder::new(
                                Arc::clone(&input.graph.storage),
                                TraversalValue::Empty,
                            );
                            edge.add_e("follows", &user.id, &nodes.id, props! {});
                            match edge.current_step {
                                TraversalValue::EdgeArray(_) => Ok(()),
                                _ => Err(GraphError::from("Invalid edge".to_string())),
                            }
                        }
                        false => {
                            // create new node and edge
                            let mut new_node = TraversalBuilder::new(
                                Arc::clone(&input.graph.storage),
                                TraversalValue::Empty,
                            );
                            let username = val.clone();
                            new_node.add_v(
                                "user",
                                props! {
                                    "username" => username,
                                    "url" => format!("https://twitter.com/{}", val), // TODO: fix this
                                    "graph_image_url" => "https://twitter.com/".to_string() + val + "/profile_image?size=original", // TODO: fix this
                                    "created_at" => "2021-01-01".to_string(),
                                    "updated_at" => "2021-01-01".to_string(),
                                },
                            );
                            if let TraversalValue::NodeArray(new_node) = new_node.current_step {
                                let new_node = new_node.first().unwrap();
                                let mut edge = TraversalBuilder::new(
                                    Arc::clone(&input.graph.storage),
                                    TraversalValue::Empty,
                                );
                                edge.add_e("follows", &user.id, &new_node.id, props! {});
                                match edge.current_step {
                                    TraversalValue::EdgeArray(_) => Ok(()),
                                    _ => Err(GraphError::from("Invalid edge".to_string())),
                                }
                            } else {
                                Err(GraphError::from("Invalid node".to_string()))
                            }
                        }
                    }
                } else {
                    Err(GraphError::from("Invalid node".to_string()))
                }
            } else {
                Err(GraphError::from("Invalid node".to_string()))
            }
        })
    } else {
        return Err(GraphError::from("Invalid node".to_string()));
    };
    let end = now.elapsed();
    return_vals.insert(
        "new_user".to_string(),
        ReturnValue::TraversalValues(tr.current_step),
    );
    response.body = sonic_rs::to_vec(&return_vals).unwrap();
    Ok(())
}

#[handler]
pub fn get_user_subgraph(input: &HandlerInput, response: &mut Response) -> Result<(), GraphError> {
    let data: Data = sonic_rs::from_slice(&input.request.body).unwrap();
    let mut return_vals = HashMap::with_capacity(4);
    let db = input.graph.storage.clone();
    let mut user = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);

    let now = Instant::now();
    user.v_from_id(data.user_id.as_str());
    // println!("USER: {:?}", user.current_step);
    const threshold: usize = 500;

    // get 350 out nodes that are mutuals
    let mut mutuals = TraversalBuilder::new(Arc::clone(&db), user.current_step.clone());
    mutuals
        .mutual("follows")
        .range(0, 300);
    // println!("MUTUALS: {:?}", mutuals.current_step);
    // get 200 random nodes
    let mut random = TraversalBuilder::new(Arc::clone(&db), TraversalValue::Empty);
    random
        .v()
        .range(0, 200);

    let end = now.elapsed();

    return_vals.insert(
        "user".to_string(),
        ReturnValue::TraversalValues(user.current_step),
    );
    return_vals.insert(
        "mutuals".to_string(),
        ReturnValue::TraversalValues(mutuals.current_step),
    );
    return_vals.insert(
        "random".to_string(),
        ReturnValue::TraversalValues(random.current_step),
    );
    response.body = match sonic_rs::to_vec(&return_vals) {
        Ok(body) => body,
        Err(e) => {
            return Err(GraphError::ConversionError(e.to_string()));
        }
    };
    Ok(())
}
