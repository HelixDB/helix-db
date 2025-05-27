use anyhow::Result;
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client;
use helixdb::ingestion_engine::sql_ingestion::IngestSqlRequest;
use helixdb::protocol::{request::Request, response::Response};
use sonic_rs::{Deserialize, JsonValueTrait, Serialize, Value};
use std::io::Write;
use std::{
    net::SocketAddr,
    process::Command,
    time::Duration,
};
use tokio::net::TcpListener;
use tokio::time::timeout;

// Constants for timeouts
//const SOCKET_TIMEOUT: Duration = Duration::from_secs(30);
const S3_OPERATION_TIMEOUT: Duration = Duration::from_secs(60);

async fn process_query_files(
    client: &Client,
    bucket: &str,
    user_id: &str,
    instance_id: &str,
    local_path: &str,
) -> Result<()> {
    let prefix = format!("{}/{}/", user_id, instance_id);

    // List objects in the bucket with the given prefix with timeout
    let objects = timeout(
        S3_OPERATION_TIMEOUT,
        client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(&prefix)
            .send()
    ).await??;

    // Create the output file
    let output_path = format!("{}/queries.hx", local_path);
    let mut output_file = match std::fs::File::create(&output_path) {
        Ok(file) => {
            println!("Output file created: {:?}", output_path);
            file
        }
        Err(e) => {
            eprintln!("Failed to create output file: {:?}", e);
            return Ok(());
        }
    };

    if let Some(contents) = objects.contents {
        println!("Contents: {:?}", contents);
        for object in contents {
            if let Some(key) = &object.key {
                // Get the object with timeout
                println!("Key: {:?}", key);
                let get_obj = timeout(
                    S3_OPERATION_TIMEOUT,
                    client.get_object().bucket(bucket).key(key).send()
                ).await??;
                println!("Get object: {:?}", get_obj);
                let data = timeout(S3_OPERATION_TIMEOUT, get_obj.body.collect()).await??;
                let json_str = String::from_utf8(data.to_vec())?;

                // Parse JSON
                let json: Value = sonic_rs::from_str(&json_str)?;

                // Extract content field and write to output file
                if let Some(content) = json["content"].as_str() {
                    println!("Content: {:?}", content);
                    match writeln!(output_file, "{}", content) {
                        Ok(_) => (),
                        Err(e) => {
                            eprintln!("Failed to write to output file: {:?}", e);
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

// make sure build is run in sudo mode

#[derive(Debug, Deserialize, Serialize)]
pub struct HBuildDeployRequest {
    user_id: String,
    instance_id: String,
    version: String,
}

#[tokio::main]
async fn main() -> Result<(), AdminError> {
    println!("Starting helix build service");
    // Initialize AWS SDK
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = Client::new(&config);

    // run server on specified port
    let port = std::env::var("PORT").unwrap_or("8080".to_string());

    let addr: SocketAddr = format!("0.0.0.0:{}", port).parse().unwrap();
    let listener = TcpListener::bind(&addr).await.map_err(|e| {
        eprintln!("Failed to bind to address {}: {}", addr, e);
        AdminError::AdminConnectionError("Failed to bind to address".to_string(), e)
    })?;

    loop {
        match listener.accept().await {
            Ok((mut conn, addr)) => {
                println!("New connection from {}", addr);
                let s3_client_clone = s3_client.clone();

                tokio::spawn(async move {
                    let result: Result<(), AdminError> = async {
                        let mut response = Response::new();
                        let request = match Request::from_stream(&mut conn).await {
                            Ok(request) => request,
                            Err(e) => {
                                response.status = 400;
                                response.body = format!("Failed to parse request: {}", e).into_bytes();
                                return Ok(());
                            }
                        };

                        // Check if this is a deploy_queries request
                        if request.path == "/deploy_queries" {
                            let json_body: HBuildDeployRequest = match sonic_rs::from_slice(&request.body) {
                                Ok(json) => json,
                                Err(e) => {
                                    response.status = 400;
                                    response.body = format!("Failed to parse JSON: {}", e).into_bytes();
                                    return Ok(());
                                }
                            };
                            let bucket = std::env::var("S3_BUCKET").unwrap_or("helix-queries".to_string());
                            let local_path = std::env::var("LOCAL_QUERY_PATH").unwrap_or("/tmp/queries".to_string());

                            // Create local directory if it doesn't exist
                            std::fs::create_dir_all(&local_path).map_err(|e| {
                                response.status = 500;
                                response.body = format!("Failed to create local directory: {}", e).into_bytes();
                                AdminError::AdminConnectionError("Failed to create local directory".to_string(), e)
                            })?;

                            // Process query files and create query.hx
                            if let Err(e) = process_query_files(
                                &s3_client_clone,
                                &bucket,
                                &json_body.user_id,
                                &json_body.instance_id,
                                &local_path,
                            )
                            .await
                            {
                                eprintln!("Failed to process query files: {:?}", e);
                                response.status = 500;
                                response.body = format!("Failed to process query files: {:?}", e).into_bytes();
                            } else {
                                // Run helix compile command
                                println!("Compiling queries at {}", local_path);
                                let compile_result = Command::new("sudo")
                                    .arg("/root/.local/bin/helix")
                                    .arg("compile")
                                    .arg("--path")
                                    .arg(local_path)
                                    .arg("--output")
                                    .arg("/root/.helix/repo/helix-db/helix-container/src")
                                    .output();
                                println!("Compile result: {:?}", compile_result);
                                match compile_result {
                                    Ok(_output) => {
                                        // recompile binary
                                        println!("Recompiling binary");
                                        let recompile_result = Command::new("sudo")
                                            .arg("/root/.cargo/bin/cargo")
                                            .arg("build")
                                            .arg("--release")
                                            .arg("--target-dir")
                                            .arg("/root/.helix/bin")
                                            .current_dir("/root/.helix/repo/helix-db/helix-container")
                                            .output();
                                        println!("Recompile result: {:?}", recompile_result);

                                        // restart helix
                                        match recompile_result {
                                        Ok(output) if output.status.success() => {
                                            // Restart the helix service
                                            let restart_result = Command::new("sudo")
                                                .arg("systemctl")
                                                .arg("restart")
                                                .arg("helix")
                                                .output();
                                            println!("Restart result: {:?}", restart_result);
                                            match restart_result {
                                                Ok(output) if output.status.success() => {
                                                    response.status = 200;
                                                    response.body = "Successfully deployed queries and restarted helix service"
                                                        .as_bytes()
                                                        .to_vec();
                                                }
                                                Ok(output) => {
                                                    response.status = 500;
                                                    response.body = format!(
                                                        "Failed to restart helix service: {}",
                                                        String::from_utf8_lossy(&output.stderr)
                                                    )
                                                    .into_bytes();
                                                }
                                                Err(e) => {
                                                    response.status = 500;
                                                    response.body = format!("Failed to execute systemctl command: {:?}", e)
                                                        .into_bytes();
                                                }
                                            }
                                        }
                                        Ok(output) => {
                                            response.status = 500;
                                            response.body = format!(
                                                "Failed to compile queries: {}",
                                                String::from_utf8_lossy(&output.stderr)
                                            )
                                            .into_bytes();
                                        }
                                        Err(e) => {
                                            response.status = 500;
                                            response.body = format!("Failed to execute helix compile command: {:?}", e)
                                                .into_bytes();
                                        }
                                    }


                                    }
                                    Err(e) => {
                                        eprintln!("Failed to compile: {:?}", e);
                                        response.status = 500;
                                        response.body = format!("Failed to compile: {:?}", e).into_bytes();
                                    }
                                }
                            }
                        }
                        else if request.path == "/download_ingestion_data" {
                            let bucket = std::env::var("S3_BUCKET").unwrap_or("helix-queries".to_string());
                            let local_path = std::env::var("LOCAL_QUERY_PATH").unwrap_or("/tmp/queries".to_string());

                            #[derive(Debug, Deserialize, Serialize)]
                            pub struct DownloadIngestionDataRequest {
                                user_id: String,
                                instance_id: String,
                                job_id: String,
                            }
                            let json_body: DownloadIngestionDataRequest = match sonic_rs::from_slice(&request.body) {
                                Ok(json) => json,
                                Err(e) => {
                                    response.status = 400;
                                    response.body = format!("Failed to parse JSON: {}", e).into_bytes();
                                    return Ok(());
                                }
                            };
                            if let Err(e) = download_ingestion_data(
                                &s3_client_clone,
                                &bucket,
                                &json_body.user_id,
                                &json_body.instance_id,
                                &local_path,
                                &json_body.job_id,
                            )
                            .await
                            {
                                eprintln!("Failed to download ingestion data: {:?}", e);
                            }
                        }
                        else {
                            response.status = 404;
                            response.body = "Endpoint not found".as_bytes().to_vec();
                        }

                        // Send response back to client
                        match response.send(&mut conn).await {
                            Ok(_) => (),
                            Err(e) => {
                                eprintln!("Failed to send response: {:?}", e);
                            }
                        }
                        Ok(())
                    }
                    .await;

                    if let Err(e) = result {
                        eprintln!("Error in handler: {:?}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Error accepting connection: {:?}", e);
            }
        }
    }
}

#[derive(Debug)]
pub enum AdminError {
    AdminConnectionError(String, std::io::Error),
    S3Error(String, std::env::VarError),
    InvalidParameter(String),
}
// replace binary
// run

async fn download_ingestion_data(
    client: &Client,
    bucket: &str,
    user_id: &str,
    instance_id: &str,
    output_path: &str,
    job_id: &str,
) -> Result<()> {
    let key = format!("{}/bulk_upload/{}/{}/ingestion.jsonl", user_id, instance_id, job_id);

    // Create output file
    let output_path = format!("{}/ingestion.jsonl", output_path);
    let mut file = std::fs::File::create(&output_path)?;
    // Get the object as a stream
    let object = client.get_object().bucket(bucket).key(&key).send().await?;
    let mut stream = object.body;

    // Stream chunks directly to file
    while let Some(chunk) = stream.try_next().await? {
        file.write_all(&chunk)?;
    }

    let input = IngestSqlRequest {
        job_id: job_id.to_string(),
        job_name: job_id.to_string(),
        batch_size: 100,
        file_path: output_path.to_string(),
    };
    let input_json = sonic_rs::to_string(&input)?;
    // call helix ingest command at localhost:6969/ingest_sql
    let ingest_result = Command::new("curl")
        .arg("http://localhost:6969/ingest_sql")
        .arg("-X POST")
        .arg("-H \"Content-Type: application/json\"")
        .arg(format!("-d {}", input_json))
        .output();
    println!("Ingest result: {:?}", ingest_result);
    Ok(())
}