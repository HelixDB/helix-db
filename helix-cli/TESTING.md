# Helix CLI Testing Guide

For each of these, make sure you're in the helix-cli directory.
Then, create a directory called test-(some name).
`cd` into that directory.
Instead of the `helix` command in the commands below, use `cargo run -- <args passed to helix>`.

## Local Flows (Non-Cloud Testing)

- `helix init` with default settings; check helix.toml created with empty template and ./db/ queries path
- `helix init --path /custom/path` with custom directory; verify project created in specified location
- `helix init --template custom_template` with custom template; confirm template applied correctly
- `helix init --queries-path ./custom-queries/` with custom queries directory; validate queries path set correctly
- `helix check` to validate all instances; verify all configurations and queries validated
- `helix check my-instance` to validate specific instance; confirm only specified instance checked
- `helix compile` to compile queries; verify queries compiled to default output
- `helix compile --path ./custom-output --output my-instance` with custom settings; check compilation to specified path and instance
- `helix build my-instance` to build local Docker instance; verify Dockerfile and docker-compose.yml generated; confirm Docker image built successfully
- `helix push my-instance` to deploy local Docker instance; verify container starts and is accessible on configured port
- `helix start my-instance` to start existing local Docker instance; verify container starts without rebuild
- `helix stop my-instance` to stop running local Docker instance; confirm container stops cleanly
- `helix status` to view all instances; confirm all instances listed with correct status and Docker container states
- `helix prune` to clean unused resources; verify containers, images cleaned while preserving volumes
- `helix prune my-instance` to clean specific instance resources; confirm only specified instance cleaned
- `helix prune --all` to clean all instances; verify all project instances cleaned
- `helix metrics full` to enable full metrics; verify metrics collection enabled
- `helix metrics basic` to enable basic metrics; confirm reduced metrics collection
- `helix metrics off` to disable metrics; verify metrics collection disabled
- `helix metrics status` to check metrics state; confirm current metrics setting displayed
- `helix update` to upgrade to latest version; verify CLI updated successfully
- `helix update --force` to force update; confirm update proceeds even if already latest
- `helix init` in directory with existing helix.toml; verify appropriate error message
- `helix build non-existent-instance` with invalid instance; confirm error for missing instance
- `helix start my-instance` without building first; verify error about missing docker-compose.yml
- `helix build my-instance` without Docker installed/running; confirm Docker availability error
- `helix push my-instance` without Docker daemon running; verify Docker daemon error
- `helix add` with conflicting instance names; verify duplicate name error

## Cloud/Remote Flows

## Project Initialization

- `helix init --cloud` with cloud instance; verify cloud instance configured in helix.toml
- `helix init --cloud --cloud-region eu-west-1` with custom region; check region set correctly
- `helix init --ecr` with ECR instance; confirm ECR instance added to config
- `helix init --fly` with Fly.io instance; verify Fly instance created with default settings
- `helix init --fly --fly-auth token --fly-volume-size 50 --fly-vm-size performance-2x --fly-public false` with custom Fly settings; check all parameters applied

## Instance Management

- `helix add my-instance --cloud` to add cloud instance; verify instance added to existing project
- `helix add my-ecr --ecr` to add ECR instance; confirm ECR instance configured
- `helix add my-fly --fly --fly-volume-size 30` to add Fly instance with custom volume; check instance created with correct volume size
- `helix delete my-instance` to remove instance; verify instance completely removed from config and infrastructure

## Build and Deployment

- `helix build my-instance` to build instance; verify build process completes successfully
- `helix push my-instance` to deploy instance; confirm instance deployed and running
- `helix start my-instance` to start existing instance; verify instance starts without rebuild
- `helix stop my-instance` to stop running instance; confirm instance stops cleanly

## Data Operations

- `helix pull my-instance` to sync .hql files from remote; verify local queries updated from instance

## Authentication

- `helix auth login` to authenticate with Helix cloud; verify login successful and credentials stored
- `helix auth logout` to sign out; confirm credentials cleared
- `helix auth create-key my-cluster` to generate API key; verify key created for specified cluster

## Error Scenarios

- `helix push` without building first; verify appropriate build dependency error
- Commands requiring authentication without login; confirm proper authentication error messages
