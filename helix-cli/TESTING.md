# Helix CLI v2 Manual Testing Guide

This document provides manual test cases to verify all CLI commands work correctly during development.

## Test Environment Setup

### Prerequisites
- Rust toolchain installed
- Docker and Docker Compose running
- Git available
- Test in a clean directory structure

### Build the CLI
```bash
cd helix-cli
cargo build --release
# Binary will be at target/release/helix
```

## Manual Test Suite

### 1. `helix init` - Project Initialization

**Test 1.1: Initialize in empty directory**
```bash
mkdir test-init && cd test-init
helix init
```
**Expected Results:**
- Creates `helix.toml` with default configuration
- Creates `schema.hx` with example content
- Creates `queries.hx` with example content  
- Creates `.gitignore`
- Creates `.helix/` directory
- Shows success message with next steps

**Test 1.2: Initialize with custom path**
```bash
helix init --path ./my-custom-project
```
**Expected Results:**
- Creates project in `./my-custom-project/` directory
- All files created in the specified path

**Test 1.3: Error - Initialize in existing project**
```bash
# From previous test directory
helix init
```
**Expected Results:**
- Error message about `helix.toml` already existing
- No files overwritten

---

### 2. `helix check` - Project Validation

**Test 2.1: Check valid project**
```bash
cd test-init  # from previous test
helix check
```
**Expected Results:**
- Shows "Checking all instances" message
- Validates `schema.hx` and `queries.hx` exist
- Shows success message for all instances

**Test 2.2: Check specific instance**
```bash
helix check dev
```
**Expected Results:**
- Shows "Checking instance 'dev'" message
- Validates the dev instance configuration
- Shows success message

**Test 2.3: Error - Check outside project**
```bash
cd /tmp
helix check
```
**Expected Results:**
- Error about not being in a Helix project directory

**Test 2.4: Error - Missing files**
```bash
cd test-init
mv schema.hx schema.hx.bak
helix check
```
**Expected Results:**
- Error about missing `schema.hx` file
```bash
mv schema.hx.bak schema.hx  # restore
```

---

### 3. `helix build` - Build Instance

**Test 3.1: Build local instance**
```bash
cd test-init
helix build dev
```
**Expected Results:**
- Shows "Building instance 'dev'" message
- Caches Helix repository (first time)
- Creates instance workspace in `.helix/instances/dev/`
- Generates `helix-container/` directory with compiled files
- Creates Dockerfile and docker-compose.yml
- Builds Docker image (if Docker available)
- Shows success message

**Test 3.2: Error - Build non-existent instance**
```bash
helix build nonexistent
```
**Expected Results:**
- Error message about instance 'nonexistent' not found

**Test 3.3: Error - Build outside project**
```bash
cd /tmp
helix build dev
```
**Expected Results:**
- Error about not being in a Helix project directory

---

### 4. `helix push` - Deploy Instance

**Test 4.1: Push local instance**
```bash
cd test-init
helix push dev
```
**Expected Results:**
- Shows "Deploying local instance 'dev'" message
- Starts Docker container
- Shows success message with:
  - Local URL (http://localhost:6969)
  - Container name
  - Data volume path

**Test 4.2: Error - Push without building**
```bash
# In a fresh project
mkdir test-push && cd test-push
helix init
helix push dev
```
**Expected Results:**
- Error about needing to build first or Docker image not found

**Test 4.3: Error - Push non-existent instance**
```bash
cd test-init
helix push nonexistent
```
**Expected Results:**
- Error about instance 'nonexistent' not found

---

### 5. `helix status` - Show Status

**Test 5.1: Status in project with running instance**
```bash
cd test-init  # should have dev instance running
helix status
```
**Expected Results:**
- Shows project name and root directory
- Lists configured instances (Local and Cloud)
- Shows Docker container status
- Shows running containers with status icons

**Test 5.2: Status in project without running instances**
```bash
mkdir test-status && cd test-status
helix init
helix status
```
**Expected Results:**
- Shows project information
- Shows "Running Containers: None"

**Test 5.3: Error - Status outside project**
```bash
cd /tmp
helix status
```
**Expected Results:**
- Error message about not being in Helix project directory

---

### 6. `helix pull` - Pull Queries

**Test 6.1: Pull from local instance**
```bash
cd test-init
helix pull dev
```
**Expected Results:**
- Warning message about local instance query extraction not implemented
- Informational message about limitation

**Test 6.2: Error - Pull non-existent instance**
```bash
helix pull nonexistent
```
**Expected Results:**
- Error about instance 'nonexistent' not found

---

### 7. `helix cloud` - Cloud Operations

**Test 7.1: Cloud login**
```bash
helix cloud login
```
**Expected Results:**
- Shows "Logging into Helix Cloud" message
- Warning about cloud authentication not implemented
- Informational message about future browser authentication

**Test 7.2: Cloud logout**
```bash
helix cloud logout
```
**Expected Results:**
- Shows "Logging out of Helix Cloud" message
- Message about "Not currently logged in" (since login not implemented)

**Test 7.3: Create API key**
```bash
helix cloud create-key my-cluster
```
**Expected Results:**
- Shows "Creating API key for cluster: my-cluster" message
- Warning about API key creation not implemented

---

### 8. `helix prune` - Cleanup

**Test 8.1: Prune specific instance**
```bash
cd test-init
helix prune dev
```
**Expected Results:**
- Shows "Pruning instance 'dev'" message
- Stops and removes Docker containers
- Removes instance workspace directory
- Shows success message

**Test 8.2: Prune unused resources**
```bash
helix prune
```
**Expected Results:**
- Shows "Pruning unused Docker resources" message
- Runs `docker system prune -f`
- Shows success message

**Test 8.3: Prune all instances**
```bash
cd test-init
helix prune --all
```
**Expected Results:**
- Shows "Pruning all instances" message
- Stops all project containers
- Removes entire `.helix/` directory
- Shows success message

---

### 9. `helix delete` - Delete Instance

**Test 9.1: Delete instance**
```bash
cd test-init
helix build dev && helix push dev  # ensure it exists and is running
helix delete dev
```
**Expected Results:**
- Warning about permanent deletion
- Shows "Deleting instance 'dev'" message
- Stops and removes containers
- Removes workspace and volumes
- Shows success message

**Test 9.2: Error - Delete non-existent instance**
```bash
helix delete nonexistent
```
**Expected Results:**
- Error about instance 'nonexistent' not found

---

### 10. `helix metrics` - Metrics Management

**Test 10.1: Enable metrics**
```bash
helix metrics on
```
**Expected Results:**
- Shows "Enabling metrics collection" message
- Creates `~/.helix/metrics.toml` with enabled=true
- Shows success message about anonymous usage data

**Test 10.2: Show metrics status**
```bash
helix metrics status
```
**Expected Results:**
- Shows "Metrics Status"
- Shows "Enabled: Yes"
- Shows last updated information

**Test 10.3: Disable metrics**
```bash
helix metrics off
```
**Expected Results:**
- Shows "Disabling metrics collection" message
- Updates `~/.helix/metrics.toml` with enabled=false
- Shows success message

---

## Complete End-to-End Workflow Test

**Full Local Development Workflow:**
```bash
# 1. Setup
mkdir e2e-test && cd e2e-test

# 2. Initialize
helix init
# ✓ Verify all files created

# 3. Validate  
helix check
# ✓ Verify validation passes

# 4. Build
helix build dev
# ✓ Verify build completes successfully

# 5. Deploy
helix push dev
# ✓ Verify instance starts and is accessible

# 6. Check status
helix status
# ✓ Verify shows running instance

# 7. Stop instance
helix prune dev
# ✓ Verify instance stopped and cleaned

# 8. Final cleanup
helix delete dev
# ✓ Verify complete removal
```

## Quick Smoke Test

For rapid verification during development:

```bash
# Build CLI
cd helix-cli && cargo build --release

# Quick test sequence
mkdir smoke-test && cd smoke-test
../target/release/helix init
../target/release/helix check
../target/release/helix status
cd .. && rm -rf smoke-test
```

## Error Condition Tests

Test these error scenarios to ensure proper error handling:

1. **No Docker running**: Stop Docker and try `helix build dev`
2. **Permission denied**: Try to init in a read-only directory  
3. **Invalid TOML**: Corrupt the `helix.toml` file and run `helix check`
4. **Missing files**: Remove `schema.hx` and run `helix build dev`
5. **Invalid instance**: Use non-existent instance name with any command

## Notes for Development

- Always test after making changes to command implementations
- Verify error messages are helpful and actionable
- Check that success messages provide useful information
- Ensure cleanup commands actually remove created resources
- Test both happy path and error conditions for each command