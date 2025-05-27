#!/bin/sh

# Check if Rust is installed
if ! command -v rustc &> /dev/null
then
    echo "Rust is not installed. Installing Rust..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    # Source cargo environment
    export PATH="$HOME/.cargo/bin:$PATH"
    source "$HOME/.cargo/env"
    cargo update
else
    echo "Rust is already installed. Skipping installation."
fi

# Ensure cargo is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Continue with build process
cargo build --release && cargo install --path .