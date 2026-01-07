#!/bin/sh

# Check if Cargo is installed
if ! command -v cargo &> /dev/null
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

# if dev profile, build with dev profile
if [ "$1" = "dev" ]; then
    cargo build --profile dev && cargo install --profile dev --path . --root ~/.local
elif [ "$1" = "offline" ]; then
    cargo build --profile dev --offline && cargo install --profile dev --offline --path . --root ~/.local
else
    cargo build --release && cargo install --path . --root ~/.local
fi

# Add helix installation path to $PATH 
if ! echo "$PATH" | grep -q "$HOME/.local/bin"; then
    export PATH="$HOME/.local/bin:$PATH"
fi

