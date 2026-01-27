#!/bin/sh

# Ensure cargo is in PATH
export PATH="$HOME/.cargo/bin:$PATH"

# Continue with build process

# if dev profile, build with dev profile
if [ "$1" = "dev" ]; then
    cargo install --profile dev --force --path . --root ~/.local
else
    cargo install --release --force --path . --root ~/.local
fi

if ! echo "$PATH" | grep -q "$HOME/.local/bin"; then
    if ! grep -Fxq 'export PATH="$HOME/.local/bin:$PATH"' ~/.bashrc; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.bashrc
    fi
    if ! grep -Fxq 'export PATH="$HOME/.local/bin:$PATH"' ~/.zshrc; then
        echo 'export PATH="$HOME/.local/bin:$PATH"' >> ~/.zshrc
    fi
    export PATH="$HOME/.local/bin:$PATH"
fi

