#!/bin/bash

# Set your repository
REPO="HelixDB/helix-db"

# Fetch the latest release version from GitHub API
VERSION=$(curl --silent "https://api.github.com/repos/$REPO/releases/latest" | grep '"tag_name"' | sed -E 's/.*"tag_name": "([^"]+)".*/\1/')

if [[ -z "$VERSION" ]]; then
    echo "Failed to fetch the latest version. Please check your internet connection or the repository."
    exit 1
fi

echo "User home directory: $HOME"
# Detect the operating system
OS=$(uname -s)
ARCH=$(uname -m)

INSTALL_DIR="$HOME/.local/bin"
mkdir -p "$INSTALL_DIR"

# Add the installation directory to PATH immediately for this session
export PATH="$INSTALL_DIR:$PATH"

# Ensure that $HOME/.local/bin is in the PATH permanently
if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    echo "Adding $HOME/.local/bin to PATH permanently"

    # Determine shell config file
    if [[ "$SHELL" == *"bash"* ]]; then
        SHELL_CONFIG="$HOME/.bashrc"
    elif [[ "$SHELL" == *"zsh"* ]]; then
        SHELL_CONFIG="$HOME/.zshrc"
    fi

    # Add to shell config if not already present
    if [[ -f "$SHELL_CONFIG" ]]; then
        if ! grep -q 'export PATH="$HOME/.local/bin:$PATH"' "$SHELL_CONFIG"; then
            echo 'export PATH="$HOME/.local/bin:$PATH"' >> "$SHELL_CONFIG"
        fi
    fi
fi

# Determine the appropriate binary to download
if [[ "$OS" == "Linux" && "$ARCH" == "x86_64" ]]; then
    FILE="helix-cli-linux-amd64"
elif [[ "$OS" == "Linux" && "$ARCH" == "aarch64" ]]; then
    FILE="helix-cli-linux-arm64"
elif [[ "$OS" == "Darwin" && "$ARCH" == "arm64" ]]; then
    FILE="helix-cli-macos-arm64"
elif [[ "$OS" == "Darwin" && "$ARCH" == "x86_64" ]]; then
    FILE="helix-cli-macos-amd64"
else
    echo "Unsupported system: This installer only works on Linux AMD64 and macOS ARM64"
    echo "Your system is: $OS $ARCH"
    exit 1
fi

# Download the binary
URL="https://github.com/$REPO/releases/download/$VERSION/$FILE"
echo "Downloading from $URL"

# Try to run the binary with current GLIBC
curl -L $URL -o "$INSTALL_DIR/helix"
if [[ "$OS" != "Windows_NT" ]]; then
    chmod +x "$INSTALL_DIR/helix"
fi

# Check if binary works with current GLIBC
if ! "$INSTALL_DIR/helix" --version &> /dev/null; then
    echo "Binary incompatible with system GLIBC version. Falling back to building from source..."
    rm "$INSTALL_DIR/helix"

    # Ensure Rust is installed
    if ! command -v cargo &> /dev/null; then
        echo "Installing Rust first..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    fi

    # Clone and build from source
    TMP_DIR=$(mktemp -d)
    git clone "https://github.com/$REPO.git" "$TMP_DIR"
    cd "$TMP_DIR"
    git checkout "$VERSION"
    cargo build --release
    mv "target/release/helix" "$INSTALL_DIR/helix"
    cd - > /dev/null
    rm -rf "$TMP_DIR"
fi

# Verify installation and ensure command is available
if command -v helix >/dev/null 2>&1; then
    echo "Installation successful!"
    echo "Helix CLI version $VERSION has been installed to $INSTALL_DIR/helix"
    echo "The 'helix' command is now available in your current shell"
    echo "For permanent installation, please restart your shell or run:"
    echo "    source $SHELL_CONFIG"
else
    echo "Installation failed."
    exit 1
fi

# Install Rust if needed
echo "Installing dependencies..."
if ! command -v cargo &> /dev/null
then
    echo "Rust/Cargo is not installed. Installing Rust..."
    if [[ "$OS" == "Linux" ]] || [[ "$OS" == "Darwin" ]]
    then
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
        source "$HOME/.cargo/env"
    elif [[ "$OS" == "Windows_NT" ]]
    then
        curl --proto '=https' --tlsv1.2 -sSf https://win.rustup.rs -o rustup-init.exe
        ./rustup-init.exe -y
        rm rustup-init.exe
    fi
else
    echo "Rust/Cargo is already installed. Skipping installation."
fi

# Final verification that helix is working
echo "Testing helix installation..."
if helix --version; then
    echo "Helix CLI is working correctly!"
else
    echo "Warning: Helix CLI is installed but may not be working correctly."
    echo "Please try running 'source $SHELL_CONFIG' or restart your terminal."
    exit 1
fi
