#!/bin/bash
set -euo pipefail

# Helix CLI Installer
# Cross-platform installer for Helix CLI

readonly REPO="HelixDB/helix-db"
readonly BINARY_NAME="helix"
readonly DEFAULT_INSTALL_DIR="$HOME/.local/bin"

# Colors
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[1;33m'
readonly BLUE='\033[0;34m'
readonly NC='\033[0m'

# Global variables
INSTALL_DIR=""
FORCE_INSTALL=false
SYSTEM_INSTALL=false

# Logging functions
log_error() { echo -e "${RED}ERROR:${NC} $*" >&2; }
log_success() { echo -e "${GREEN}SUCCESS:${NC} $*"; }
log_info() { echo -e "${BLUE}INFO:${NC} $*"; }
log_warn() { echo -e "${YELLOW}WARN:${NC} $*"; }

# Print usage information
usage() {
    cat << EOF
Helix CLI Installer

USAGE:
    curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash
    
    # Or with options:
    curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash -s -- [OPTIONS]

OPTIONS:
    -d, --dir <DIR>     Install directory (default: ~/.local/bin)
    -s, --system        System install (/usr/local/bin, requires sudo)
    -f, --force         Force install even if same version exists
    -h, --help          Show this help

EXAMPLES:
    # User install (default)
    curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash
    
    # System install
    curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash -s -- --system
    
    # Custom directory
    curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | bash -s -- --dir ~/bin
EOF
}

# Parse command line arguments
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -d|--dir)
                INSTALL_DIR="$2"
                shift 2
                ;;
            -s|--system)
                SYSTEM_INSTALL=true
                shift
                ;;
            -f|--force)
                FORCE_INSTALL=true
                shift
                ;;
            -h|--help)
                usage
                exit 0
                ;;
            *)
                log_error "Unknown option: $1"
                usage
                exit 1
                ;;
        esac
    done
}

# Set installation directory based on options
set_install_dir() {
    if [[ -n "$INSTALL_DIR" ]]; then
        # Custom directory specified
        INSTALL_DIR=$(realpath "$INSTALL_DIR" 2>/dev/null || echo "$INSTALL_DIR")
    elif [[ "$SYSTEM_INSTALL" == true ]]; then
        # System install
        if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
            INSTALL_DIR="/c/Program Files/helix"
        else
            INSTALL_DIR="/usr/local/bin"
        fi
    else
        # Default user install
        INSTALL_DIR="$DEFAULT_INSTALL_DIR"
    fi

    log_info "Install directory: $INSTALL_DIR"
}

# Detect platform and architecture
detect_platform() {
    local target

    # Detect OS and architecture combination to match Rust targets
    case "$OSTYPE" in
        linux*)
            case "$(uname -m)" in
                x86_64|amd64)
                    target="x86_64-unknown-linux-gnu"
                    ;;
                aarch64|arm64)
                    target="aarch64-unknown-linux-gnu"
                    ;;
                *)
                    log_error "Unsupported architecture: $(uname -m)"
                    exit 1
                    ;;
            esac
            ;;
        darwin*)
            case "$(uname -m)" in
                x86_64|amd64)
                    target="x86_64-apple-darwin"
                    ;;
                aarch64|arm64)
                    target="aarch64-apple-darwin"
                    ;;
                *)
                    log_error "Unsupported architecture: $(uname -m)"
                    exit 1
                    ;;
            esac
            ;;
        msys*|cygwin*)
            case "$(uname -m)" in
                x86_64|amd64)
                    target="x86_64-pc-windows-msvc"
                    ;;
                *)
                    log_error "Unsupported architecture: $(uname -m)"
                    exit 1
                    ;;
            esac
            ;;
        *)
            log_error "Unsupported OS: $OSTYPE"
            exit 1
            ;;
    esac

    # Set binary name with extension for Windows
    local binary_ext=""
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        binary_ext=".exe"
    fi

    echo "${BINARY_NAME}-${target}${binary_ext}"
}

# Get latest release version from GitHub
get_latest_version() {
    local version
    version=$(curl -fsSL "https://api.github.com/repos/$REPO/releases/latest" | \
              grep '"tag_name"' | \
              sed -E 's/.*"tag_name": "([^"]+)".*/\1/')
    
    if [[ -z "$version" ]]; then
        log_error "Failed to fetch latest version from GitHub API"
        exit 1
    fi
    
    echo "$version"
}

# Get version of installed binary
get_installed_version() {
    local binary_path="$INSTALL_DIR/$BINARY_NAME"
    
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        binary_path="$binary_path.exe"
    fi
    
    if [[ -x "$binary_path" ]]; then
        "$binary_path" --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1 || echo ""
    else
        echo ""
    fi
}

# Check if update is needed
should_install() {
    local latest_version="$1"
    local installed_version
    
    if [[ "$FORCE_INSTALL" == true ]]; then
        log_info "Force install requested"
        return 0
    fi
    
    installed_version=$(get_installed_version)
    
    if [[ -z "$installed_version" ]]; then
        log_info "No existing installation found"
        return 0
    fi
    
    # Remove 'v' prefix for comparison
    latest_version="${latest_version#v}"
    
    log_info "Installed version: $installed_version"
    log_info "Latest version: $latest_version"
    
    if [[ "$installed_version" == "$latest_version" ]]; then
        log_success "Already up to date!"
        log_info "Use --force to reinstall"
        return 1
    fi
    
    log_info "Update available: $installed_version -> $latest_version"
    return 0
}

# Download and install binary
install_binary() {
    local version="$1"
    local binary_filename="$2"
    local download_url="https://github.com/$REPO/releases/download/$version/$binary_filename"
    local binary_path="$INSTALL_DIR/$BINARY_NAME"
    
    # Add .exe extension on Windows
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        binary_path="$binary_path.exe"
    fi
    
    log_info "Downloading: $download_url"
    
    # Create install directory
    mkdir -p "$INSTALL_DIR"
    
    # Download to temporary file
    local temp_file
    temp_file=$(mktemp)
    
    if ! curl -fsSL "$download_url" -o "$temp_file"; then
        log_error "Failed to download binary"
        rm -f "$temp_file"
        exit 1
    fi
    
    # Make executable
    chmod +x "$temp_file"
    
    # Atomic move to final location
    if ! mv "$temp_file" "$binary_path"; then
        log_error "Failed to install binary to $binary_path"
        log_info "Check permissions or try --system for system-wide install"
        rm -f "$temp_file"
        exit 1
    fi
    
    log_success "Installed to: $binary_path"
}

# Setup PATH for user installs
setup_path() {
    if [[ "$SYSTEM_INSTALL" == true ]]; then
        # System installs should already be in PATH
        return 0
    fi
    
    # Only setup PATH for default user installs
    if [[ "$INSTALL_DIR" != "$DEFAULT_INSTALL_DIR" ]]; then
        log_info "Custom install directory. Add to PATH manually:"
        log_info "  export PATH=\"$INSTALL_DIR:\$PATH\""
        return 0
    fi
    
    local shell_config=""
    local path_line=""
    
    # Determine shell config file
    case "$SHELL" in
        */bash)
            shell_config="$HOME/.bashrc"
            path_line="export PATH=\"\$HOME/.local/bin:\$PATH\""
            ;;
        */zsh)
            shell_config="$HOME/.zshrc"
            path_line="export PATH=\"\$HOME/.local/bin:\$PATH\""
            ;;
        */fish)
            shell_config="$HOME/.config/fish/config.fish"
            path_line="set -gx PATH \$HOME/.local/bin \$PATH"
            ;;
        *)
            log_warn "Unknown shell: $SHELL"
            log_info "Add to PATH manually: export PATH=\"\$HOME/.local/bin:\$PATH\""
            return 0
            ;;
    esac
    
    # Add to shell config if not already present
    if [[ -f "$shell_config" ]] && ! grep -Fq "$path_line" "$shell_config"; then
        echo "" >> "$shell_config"
        echo "# Added by Helix CLI installer" >> "$shell_config"
        echo "$path_line" >> "$shell_config"
        log_success "Added $INSTALL_DIR to PATH in $shell_config"
        log_info "Restart your shell or run: source $shell_config"
    fi
}

# Verify installation
verify_installation() {
    local binary_path="$INSTALL_DIR/$BINARY_NAME"
    
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
        binary_path="$binary_path.exe"
    fi
    
    if [[ ! -x "$binary_path" ]]; then
        log_error "Installation verification failed: binary not executable"
        return 1
    fi
    
    local installed_version
    installed_version=$(get_installed_version)
    
    if [[ -n "$installed_version" ]]; then
        log_success "Installation verified: v$installed_version"
    else
        log_warn "Could not verify version, but binary is installed"
    fi
    
    # Test basic functionality
    if "$binary_path" --help >/dev/null 2>&1; then
        log_success "Basic functionality test passed"
    else
        log_warn "Basic functionality test failed - binary may need additional setup"
    fi
}

# Check if Homebrew is installed (Mac only)
check_homebrew() {
    if command -v brew >/dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Check if Docker Desktop is installed and prompt for installation (Mac only)
check_docker_desktop() {
    # Only run on macOS
    if [[ "$OSTYPE" != "darwin"* ]]; then
        return 0
    fi

    log_info "Checking for Docker Desktop..."

    # Check if Docker Desktop is installed
    # Check both /Applications/Docker.app and docker command availability
    if [[ -d "/Applications/Docker.app" ]] || command -v docker >/dev/null 2>&1; then
        log_success "Docker Desktop is installed"
        return 0
    fi

    log_warn "Docker Desktop is not installed"
    log_info "Helix CLI requires Docker to be running for local development"

    # Check if Homebrew is available
    if ! check_homebrew; then
        log_warn "Homebrew is not installed"
        log_warn "Please ensure you have Docker Desktop installed and running"
        log_info "You can install Docker Desktop from: https://www.docker.com/products/docker-desktop"
        return 0
    fi

    # Prompt user for installation
    echo ""
    read -p "Would you like to install Docker Desktop via Homebrew? (y/n): " -n 1 -r
    echo ""

    if [[ $REPLY =~ ^[Yy]$ ]]; then
        log_info "Installing Docker Desktop via Homebrew..."
        log_info "This may take several minutes..."

        if brew install --cask docker; then
            log_success "Docker Desktop installed successfully"
            log_info "Please start Docker Desktop from your Applications folder"
            log_info "You'll need to complete the Docker Desktop setup before using Helix CLI"
        else
            log_error "Failed to install Docker Desktop via Homebrew"
            log_warn "Please install Docker Desktop manually from: https://www.docker.com/products/docker-desktop"
        fi
    else
        log_warn "Skipping Docker Desktop installation"
        log_warn "IMPORTANT: You need to have the Docker daemon running to use Helix CLI"
        log_info "Install Docker Desktop from: https://www.docker.com/products/docker-desktop"
    fi

    echo ""
}

# Main installation function
main() {
    log_info "Helix CLI Installer"
    log_info "Repository: $REPO"
    
    parse_args "$@"
    set_install_dir
    
    # Check for required tools
    if ! command -v curl >/dev/null; then
        log_error "curl is required but not installed"
        exit 1
    fi
    
    local binary_filename latest_version
    
    binary_filename=$(detect_platform)
    log_info "Platform: $binary_filename"
    
    latest_version=$(get_latest_version)
    log_info "Latest version: $latest_version"
    
    if ! should_install "$latest_version"; then
        exit 0
    fi
    
    # Check permissions for system install
    if [[ "$SYSTEM_INSTALL" == true ]] && [[ ! -w "$INSTALL_DIR" ]] && [[ $EUID -ne 0 ]]; then
        log_error "System install requires sudo permissions"
        log_info "Run: curl -fsSL https://raw.githubusercontent.com/$REPO/main/install.sh | sudo bash -s -- --system"
        exit 1
    fi
    
    install_binary "$latest_version" "$binary_filename"
    setup_path
    verify_installation

    # Check for Docker Desktop on Mac
    check_docker_desktop

    log_success "Installation complete!"
    log_info ""
    log_info "Next steps:"
    log_info "1. Restart your shell or run: source ~/.bashrc (or ~/.zshrc)"
    log_info "2. Run: helix --version"
    log_info "3. Run: helix --help"
    log_info ""
    log_info "To update in the future, run: helix update"
    log_info ""
    log_info "Anonymous metrics are enabled by default."
    log_info "To help us improve Helix, please consider enabling full metrics."
    log_info "Run: helix metrics --full"
    log_info ""
    log_info "To disable metrics, run: helix metrics --off"
    log_info ""
    log_info "To show metrics status, run: helix metrics --status"
}

main "$@"