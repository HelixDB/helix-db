# Use single stage to keep all source and build tools for development
FROM rust:1.88

# Install build dependencies for LMDB
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Set up working directory
WORKDIR /usr/src/app

# Copy source code
COPY . .

# Create non-root user with proper permissions
RUN useradd -m -u 1000 user && \
    chown -R user:user /usr/src/app && \
    mkdir -p /home/user/.helix && \
    chown -R user:user /home/user/.helix

USER user

# Create data directory with proper permissions
RUN mkdir -p /usr/src/app/data

# Expose the default port
EXPOSE 6969

# Build and run in development mode - this will run every time the container starts
CMD ["sh", "-c", "cd helix-container && RUSTFLAGS='' cargo build && ../target/debug/helix-container"]
