# Runtime image based on Debian
FROM debian:bookworm-slim

# Set working directory
WORKDIR /app

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Copy the helix-container binary
COPY ./helix-container /usr/local/bin/helix-container
RUN chmod +x /usr/local/bin/helix-container

# Create data directory and set permissions
# - chown root:0 allows random OpenShift UIDs (which belong to group 0)
# - chmod g+rwX allows write access to group 0
RUN mkdir -p /data && chown -R root:0 /data && chmod -R g+rwX /data

# Define environment variable for data path
ENV HELIX_DATA_DIR=/data

# Optional: declare a non-root default UID (still OpenShift-safe)
USER 1001

# Entry point
ENTRYPOINT ["/usr/local/bin/helix-container"]
