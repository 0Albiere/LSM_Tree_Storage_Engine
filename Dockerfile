# Use the official Rust image
FROM rust:latest

# Create and set the working directory
WORKDIR /usr/src/lsm-tree

# Copy the project files
COPY . .

# Build the project in release mode
RUN cargo build --release

# Run benchmarks by default
CMD ["cargo", "bench", "--bench", "ycsb"]
