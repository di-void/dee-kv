FROM rust:latest AS builder
WORKDIR /app

# Install protoc
RUN apt-get update && \
    apt-get install -y protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

COPY Cargo.toml build.rs ./
COPY proto proto
COPY src src
# path to your cluster config
COPY DATA/docker-cluster.config.json ./

RUN cargo build --release

FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /app/target/release/dee-kv .
COPY --from=builder /app/docker-cluster.config.json .

EXPOSE 9000

ENTRYPOINT [ "./dee-kv" ]
CMD [ "--id=1", "--config=./cluster.config.json" ]
