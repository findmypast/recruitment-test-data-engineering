FROM rust:1.40 as builder
WORKDIR /usr/src/example-rust
COPY . .
RUN cargo install --path .

FROM debian:buster-slim
COPY --from=builder /usr/local/cargo/bin/example-rust /usr/local/bin/example-rust
CMD ["example-rust"]
