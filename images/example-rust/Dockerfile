FROM rust:1.40
WORKDIR /usr/src/myapp
COPY . .
RUN cargo install --path .
ENTRYPOINT cargo run