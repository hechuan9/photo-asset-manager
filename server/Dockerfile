FROM rust:1.94-bookworm AS build
WORKDIR /src
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/src ./src
RUN cargo build --release --locked

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl tzdata imagemagick libraw-bin libimage-exiftool-perl libheif-examples libheif-plugin-libde265 libheif-plugin-x265 libheif-plugin-jpegdec util-linux \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/keeps-server /usr/local/bin/keeps-server
COPY --from=build /src/target/release/keeps-inspect /usr/local/bin/keeps-inspect
ENV RUST_LOG=keeps_server=info,tower_http=info RUST_BACKTRACE=1
EXPOSE 2283
ENTRYPOINT ["keeps-server"]
