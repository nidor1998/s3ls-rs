FROM rust:1-trixie AS builder
WORKDIR /s3ls
COPY . ./
RUN git config --global --add safe.directory /s3ls \
&& cargo build --release

FROM debian:trixie-slim
RUN apt-get update \
&& apt-get install --no-install-recommends -y ca-certificates \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

COPY --from=builder /s3ls/target/release/s3ls /usr/local/bin/s3ls

RUN useradd -m -s /bin/bash s3ls
USER s3ls
WORKDIR /home/s3ls/
ENTRYPOINT ["/usr/local/bin/s3ls"]
