FROM rust:bookworm as build-stage
ARG PROJECT_VERSION
WORKDIR /mqtt
COPY . ./
RUN cargo build --release

FROM debian:bookworm as production-stage
ARG DEBIAN_FRONTEND=noninteractive
RUN apt update && apt-get --yes install libsqlite3-0 libssl3 && apt clean
RUN groupadd -r mqtt && useradd -r -g mqtt -d /opt/mqtt -s /usr/sbin/nologin mqtt
RUN mkdir -p /opt/mqtt/cert
RUN mkdir -p /opt/mqtt/bin
RUN mkdir -p /opt/mqtt/etc
RUN mkdir -p /opt/mqtt/data
COPY config.yaml /opt/mqtt/etc/
COPY --from=build-stage /mqtt/target/release/mqtt /opt/mqtt/bin/
RUN chmod +x /opt/mqtt/bin/mqtt
RUN chown -R mqtt:mqtt /opt/mqtt

WORKDIR /opt/mqtt
USER mqtt

ENTRYPOINT [ "/opt/mqtt/bin/mqtt", "/opt/mqtt/etc/config.yaml" ]
