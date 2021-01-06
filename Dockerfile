FROM rust:1-slim-stretch as builder

WORKDIR /srv/addresses-importer

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y libgeos-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=remidupre/libpostal /usr/local/lib/libpostal.so /usr/local/lib/
COPY . ./

RUN cd deduplicator && cargo build --release
RUN rm /usr/local/lib/libpostal.so


FROM remidupre/libpostal

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive

COPY --from=builder /srv/addresses-importer/deduplicator/target/release/deduplicator /usr/bin/deduplicator

RUN ldconfig

ENTRYPOINT ["deduplicator"]
