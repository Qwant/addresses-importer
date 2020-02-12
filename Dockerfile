FROM debian:stretch-slim as libpostal

WORKDIR /srv/libpostal

ENV LIBPOSTAL_COMMIT "95f31de3b25eaf0b23c8efd97b1243d9d690ba58"

RUN apt-get update \
    && apt-get install -y git autoconf libtool build-essential curl libgeos-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN git clone https://github.com/openvenues/libpostal libpostal \
    && cd libpostal \
    && git checkout ${LIBPOSTAL_COMMIT} \
    && ./bootstrap.sh \
    && ./configure --datadir=/srv/data \
    && make install -j 4 \
    && cd .. \
    && rm -rf libpostal


FROM rust:1-slim-stretch as builder

WORKDIR /srv/addresses-importer

ENV DEBIAN_FRONTEND noninteractive

RUN apt-get update \
    && apt-get install -y libgeos-dev libsqlite3-dev \
    && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

COPY --from=libpostal /usr/local/lib/libpostal.so /usr/local/lib/
COPY . ./

RUN cd deduplicator && cargo build --release
RUN rm /usr/local/lib/libpostal.so


FROM libpostal

WORKDIR /srv

ENV DEBIAN_FRONTEND noninteractive

COPY --from=builder /srv/addresses-importer/deduplicator/target/release/deduplicator /usr/bin/deduplicator

RUN ldconfig

ENTRYPOINT ["deduplicator"]
