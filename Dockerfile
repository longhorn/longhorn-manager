# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b

ARG GOLANGCI_LINT_VERSION=v2.14.0@sha256:ad862ba6b3798cbe0fd9fd7408d498fd74fbd2623a92406b2fd3898faf0bf98f
FROM golangci/golangci-lint:${GOLANGCI_LINT_VERSION} AS golangci-lint

FROM registry.suse.com/bci/golang:1.27@sha256:107c1d9c1be31452eac6d71afff3fc33144001e515f4f7e843ebbdeedc5cee12 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy
ARG LONGHORN_TWO_MINOR_UPGRADE_DISTROS

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor
ENV LONGHORN_TWO_MINOR_UPGRADE_DISTROS=${LONGHORN_TWO_MINOR_UPGRADE_DISTROS}

# Install packages
RUN zypper update -y && \
    zypper -n install gcc ca-certificates git wget curl vim less file awk zip unzip && \
    rm -rf /var/cache/zypp/*

# Copy golangci-lint binary from official image
COPY --from=golangci-lint /usr/bin/golangci-lint /usr/local/bin/golangci-lint

WORKDIR /go/src/github.com/longhorn/longhorn-manager
COPY . .

FROM base AS build
RUN ./scripts/build

FROM base AS validate
RUN ./scripts/validate && touch /validate.done

FROM scratch AS build-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-manager/bin/ /bin/

FROM scratch AS ci-artifacts
COPY --from=build /go/src/github.com/longhorn/longhorn-manager/bin/ /bin/
COPY --from=validate /validate.done /validate.done
