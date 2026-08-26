# syntax=docker/dockerfile:1.26.0@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32

ARG GOLANGCI_LINT_VERSION=v2.12.2@sha256:5cceeef04e53efe1470638d4b4b4f5ceefd574955ab3941b2d9a68a8c9ad5240
FROM golangci/golangci-lint:${GOLANGCI_LINT_VERSION} AS golangci-lint

FROM registry.suse.com/bci/golang:1.26@sha256:f6f04957fbc16399c76b1e8266cc7d0f3b4a5434b824fb251d7e3ae559d6a910 AS base

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
