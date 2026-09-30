# syntax=docker/dockerfile:1.27.0@sha256:bde3983e9c939224420ddaf6b784cc30e09b035a4dea01f581230c50809f372e

ARG GOLANGCI_LINT_VERSION=v2.13.2@sha256:ba07dffad130794ae79ebaa0056809d18c0168f3f846480ffd3eb6c04578b83d
FROM golangci/golangci-lint:${GOLANGCI_LINT_VERSION} AS golangci-lint

FROM registry.suse.com/bci/golang:1.27@sha256:3133af6c5d2aa363aa482d9877b807b513597ac3fe01b735a458ef1d0a341c7d AS base

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
