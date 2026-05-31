# syntax=docker/dockerfile:1.24.0@sha256:87999aa3d42bdc6bea60565083ee17e86d1f3339802f543c0d03998580f9cb89
FROM registry.suse.com/bci/golang:1.26@sha256:9b829b1026dac4281baa0715c4dd975735665d5ff843b0e1cd20e3fbbde95302 AS base

ARG TARGETARCH
ARG http_proxy
ARG https_proxy
ARG LONGHORN_TWO_MINOR_UPGRADE_DISTROS

ENV GOLANGCI_LINT_VERSION=v2.11.4

ENV ARCH=${TARGETARCH}
ENV GOFLAGS=-mod=vendor
ENV LONGHORN_TWO_MINOR_UPGRADE_DISTROS=${LONGHORN_TWO_MINOR_UPGRADE_DISTROS}

# Install packages
RUN zypper update -y && \
    zypper -n install gcc ca-certificates git wget curl vim less file awk zip unzip && \
    rm -rf /var/cache/zypp/*

# Install golangci-lint
RUN curl -fsSL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh -o /tmp/install.sh \
    && chmod +x /tmp/install.sh \
    && /tmp/install.sh -b /usr/local/bin ${GOLANGCI_LINT_VERSION}

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
