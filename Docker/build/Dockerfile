FROM --platform=linux/amd64 golang:1.25-bookworm

# Install build tools + Node.js (LTS via NodeSource)
RUN apt-get update && apt-get install -y --no-install-recommends \
    make \
    git \
    ca-certificates \
    build-essential \
    curl \
 && curl -fsSL https://deb.nodesource.com/setup_lts.x | bash - \
 && apt-get install -y --no-install-recommends nodejs \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

# Pre-warm the Go module cache (layer invalidates only when go.mod/go.sum change).
COPY go.mod go.sum ./
RUN go mod download

ENV GOCACHE=/root/.cache/go-build
ENV GOMODCACHE=/go/pkg/mod

CMD ["bash"]
