# ----------- BUILDER STAGE -----------
FROM golang:1.22-bookworm AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN apt-get update && apt-get install -y gcc libc6-dev
RUN CGO_ENABLED=1 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o sentra ./cmd

# ----------- RUNTIME STAGE -----------
FROM ubuntu:22.04

LABEL org.opencontainers.image.title="Sentra Agent"
LABEL org.opencontainers.image.description="Distributed compute runtime for data processing and ML workloads"

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libc6 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd -r sentra && useradd -r -g sentra sentra

WORKDIR /app

COPY --from=builder /app/sentra .

RUN mkdir -p /root/.sentra/plugins \
    && chown -R sentra:sentra /app \
    && chown -R sentra:sentra /root/.sentra

USER sentra

ENV SENTRA_HOME=/root/.sentra
ENV HEALTH_CHECK_PORT=8080

EXPOSE 8080

CMD ["./sentra"]
