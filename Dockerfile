FROM alpine:3.19

LABEL org.opencontainers.image.title="Sentra Agent"
LABEL org.opencontainers.image.description="Distributed compute runtime for data processing and ML workloads"

RUN apk add --no-cache ca-certificates python3 py3-pip && \
    python3 -m venv --help >/dev/null 2>&1 && \
    addgroup -S sentra && adduser -S sentra -G sentra

WORKDIR /app

COPY bin/sentra-agent-linux-amd64 /app/sentra-agent

RUN mkdir -p /root/.sentra/plugins /root/.sentra/tokens && \
    chown -R sentra:sentra /app /root/.sentra

USER sentra

ENV SENTRA_HOME=/root/.sentra
ENV HEALTH_CHECK_PORT=8080

EXPOSE 8080

CMD ["/app/sentra-agent"]
