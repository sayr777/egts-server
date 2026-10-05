# ── Build stage ─────────────────────────────────────────────────────────────
FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /egts-server ./cmd/server

# ── Runtime stage ────────────────────────────────────────────────────────────
FROM scratch

COPY --from=builder /egts-server /egts-server
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 5555 9090

ENTRYPOINT ["/egts-server"]
CMD ["-config", "/app/config.yaml"]
