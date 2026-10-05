FROM golang:1.24-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /egts-server ./cmd/server

FROM gcr.io/distroless/static-debian12

COPY --from=builder /egts-server /egts-server

EXPOSE 5555 9090

ENTRYPOINT ["/egts-server"]
CMD ["-config", "/app/config.yaml"]
