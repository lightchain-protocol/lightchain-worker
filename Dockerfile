FROM golang:1.26-alpine AS builder
WORKDIR /workspace
COPY worker/go.mod worker/go.sum* worker/
RUN cd worker && GOWORK=off go mod download
COPY pkg/ pkg/
COPY worker/ worker/
ARG VERSION=dev
RUN printf 'go 1.26\n\nuse (\n\t./pkg\n\t./worker\n)\n' > go.work
RUN cd worker && CGO_ENABLED=0 GOOS=linux go build -ldflags="-X github.com/lightchain/worker/internal/buildinfo.Version=${VERSION}" -o /bin/worker ./cmd/sidecar
RUN cd worker && CGO_ENABLED=0 GOOS=linux go build -ldflags="-X github.com/lightchain/worker/internal/buildinfo.Version=${VERSION}" -o /bin/lightchain-worker ./cmd/cli

FROM alpine:3.19
RUN apk add --no-cache ca-certificates && adduser -D -u 1000 appuser
RUN mkdir -p /data && chown appuser:appuser /data
USER appuser
COPY --from=builder /bin/worker /bin/worker
COPY --from=builder /bin/lightchain-worker /bin/lightchain-worker
ENTRYPOINT ["/bin/worker"]
