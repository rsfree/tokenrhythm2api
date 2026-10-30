# syntax=docker/dockerfile:1
FROM golang:1.25-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/trpool ./cmd/trpool

FROM alpine:3.20
RUN adduser -D -u 10001 -h /data trpool && mkdir -p /data/data && chown -R trpool /data
COPY --from=build /out/trpool /usr/local/bin/trpool
USER trpool
WORKDIR /data
ENV TRPOOL_LISTEN=:8787 TRPOOL_DATA_DIR=/data/data
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD wget -qO- http://127.0.0.1:8787/healthz || exit 1
ENTRYPOINT ["trpool"]
