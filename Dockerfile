FROM golang:1.25-alpine AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod ./
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath \
      -ldflags "-s -w -X main.version=${VERSION}" \
      -o /out/trpool ./cmd/trpool

FROM alpine:3.20
# 标签要显式声明：ARG 的作用域到 FROM 就断了，上面那个 VERSION 在这里看不见。
# 没有 revision 标签时，镜像无法反查是哪次提交构建的——线上出问题时只能靠
# `docker images` 的时间戳猜，而"这个 tag 到底对应哪个 commit"是排查的第一个问题。
ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="trpool" \
      org.opencontainers.image.description="TokenRhythm 号池网关：号池调度、canonical 错误契约、渠道信息不外泄" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}" \
      org.opencontainers.image.source="https://github.com/rsfree/tokenrhythm2api" \
      org.opencontainers.image.licenses="MIT"
# 必须装 ca-certificates：alpine 基础镜像不带 CA 根证书，否则访问
# https://tokenrhythm.studio 会直接 x509: certificate signed by unknown authority
# —— 容器能起来，但一个请求都发不出去。
# tzdata 是为了日账按 Asia/Shanghai 切日，不然跨零点会算到前一天。
RUN apk add --no-cache ca-certificates tzdata \
 && adduser -D -u 10001 -h /data trpool \
 && mkdir -p /data/data \
 && chown -R trpool /data

COPY --from=build /out/trpool /usr/local/bin/trpool

USER trpool
WORKDIR /data
ENV TRPOOL_LISTEN=:8787 \
    TRPOOL_DATA_DIR=/data/data \
    TZ=Asia/Shanghai
EXPOSE 8787
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s \
  CMD wget -qO- http://127.0.0.1:8787/healthz || exit 1
ENTRYPOINT ["trpool"]
