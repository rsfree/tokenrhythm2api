# 部署（服务器 Docker）

一个 Go 单二进制 + 一个容器，没有数据库、没有外置缓存。下面以 Ubuntu 22.04 + Docker Compose
为例，路径用 `/opt/trpool`（生产就是这个路径，改别的路径也能跑）。

## 0. 出口网络：先确认这一条

这是**唯一一个部署前必须验证**的前置条件。容器起来、面板能开、`/healthz` 返回 200，
但只要出站到上游不通，任何业务请求都是 `502 unknown_upstream_error`。

```bash
docker run --rm curlimages/curl -sS -o /dev/null -w "%{http_code}\n" \
  -m 10 https://tokenrhythm.studio/v1/models
```

| 结果 | 含义 |
|---|---|
| `401` / `200` | 通。401 是正常的：没带鉴权，但 TCP + TLS + HTTP 全程打通 |
| 超时 / `000` | 出站被拦：安全组、企业出口白名单、DNS 污染，逐个查 |
| `x509: certificate signed by unknown authority` | 容器缺 CA 根证书。本项目镜像已装 `ca-certificates`，自建镜像要补 |

上游可达性不依赖客户端 IP，但**同一个出口 IP 挂太多号更容易被判号池**，这一点代码层解决不了。

## 1. 环境要求

| 项 | 要求 | 说明 |
|---|---|---|
| Docker | 24+，带 compose v2 | `docker compose version` |
| 架构 | `linux/amd64` | 当前只构建 amd64，arm 机器需自行 `docker build` |
| 磁盘 | 每 10 万请求约 60 MB | `data/requests/*.jsonl` 一天一个文件，按天滚 |
| 内存 | 512 MB 起 | 流式为逐行转发，不缓存整包 |
| 时区 | 容器内 `TZ=Asia/Shanghai` | 日账跨零点按这个切日 |

## 2. 取镜像（三种方式，任选其一）

```bash
# A) 从 GHCR 拉（推荐）
docker pull ghcr.io/rsfree/tokenrhythm2api:0.0.28
# tag 与 git tag 同版本但不带 v；另有 latest 与 sha-<7位>，三者 digest 相同

# B) 离线包（CI 每次发版会产出 7 天有效的 artifact，也可自行导出）
docker save ghcr.io/rsfree/tokenrhythm2api:0.0.28 | gzip > trpool-0.0.28.tar.gz
docker load < trpool-0.0.28.tar.gz

# C) 本机构建（改过代码时）
docker build --build-arg VERSION=0.0.28 -t ghcr.io/rsfree/tokenrhythm2api:0.0.28 .
```

## 3. 目录与 `.env`

```bash
mkdir -p /opt/trpool/{conf,data}
cd /opt/trpool

curl -fsSLo docker-compose.yml \
  https://raw.githubusercontent.com/rsfree/tokenrhythm2api/main/deploy/docker-compose.prod.yml

# .env 手写即可（键很少，比下模板再改省事），内容见下
```

生产在用的 compose 就是 [`deploy/docker-compose.prod.yml`](deploy/docker-compose.prod.yml)，
与仓库根目录那份 `docker-compose.yml` 的区别是：根目录那份给本机开发用（`build: .`、
端口 `8787:8787`），生产这份拉已发布镜像、只绑回环、挂 `conf/` 目录。

`.env` 最小内容：

```bash
TRPOOL_ADMIN_KEY=$(openssl rand -hex 32)
TRPOOL_CLIENT_KEYS=sk-trpool-$(openssl rand -hex 12)
TRPOOL_IMAGE_TAG=0.0.28
TRPOOL_UPSTREAM=https://tokenrhythm.studio
```

`TRPOOL_IMAGE_TAG` 是**必填**（compose 里写的是 `${TRPOOL_IMAGE_TAG:?}`）。老版本给过默认值
`:-0.0.6`，结果是：这个键被误删时会静默拉起三个月前的镜像，日志里看不出任何异常。

## 4. 启动

```bash
cd /opt/trpool
docker compose up -d
docker compose ps                     # STATUS 里应看到 (healthy)
docker compose logs -f --tail=50
```

启动日志里两行是正常的：

```
状态已恢复：N 个号，M 天账本，K 条历史日志（格式 v2）
trpool v0.0.28 启动：listen=:8787 upstream=https://... 号池=N client_key=1 归档=true
```

`listen=:8787` 是**容器内**端口。compose 已把它绑到 `127.0.0.1:39310`，宿主机外访问不到。

## 5. 验证清单

```bash
# 1) 存活（无令牌：只回 ok + service，不给内部计数）
curl -s http://127.0.0.1:39310/healthz

# 2) 版本（带 admin 令牌：给计数与 version）
A=$(grep ^TRPOOL_ADMIN_KEY /opt/trpool/.env | cut -d= -f2-)
curl -s -H "Authorization: Bearer $A" http://127.0.0.1:39310/healthz

# 3) 模型列表（用 client key，不是 admin key）
K=$(grep ^TRPOOL_CLIENT_KEYS /opt/trpool/.env | cut -d= -f2- | cut -d, -f1)
curl -s -H "Authorization: Bearer $K" http://127.0.0.1:39310/v1/models | head -c 200

# 4) 真实生成（这一步才算通：能拿到 choices 就说明号、上游、记账全链路正常）
curl -s -m 60 -X POST http://127.0.0.1:39310/v1/chat/completions \
  -H "Authorization: Bearer $K" -H "Content-Type: application/json" \
  -d '{"model":"glm-5.3-flash","messages":[{"role":"user","content":"hi"}],"max_tokens":8}'

# 5) 流式（必须看到 data: [DONE]，没有就是被截断了）
curl -sN -m 60 -X POST http://127.0.0.1:39310/v1/chat/completions \
  -H "Authorization: Bearer $K" -H "Content-Type: application/json" \
  -d '{"model":"glm-5.3-flash","stream":true,"messages":[{"role":"user","content":"hi"}],"max_tokens":8}' | tail -3

# 6) 面板
curl -s -o /dev/null -w "%{http_code}\n" -H "Authorization: Bearer $A" http://127.0.0.1:39310/panel/
```

面板里要看到：号有状态、余额有数、`配置` 页的 `上游 UA` 为空（= 透传客户端 UA）。

## 6. 反向代理

生产在宿主机跑 nginx（宝塔），容器只在回环监听。配置与理由见
[`deploy/nginx/README.md`](deploy/nginx/README.md)，四个关键点：

- `proxy_read_timeout 660s`：= `video_timeout(600s) + retry_reserve(30s) + 余量`。nginx 默认 60s 会
  在网关还正常时切断长回答，客户端看到 504，而服务端日志一片干净。
- `proxy_buffering off`：SSE 必须逐行透传。
- `location ^~ /panel` 里 `access_log off`：面板支持 `?token=<admin_key>`，日志会把令牌写明文。
- ACME 挑战路径必须排在 `location /` 之前。

## 7. 升级与回滚

```bash
cd /opt/trpool
B=backup-$(date +%Y%m%d-%H%M)
mkdir -p $B && cp -a conf data $B/      # 目标目录必须先存在，否则 cp -a 多源会失败

sed -i "s/^TRPOOL_IMAGE_TAG=.*/TRPOOL_IMAGE_TAG=0.0.29/" .env
docker compose pull && docker compose up -d
sleep 5 && docker compose ps
```

回滚就是把 tag 改回上一个版本再来一次 `up -d`。**配置与数据一般不用回滚**：新版本启动时会
按当前格式读 `state.json`，向后兼容；只有在明确知道某次升级写坏了配置时，才把 `$B/conf`
拷回去。`$B/data` 只作为最后手段——回滚数据会丢那次升级之后的所有账。

没装 `gh` 时，判断 CI 是否真的发版了用：

```bash
git ls-remote --tags origin 'v0.0.29'      # 有输出 = 已打 tag（说明测试全过）
```

**"push 完很久没有 tag" 先怀疑测试挂了，不要先怀疑 Actions 停摆。** workflow 的顺序是
`Test → 打 tag → 推镜像`，`go test` 一失败就什么都不产，外在表现和"Actions 没执行"一模一样。

## 8. 排障

| 现象 | 首要怀疑 | 查法 |
|---|---|---|
| 容器 `unhealthy` | 进程没起来 / 端口没监听 | `docker compose logs --tail=50`；镜像里是 `:8787` |
| 面板白但接口通 | 前端资源没打进二进制（`go:embed`） | `curl /panel/app.js` 看是否 404 |
| 所有请求 `502 unknown_upstream_error` | 出站不通（见第 0 节） | 容器内 `wget -qO- https://<上游>/v1/models` |
| 全部 `429 insufficient_quota` | 全池欠费/冻结 | 面板号池；充值后点「恢复停用号」或重探活 |
| 部分 `403 permission_denied` | 号没开通该模型 | 只封「号 × 模型」，换号即可 |
| `504 upstream_timeout` | 单跳超单次上限 | 看 `latency_ms`；长回答建议客户端用 `stream:true` |
| 流式没有 `[DONE]` | 反代超时掐断 / 上游卡住 | 查 `proxy_read_timeout`；日志里看 `note` / `err` 是"客户端断开"还是"空闲超时" |
| 面板改策略不生效、重启后丢失 | 配置绑的是单文件而非目录（tmp+rename → EBUSY） | `volumes` 必须是 `./conf:/data/conf` |
| 号反复被探活"救活"却仍欠费 | 探活没配模型，只打了免费的 `/v1/models` | 面板「探活」里配 `model`，免费端点欠费号也返 200 |

日志里一次请求一行，两套视图共存：

```
[req] id=ef292e4b6166 key=key-3 model=glm-5.1 /v1/chat/completions 429 2ms attempt=1
      canon=insufficient_quota/tr.quota.balance raw=402 INSUFFICIENT_BALANCE err=...
```

`canon=` 后面的第一个值是**对外**给客户端的 code，`rule=` 是命中的判定规则（出现 `fallback`
说明还有未覆盖的上游错误形态，需要补规则表），`raw=` 是上游原话——只进日志，不出网关。

## 9. 安全清单

- [ ] `.env` 权限 `600`，且**不入库**（`.gitignore` 已排除）
- [ ] 容器端口只绑 `127.0.0.1`，公网入口只走 nginx + HTTPS
- [ ] 面板关访问日志（令牌会以 `?token=` 形式出现在 URL 里）
- [ ] `TRPOOL_ADMIN_KEY` 与 `TRPOOL_CLIENT_KEYS` 不同值；client key 按客户分发，便于单独吊销
- [ ] 定期 `docker compose pull` 前先备份 `conf/` 与 `data/`
- [ ] 备份目录 `backup-*` 会一直堆积，按需清理旧的（**别删 `conf`/`data` 本体**）
