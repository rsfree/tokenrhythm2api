# 反代配置（生产实际在用的那一份）

这几个文件是从生产机 `/opt/trpool/nginx/` **原样复制**进仓库的。它们不属于容器镜像，
属于宿主机 nginx，但如果只留在服务器上，重装/换机就会丢掉下面这些"改一次踩一次"的设置。

## 文件与落点

| 仓库文件 | 生产路径 | 作用 |
|---|---|---|
| `trpool-proxy.inc` | `/www/server/panel/vhost/nginx/trpool-proxy.inc` | 回环反代公共片段，三个 location 都 include 它 |
| `trpool.1task.cn.conf` | `/www/server/panel/vhost/nginx/trpool.1task.cn.conf` | `:80` 入口，除 ACME 外一律 301 到 HTTPS |
| `trpool.1task.cn-ssl.conf` | `/www/server/panel/vhost/nginx/trpool.1task.cn-ssl.conf` | `:443` 入口，证书走宝塔 `/www/server/panel/vhost/cert/` |
| `apply-https.sh` | `/opt/trpool/nginx/apply-https.sh` | 一键落地：DNS 预检 → ACME → 签发 → 投 vhost → reload |

`/opt/trpool/nginx/` 是这几个文件的**暂存区**，改完要 `install` 到宝塔的 vhost 目录再 reload。

## 四个不能改错的设置

**1. `proxy_read_timeout 660s`**

不是随手填的：`video_timeout(600s) + retry_reserve(30s) + 余量`。
nginx 默认 60s，视频任务或长回答会在**网关还在正常工作时**被反代切断，客户端看到 504
而服务端日志里一条错误都没有——这类故障查起来极容易跑偏。

改了 `video_timeout` / `retry_reserve` 就必须同步改这里。

**2. `proxy_buffering off`**

SSE 逐行透传。开缓冲会把流式变成"憋一大坨再吐"，首字节延迟直接等于整段生成时间。

**3. `location ^~ /panel` 里的 `access_log off`**

面板支持 `?token=<admin_key>` 取用，一旦开访问日志，管理令牌会以明文写进
`/www/wwwlogs/*.log`。这条 `access_log off` 是安全设置，不是性能设置。

**4. ACME 的 `location ^~ /.well-known/acme-challenge/` 必须排在 `location /` 之前**

`^~` 前缀匹配优先级高于正则与普通前缀。顺序错了会被 catch-all 的 301 吃掉，
续期失败——而证书到期前 30 天才有告警，容易发现得太晚。

## 改了之后

```bash
# 1) 改暂存区
vi /opt/trpool/nginx/trpool-proxy.inc
# 2) 同步到 vhost 目录（宝塔面板的配置以这个目录为准）
install -m 644 /opt/trpool/nginx/trpool-proxy.inc /www/server/panel/vhost/nginx/
# 3) 必须 -t，再 reload
nginx -t && nginx -s reload
# 4) 回写仓库
scp ai-prod:/opt/trpool/nginx/trpool-proxy.inc deploy/nginx/
```

第 4 步别省。服务器上的配置不回流仓库，下次换机就是一次故障复盘。
