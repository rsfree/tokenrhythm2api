#!/usr/bin/env bash
# trpool · ai-prod 域名入口一键落地（幂等；DNS 未就绪会拒绝执行）
#
# 用法（在 ai-prod 上跑，root）：
#   bash /opt/trpool/nginx/apply-https.sh            # 默认 trpool.1task.cn
#
# 它做什么：DNS 预检 → ACME 通道自检 → acme.sh 签发（HTTP-01 webroot）→ install-cert
#          （含续期 reloadcmd）→ 投 :443 vhost → nginx -t + reload → 打印验证矩阵 + 终态断言
# 前置：:80 的 vhost 已投好（本脚本第 2 步会自检挑战路径可达）。
set -euo pipefail

HOST="${HOST:-trpool.1task.cn}"
IP_EXPECT="${IP_EXPECT:-210.121.44.245}"
VHOST_DIR=/www/server/panel/vhost/nginx
CERT_DIR="/www/server/panel/vhost/cert/$HOST"
ACME=/root/.acme.sh/acme.sh
NGINX=/www/server/nginx/sbin/nginx
SVC_DIR=/opt/trpool

echo "== 1) DNS 预检：$HOST 必须已指向 $IP_EXPECT =="
# 预检走**多解析器**：权威 NS 最准（无缓存），公共解析器兜底（阿里云解析器有自己的缓存窗口）
GOT=""
for R in dns13.hichina.com dns14.hichina.com 8.8.8.8 1.1.1.1 223.5.5.5; do
  GOT=$(dig +short "$HOST" @"$R" 2>/dev/null | grep -E '^[0-9]+\.' | head -1 || true)
  [ "$GOT" = "$IP_EXPECT" ] && break
done
if [ "$GOT" != "$IP_EXPECT" ]; then
  echo "❌ DNS 未就绪：$HOST -> '${GOT:-<空>}'（期望 $IP_EXPECT）—— 先在万网加 A 记录，再跑本脚本"
  exit 2
fi
echo "✅ $HOST -> $GOT"

echo "== 2) ACME 通道自检（挑战文件名必须能经本 vhost 取回）=="
mkdir -p /www/wwwroot/acme/.well-known/acme-challenge
echo "preflight-$$" > /www/wwwroot/acme/.well-known/acme-challenge/preflight
CODE=$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $HOST" http://127.0.0.1/.well-known/acme-challenge/preflight)
[ "$CODE" = "200" ] || { echo "❌ 挑战路径返回 $CODE（应 200）—— 检查 $VHOST_DIR/$HOST.conf 的 ^~ location"; exit 3; }
echo "✅ 挑战路径 200"

echo "== 3) 签发证书（已存在则跳过）=="
if [ -d "/root/.acme.sh/${HOST}_ecc" ]; then
  echo "· 证书已存在，跳过签发"
else
  "$ACME" --issue -d "$HOST" --webroot /www/wwwroot/acme --keylength ec-256
fi
mkdir -p "$CERT_DIR"
"$ACME" --install-cert -d "$HOST" --ecc \
  --key-file       "$CERT_DIR/privkey.pem" \
  --fullchain-file "$CERT_DIR/fullchain.pem" \
  --reloadcmd      "$NGINX -s reload"
echo "✅ 证书就位：$CERT_DIR"

echo "== 4) 投 :443 vhost（含 -proxy.inc）并 reload（幂等）=="
SSL_CONF="$VHOST_DIR/$HOST-ssl.conf"
# 🔴 反代片段必须与 vhost 同目录：-ssl.conf 里 include 的是 **vhost 目录**下的 .inc
#    （漏了它 nginx -t 会报 "No such file or directory"，:443 全 000）
INC_CONF="$VHOST_DIR/trpool-proxy.inc"
[ -f "$INC_CONF" ] || cp -f "$SVC_DIR/nginx/trpool-proxy.inc" "$INC_CONF"
if [ ! -f "$SSL_CONF" ]; then
  SRC="$SVC_DIR/nginx/$HOST-ssl.conf"
  [ -f "$SRC" ] || SRC="/tmp/trpool-ingress/$HOST-ssl.conf"
  sed "s/trpool\.1task\.cn/$HOST/g" "$SRC" > "$SSL_CONF"
  echo "· 已写入 $SSL_CONF"
else
  echo "· $SSL_CONF 已存在，跳过"
fi
# ⚠️ `nginx -t && nginx -s reload` 写成一行是**错的**：set -e 对 AND-OR 列表首命令不生效
#    ⇒ `-t` 失败会被吞掉，脚本继续打印"✅ 完成"。必须分两行。
"$NGINX" -t
"$NGINX" -s reload
echo "✅ nginx reloaded"

echo "== 5) 验证矩阵 =="
R="--resolve $HOST:443:127.0.0.1"
code() { curl -s -o /dev/null -w '%{http_code}' $R "$@"; }
# 🔴 reload 是**异步**的：老 worker 会继续服务片刻 ⇒ 立刻断言会拿到旧配置（本仓踩过一次：
#    新 vhost 明明已加载，却断言到 404）。这里先等到 :443 真的应答，再跑矩阵。
for _ in $(seq 1 20); do
  [ "$(curl -s -o /dev/null -w '%{http_code}' $R https://$HOST/healthz)" = "200" ] && break
  sleep 0.5
done
printf '%-32s %s\n' ":80 → :443 301"        "$(curl -s -o /dev/null -w '%{http_code}' -H "Host: $HOST" http://127.0.0.1/healthz)"
# ⚠️ :443 走 **SNI 选路**：必须 --resolve（带 SNI）才能命中本 vhost；
#    只用 -H "Host: <host>" 访问 IP 会落到 default_server，看着像 404（曾踩）。
printf '%-32s %s\n' ":443 /healthz（SNI）"   "$(code https://$HOST/healthz)"
printf '%-32s %s\n' ":443 /healthz body"     "$(curl -s $R https://$HOST/healthz)"
printf '%-32s %s\n' ":443 /v1/models 无密钥" "$(code https://$HOST/v1/models)"
printf '%-32s %s\n' ":443 /panel 无令牌"     "$(code https://$HOST/panel/api/state)"
printf '%-32s %s\n' ":443 / 无令牌"          "$(code https://$HOST/)"
printf '%-32s %s\n' "证书链校验（不加 -k）"   "$(curl -s -o /dev/null -w '%{http_code} ssl_verify=%{ssl_verify_result}' $R https://$HOST/healthz)"
printf '%-32s %s\n' "未登记 Host 应 404"     "$(curl -s -o /dev/null -w '%{http_code}' -H 'Host: nope.example.com' http://127.0.0.1/healthz)"
echo
echo "证书：$(openssl x509 -in "$CERT_DIR/fullchain.pem" -noout -subject -dates 2>/dev/null | tr '\n' ' ')"
# 🔴 终态断言：只打印矩阵不判定 = "看着像成功"的假绿灯。
HZ=$(code https://$HOST/healthz)
SV=$(curl -s -o /dev/null -w '%{ssl_verify_result}' $R https://$HOST/healthz)
MODELS=$(code https://$HOST/v1/models)
[ "$HZ" = "200" ]     || { echo "❌ :443 /healthz 返回 $HZ（应 200）"; exit 4; }
[ "$SV" = "0" ]       || { echo "❌ 证书链校验失败 ssl_verify=$SV"; exit 5; }
[ "$MODELS" = "401" ] || { echo "❌ /v1/models 无密钥应 401，实际 $MODELS（鉴权可能没生效）"; exit 6; }
echo "✅ 完成：https://$HOST"
