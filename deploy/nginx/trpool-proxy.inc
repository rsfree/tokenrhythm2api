# trpool 反代（回环；鉴权在服务侧 Bearer，入口层不做认证）
proxy_pass http://127.0.0.1:39310;
proxy_http_version 1.1;
proxy_set_header Host              $host;
proxy_set_header X-Real-IP         $remote_addr;
proxy_set_header X-Forwarded-For   $proxy_add_x_forwarded_for;
proxy_set_header X-Forwarded-Proto $scheme;
proxy_set_header Connection        "";      # 回源 keepalive；不要用 $connection_upgrade（需 http 块 map）
proxy_buffering off;          # SSE 逐行透传：缓冲一次就能把流式变成"憋一大坨"
proxy_cache off;
proxy_read_timeout 660s;      # video_timeout 600s + retry_reserve 30s + 余量
proxy_send_timeout 660s;
client_max_body_size 32m;     # 多为 JSON；多模态请求里可能夹 base64 图
