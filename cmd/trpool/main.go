// Command trpool 是面向 tokenrhythm 的多账号号池网关。
//
// 一句话：客户端只认一个 key，池子替你选号、换号、冷却、记账。
//
// tokenrhythm 本身就是 OpenAI/Anthropic 兼容的纯 API，所以这里没有
// workbuddy2api 那套逆向层（OAuth 设备码、客户端指纹、协议桥接、行为上报）：
// 只有一条"鉴权 → 选号 → 透传 → 换号 → 记账"的主干。
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"trpool/internal/config"
	"trpool/internal/livecfg"
	"trpool/internal/panel"
	"trpool/internal/pool"
	"trpool/internal/reqlog"
	"trpool/internal/scheduler"
	"trpool/internal/server"
	"trpool/internal/session"
	"trpool/internal/upstream"
	"trpool/internal/usage"
)

// version 由发布流水线通过 -ldflags "-X main.version=vX.Y.Z" 注入。
var version = "dev"

func main() {
	cfgPath := flag.String("config", "config.json", "配置文件路径")
	listen := flag.String("listen", "", "覆盖监听地址，如 :8787")
	dataDir := flag.String("data", "", "覆盖数据目录")
	showVersion := flag.Bool("version", false, "打印版本后退出")
	flag.Parse()

	if *showVersion {
		log.Printf("trpool %s", version)
		return
	}

	cfg, existed, err := config.Load(*cfgPath)
	if !existed && *cfgPath != "" {
		// 首启自动生成配置：随机 admin/client key，写盘一次并打印。
		// 之后可用环境变量覆盖（TRPOOL_ADMIN_KEY / TRPOOL_CLIENT_KEYS），
		// 面板改调度参数时只替换 pool 段，不会动这两个值。
		if cfg.AdminKey == "" {
			cfg.AdminKey = "sk-trpadmin-" + randomHex(12)
		}
		if len(cfg.ClientKeys) == 0 {
			cfg.ClientKeys = []string{"sk-trpool-" + randomHex(16)}
		}
		if werr := config.WriteDefault(*cfgPath, cfg); werr == nil {
			log.Printf("已生成默认配置 %s（含随机密钥，权限 600）", *cfgPath)
			log.Printf("  admin_key  = %s", cfg.AdminKey)
			log.Printf("  client_key = %s", cfg.ClientKeys[0])
		}
		err = cfg.Validate()
	}
	if err != nil {
		log.Fatalf("配置错误: %v", err)
	}
	if *listen != "" {
		cfg.Listen = *listen
	}
	if *dataDir != "" {
		cfg.DataDir = *dataDir
	}
	if err := scheduler.EnsureDir(cfg.DataDir); err != nil {
		log.Fatalf("创建数据目录失败: %v", err)
	}

	// ---- 组件装配（顺序即依赖方向：usage/upstream ← pool ← server ← panel）----
	live := livecfg.New(livecfg.Snapshot{
		AdminKey:         cfg.AdminKey,
		ClientKeys:       cfg.ClientKeys,
		AllowPassthrough: cfg.AllowUpstreamKeyPassthrough,
		LogBodies:        cfg.Logging.LogBodies,
		ProbeModel:       cfg.AutoProbe.Model,
		ProbeMaxTokens:   cfg.AutoProbe.MaxTokens,
	})
	prices := usage.NewPrices()
	daily := usage.NewDaily()
	sess := session.New(cfg.Pool.SessionSticky.TTL.Duration(), cfg.Pool.SessionSticky.MaxEntries)
	up := upstream.New(cfg.Upstream)
	p := pool.New(cfg.Pool, sess)

	archiveDir := cfg.Logging.ArchiveDir
	if archiveDir == "" {
		archiveDir = filepath.Join(cfg.DataDir, "requests")
	}
	logs := reqlog.New(reqlog.Config{
		RingSize:   cfg.Logging.RingSize,
		ArchiveDir: archiveDir,
		Archive:    cfg.Logging.ArchiveOn(),
		RetainDays: cfg.Logging.RetentionDays,
		MaxMB:      cfg.Logging.MaxMB,
	})

	sch := scheduler.New(scheduler.Deps{
		Cfg: &cfg, Pool: p, Up: up, Prices: prices, Log: logs, Daily: daily,
		StatePath: scheduler.DefaultStatePath(cfg.DataDir),
		Live:      live,
	})
	if err := sch.Load(); err != nil {
		log.Printf("WARN: %v", err)
	}
	scheduler.SeedFromEnv(p)
	p.SetOnChange(sch.MarkDirty)
	logs.OnRecord = sch.MarkDirty

	srv := server.New(server.Deps{
		Cfg: &cfg, Live: live, Pool: p, Up: up, Prices: prices, Log: logs, Daily: daily,
		Version: version,
	})
	pan := panel.New(panel.Deps{
		Cfg: &cfg, Live: live, Pool: p, Prices: prices, Log: logs, Daily: daily, Up: up,
		Version: version, ConfigPath: *cfgPath,
		Stats:   srv.Stats().Snapshot,
		Uptime:  srv.Uptime,
		Persist: sch.MarkDirty,
	})
	srv.MountPanel(pan)

	httpSrv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           srv,
		ReadHeaderTimeout: 15 * time.Second,
		ReadTimeout:       cfg.Server.ReadTimeout.Duration(),
		IdleTimeout:       120 * time.Second,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go sch.Run(ctx)

	go func() {
		log.Printf("trpool %s 启动：listen=%s upstream=%s 号池=%d client_key=%d 归档=%v",
			version, cfg.Listen, cfg.Upstream, len(p.Views()), len(cfg.ClientKeys), cfg.Logging.ArchiveOn())
		if err := httpSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP 服务失败: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("收到退出信号，正在保存状态…")
	shutCtx, cancel := context.WithTimeout(context.Background(), cfg.Server.ShutdownTimeout.Duration())
	defer cancel()
	_ = httpSrv.Shutdown(shutCtx)
	sch.Save()
	logs.Close()
	log.Println("已退出")
}

// randomHex 生成 n 字节随机数的 hex 串（n=12 得 24 位）。
func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		// 极端情况下退回时间戳，至少不要生成空密钥。
		ts := time.Now().UnixNano()
		for i := range b {
			b[i] = byte(ts >> (uint(i%8) * 8))
		}
	}
	return hex.EncodeToString(b)
}
