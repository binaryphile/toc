package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/binaryphile/toc/internal/adapter"
	"github.com/binaryphile/toc/internal/adapter/restsource"
	"github.com/binaryphile/toc/natstransport"
	"github.com/nats-io/nats.go"
)

func main() {
	configPath := flag.String("config", "toc-adapter.yaml", "config file path")
	flag.Parse()

	logger := slog.Default()

	cfg, err := adapter.LoadConfig(*configPath)
	if err != nil {
		logger.Error("load config", "error", err)
		os.Exit(1)
	}

	nc, err := nats.Connect(cfg.NATS.URL)
	if err != nil {
		logger.Error("connect to NATS", "url", cfg.NATS.URL, "error", err)
		os.Exit(1)
	}
	defer nc.Close()

	transport, err := natstransport.New(nc, natstransport.WithPrefix(cfg.NATS.Prefix))
	if err != nil {
		logger.Error("create transport", "error", err)
		os.Exit(1)
	}

	sources := make([]adapter.NamedSource, 0, len(cfg.Sources))
	for _, srcCfg := range cfg.Sources {
		switch srcCfg.Type {
		case "rest":
			src, err := restsource.NewRESTSource(srcCfg, logger)
			if err != nil {
				logger.Error("create REST source", "url", srcCfg.URL, "error", err)
				os.Exit(1)
			}
			sources = append(sources, adapter.NamedSource{
				Source: src,
				Type:   srcCfg.Type,
				URL:    srcCfg.URL,
			})
		default:
			logger.Error("unknown source type", "type", srcCfg.Type)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	logger.Info("toc-adapter starting",
		"pipeline", cfg.PipelineID,
		"sources", len(sources),
		"poll_interval", cfg.PollInterval,
	)

	if err := adapter.Run(ctx, cfg, sources, transport, logger); err != nil {
		logger.Error("run", "error", err)
		os.Exit(1)
	}

	logger.Info("toc-adapter stopped")
}
