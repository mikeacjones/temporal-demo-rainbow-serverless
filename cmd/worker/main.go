// Command worker runs one version of the customer-order worker.
// ORDER_VERSION selects the pipeline; every version runs at once so any-to-any
// rollouts are routing changes only.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

func main() {
	logger := config.Logger()
	if err := run(logger); err != nil {
		logger.Error("order worker failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.TemporalFromEnv()

	version, ok := orders.ParseVersion(config.Env("ORDER_VERSION", string(orders.V1)))
	if !ok {
		return fmt.Errorf("invalid ORDER_VERSION %q", os.Getenv("ORDER_VERSION"))
	}
	profile, ok := orders.ParseProfile(config.Env("ORDER_PROFILE", string(orders.ProfileDemo)))
	if !ok {
		return fmt.Errorf("invalid ORDER_PROFILE %q", os.Getenv("ORDER_PROFILE"))
	}

	buildID := config.Env("TEMPORAL_WORKER_BUILD_ID", string(version))

	logger = logger.With("version", version, "buildId", buildID)
	logger.Info("starting order worker",
		"address", cfg.Address,
		"namespace", cfg.Namespace,
		"deployment", cfg.DeploymentName,
		"taskQueue", orders.TaskQueue,
		"profile", profile,
	)

	c, err := client.Dial(cfg.ClientOptions(logger))
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	w := worker.New(c, orders.TaskQueue, worker.Options{
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: cfg.DeploymentName,
				BuildID:        buildID,
			},
		},
		MaxConcurrentActivityExecutionSize:     config.EnvInt("WORKER_MAX_CONCURRENT_ACTIVITIES", 20),
		MaxConcurrentWorkflowTaskExecutionSize: config.EnvInt("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", 20),
		// Keep Activity tasks on the queue so backlog / sync-match stay visible.
		DisableEagerActivities: true,
	})

	orders.Register(w, version, profile)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	go deploy.PublishLabel(ctx, c, cfg.DeploymentName, buildID, string(version), logger)

	<-ctx.Done()
	logger.Info("order worker stopped")
	return nil
}
