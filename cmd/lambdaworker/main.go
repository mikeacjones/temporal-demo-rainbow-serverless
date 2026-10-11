// Command lambdaworker hosts the same order pipeline as cmd/worker inside
// AWS Lambda as a Temporal serverless worker (one function/alias per version).
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/awssecret"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

func main() {
	version, ok := orders.ParseVersion(config.Env("ORDER_VERSION", string(orders.V1)))
	if !ok {
		log.Fatalf("invalid ORDER_VERSION %q", os.Getenv("ORDER_VERSION"))
	}
	profile, ok := orders.ParseProfile(config.Env("ORDER_PROFILE", string(orders.ProfileDemo)))
	if !ok {
		log.Fatalf("invalid ORDER_PROFILE %q", os.Getenv("ORDER_PROFILE"))
	}

	if err := awssecret.ResolveTemporalAPIKey(context.Background()); err != nil {
		log.Fatalf("cannot resolve the Temporal API key: %v", err)
	}

	deploymentName := config.Env("TEMPORAL_DEPLOYMENT_NAME", "rainbow-orders")
	buildID := config.Env("TEMPORAL_WORKER_BUILD_ID", string(version))

	deploymentVersion := worker.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildID:        buildID,
	}

	// Address / namespace / API key / TLS come from the environment via lambdaworker.
	lambdaworker.RunWorker(deploymentVersion, func(opts *lambdaworker.Options) error {
		opts.TaskQueue = orders.TaskQueue

		opts.WorkerOptions.DeploymentOptions = worker.DeploymentOptions{
			UseVersioning: true,
			Version:       deploymentVersion,
		}
		opts.WorkerOptions.DisableEagerActivities = true
		tuneWorker(&opts.WorkerOptions) // see tuning.go

		orders.Register(opts, version, profile)
		return nil
	})
}
