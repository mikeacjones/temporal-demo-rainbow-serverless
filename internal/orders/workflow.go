package orders

import (
	"time"

	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

const QueryGetState = "getState"

const (
	HealthOK       = "ok"
	HealthDegraded = "degraded"
)

var (
	saOrderVersion = temporal.NewSearchAttributeKeyKeyword("OrderVersion")
	saOrderStep    = temporal.NewSearchAttributeKeyKeyword("OrderStep")
	saOrderHealth  = temporal.NewSearchAttributeKeyKeyword("OrderHealth")
)

const (
	stepStartToClose = 30 * time.Second
	// gateAttempts: canary fails fast. Real orders use unlimited Activity retries.
	gateAttempts = 3
)

func setup(ctx workflow.Context, v Version, in OrderInput) (*OrderState, error) {
	state := &OrderState{OrderID: in.OrderID, Version: v}
	if err := workflow.SetQueryHandler(ctx, QueryGetState, func() (OrderState, error) {
		return *state, nil
	}); err != nil {
		return nil, err
	}
	_ = workflow.UpsertTypedSearchAttributes(ctx,
		saOrderVersion.ValueSet(string(v)),
		saOrderHealth.ValueSet(HealthOK),
	)
	return state, nil
}

func complete(state *OrderState) OrderResult {
	state.Done = true
	return OrderResult{OrderID: state.OrderID, Version: state.Version, Steps: state.Steps}
}

// withActivities sets StartToClose (execution only — queuing is not failure).
// forever=true → unlimited retries (stuck orders keep retrying until Rescue).
// forever=false → gateAttempts (canary fails fast).
func withActivities(ctx workflow.Context, forever bool) workflow.Context {
	maxAttempts := int32(gateAttempts)
	if forever {
		maxAttempts = 0
	}
	return workflow.WithActivityOptions(ctx, workflow.ActivityOptions{
		StartToCloseTimeout: stepStartToClose,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    2 * time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    60 * time.Second,
			MaximumAttempts:    maxAttempts,
		},
	})
}

// stepPayload builds the Activity input and records step / health search attributes.
func stepPayload(ctx workflow.Context, state *OrderState, in OrderInput, step Step) StepInput {
	state.CurrentStep = len(state.Steps)
	state.Steps = append(state.Steps, step)
	_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderStep.ValueSet(string(step)))

	faulted := in.Chaos.appliesTo(state.Version, step)
	if faulted {
		state.Degraded = true
		_ = workflow.UpsertTypedSearchAttributes(ctx, saOrderHealth.ValueSet(HealthDegraded))
	}

	return StepInput{
		OrderID: state.OrderID,
		Version: state.Version,
		Step:    step,
		Fail:    faulted && in.Chaos.Mode == ChaosFail,
		Slow:    faulted && in.Chaos.Mode == ChaosSlow,
	}
}
