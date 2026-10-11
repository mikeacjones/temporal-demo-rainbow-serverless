package orders

import "go.temporal.io/sdk/workflow"

// v1 code bundle — Received → Payment → Prep → Handoff.
//
// In production this file would be its own commit / Lambda image. Temporal
// Worker Deployment routes CustomerOrder to the Build ID that registered it.
// This repo keeps five bundles so every version can run at once for the demo.

func OrderV1(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV1(ctx, in, true)
}

func GateV1(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV1(ctx, in, false)
}

func runV1(ctx workflow.Context, in OrderInput, forever bool) (OrderResult, error) {
	state, err := setup(ctx, V1, in)
	if err != nil {
		return OrderResult{}, err
	}
	ctx = withActivities(ctx, forever)

	if err := workflow.ExecuteActivity(ctx, (*Activities).ReceiveOrder, stepPayload(ctx, state, in, StepReceived)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).ChargePayment, stepPayload(ctx, state, in, StepPayment)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).PrepareOrder, stepPayload(ctx, state, in, StepPrep)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).HandOffToCustomer, stepPayload(ctx, state, in, StepHandoff)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	return complete(state), nil
}
