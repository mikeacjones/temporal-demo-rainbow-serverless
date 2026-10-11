package orders

import "go.temporal.io/sdk/workflow"

// v4 code bundle — v3 + Mobile pickup dispatch before Handoff.

func OrderV4(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV4(ctx, in, true)
}

func GateV4(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV4(ctx, in, false)
}

func runV4(ctx workflow.Context, in OrderInput, forever bool) (OrderResult, error) {
	state, err := setup(ctx, V4, in)
	if err != nil {
		return OrderResult{}, err
	}
	ctx = withActivities(ctx, forever)

	if err := workflow.ExecuteActivity(ctx, (*Activities).ReceiveOrder, stepPayload(ctx, state, in, StepReceived)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).ScreenForFraud, stepPayload(ctx, state, in, StepFraudCheck)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).ChargePayment, stepPayload(ctx, state, in, StepPayment)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).AccrueLoyalty, stepPayload(ctx, state, in, StepLoyalty)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).PrepareOrder, stepPayload(ctx, state, in, StepPrep)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).DispatchMobilePickup, stepPayload(ctx, state, in, StepMobileDispatch)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).HandOffToCustomer, stepPayload(ctx, state, in, StepHandoff)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	return complete(state), nil
}
