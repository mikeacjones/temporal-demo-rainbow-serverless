package orders

import "go.temporal.io/sdk/workflow"

// v5 code bundle — restructured handoff (drive-thru); drops fraud + mobile.

func OrderV5(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV5(ctx, in, true)
}

func GateV5(ctx workflow.Context, in OrderInput) (OrderResult, error) {
	return runV5(ctx, in, false)
}

func runV5(ctx workflow.Context, in OrderInput, forever bool) (OrderResult, error) {
	state, err := setup(ctx, V5, in)
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
	if err := workflow.ExecuteActivity(ctx, (*Activities).AccrueLoyalty, stepPayload(ctx, state, in, StepLoyalty)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).PrepareOrder, stepPayload(ctx, state, in, StepPrep)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	if err := workflow.ExecuteActivity(ctx, (*Activities).HandOffAtDriveThru, stepPayload(ctx, state, in, StepDriveThru)).Get(ctx, nil); err != nil {
		return OrderResult{}, err
	}
	return complete(state), nil
}
