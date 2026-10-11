package orders

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"time"

	"go.temporal.io/sdk/activity"
)

// Profile is an end-to-end work budget shared across a version's steps so every
// pipeline takes about the same wall time (shape changes, not latency).
type Profile string

const (
	ProfileFast  Profile = "fast"  // ~1s — high rate on a laptop
	ProfileDemo  Profile = "demo"  // ~8s — presentation default
	ProfileHeavy Profile = "heavy" // ~40s — saturates workers
)

func (p Profile) orderWork() time.Duration {
	switch p {
	case ProfileFast:
		return time.Second
	case ProfileHeavy:
		return 40 * time.Second
	default:
		return 8 * time.Second
	}
}

func ParseProfile(s string) (Profile, bool) {
	switch Profile(s) {
	case ProfileFast, ProfileDemo, ProfileHeavy:
		return Profile(s), true
	default:
		return "", false
	}
}

// Absolute so it exceeds stepStartToClose in every profile.
const slowStepWork = 45 * time.Second

var ErrStepFailed = errors.New("step failed")

// Activities holds worker-local settings (speed profile). Each method is a
// real Activity type registered on the worker.
type Activities struct {
	Profile Profile
}

func (a *Activities) ReceiveOrder(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) ScreenForFraud(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) ChargePayment(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) AccrueLoyalty(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) PrepareOrder(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) DispatchMobilePickup(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) HandOffAtDriveThru(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) HandOffToCustomer(ctx context.Context, in StepInput) error {
	return a.perform(ctx, in)
}

func (a *Activities) perform(ctx context.Context, in StepInput) error {
	logger := activity.GetLogger(ctx)
	attempt := activity.GetInfo(ctx).Attempt

	if in.Fail {
		logger.Warn("step failing (injected fault)",
			"orderId", in.OrderID, "version", in.Version, "step", in.Step, "attempt", attempt)
		return fmt.Errorf("%s for order %s: %w", in.Step, in.OrderID, ErrStepFailed)
	}

	work := a.stepDuration(in.Version, in.Slow)
	logger.Info("step started",
		"orderId", in.OrderID, "version", in.Version, "step", in.Step, "work", work)

	// Sleep here (not a Workflow timer) so a worker slot stays busy.
	select {
	case <-time.After(work):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (a *Activities) stepDuration(version Version, slow bool) time.Duration {
	if slow {
		return slowStepWork
	}

	steps := len(StepsFor(version))
	if steps == 0 {
		steps = 1
	}

	share := a.Profile.orderWork() / time.Duration(steps)
	return time.Duration(float64(share) * (0.8 + 0.4*rand.Float64()))
}

// activitiesFor returns the Activity methods this version's worker registers.
func activitiesFor(a *Activities, v Version) []any {
	switch v {
	case V1:
		return []any{a.ReceiveOrder, a.ChargePayment, a.PrepareOrder, a.HandOffToCustomer}
	case V2:
		return []any{a.ReceiveOrder, a.ChargePayment, a.AccrueLoyalty, a.PrepareOrder, a.HandOffToCustomer}
	case V3:
		return []any{a.ReceiveOrder, a.ScreenForFraud, a.ChargePayment, a.AccrueLoyalty, a.PrepareOrder, a.HandOffToCustomer}
	case V4:
		return []any{a.ReceiveOrder, a.ScreenForFraud, a.ChargePayment, a.AccrueLoyalty, a.PrepareOrder, a.DispatchMobilePickup, a.HandOffToCustomer}
	case V5:
		return []any{a.ReceiveOrder, a.ChargePayment, a.AccrueLoyalty, a.PrepareOrder, a.HandOffAtDriveThru}
	default:
		return nil
	}
}
