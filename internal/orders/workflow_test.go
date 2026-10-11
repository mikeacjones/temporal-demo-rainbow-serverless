package orders

import (
	"context"
	"sync"
	"testing"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/converter"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
)

func orderFn(v Version) any {
	order, _ := bundleFor(v)
	return order
}

func gateFn(v Version) any {
	_, gate := bundleFor(v)
	return gate
}

func newEnv(t *testing.T, v Version, gate bool) *testsuite.TestWorkflowEnvironment {
	t.Helper()

	var suite testsuite.WorkflowTestSuite
	env := suite.NewTestWorkflowEnvironment()

	acts := &Activities{Profile: ProfileFast}
	for _, fn := range activitiesFor(acts, v) {
		env.RegisterActivity(fn)
	}

	fn := orderFn(v)
	if gate {
		fn = gateFn(v)
	}
	env.RegisterWorkflowWithOptions(fn, workflow.RegisterOptions{Name: WorkflowTypeName})
	return env
}

func TestOrderRunsItsVersionsPipeline(t *testing.T) {
	for _, v := range AllVersions {
		t.Run(string(v), func(t *testing.T) {
			env := newEnv(t, v, false)
			env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

			if !env.IsWorkflowCompleted() {
				t.Fatal("workflow did not complete")
			}
			if err := env.GetWorkflowError(); err != nil {
				t.Fatalf("workflow failed: %v", err)
			}

			var result OrderResult
			if err := env.GetWorkflowResult(&result); err != nil {
				t.Fatalf("decode result: %v", err)
			}
			if result.Version != v {
				t.Errorf("result version = %q, want %q", result.Version, v)
			}

			want := StepsFor(v)
			if len(result.Steps) != len(want) {
				t.Fatalf("ran %d steps, want %d", len(result.Steps), len(want))
			}
			for i := range want {
				if result.Steps[i] != want[i] {
					t.Errorf("step %d = %q, want %q", i, result.Steps[i], want[i])
				}
			}
		})
	}
}

func TestOrderIgnoresChaosAimedElsewhere(t *testing.T) {
	env := newEnv(t, V2, false)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V5,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("v2 order should be unaffected by a v5 fault, got: %v", err)
	}
}

func TestOrderIgnoresChaosOnAStepItDoesNotHave(t *testing.T) {
	env := newEnv(t, V1, false)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V1,
		Step:          StepLoyalty,
		Mode:          ChaosFail,
	}))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("v1 order should be unaffected by a fault on a step it lacks, got: %v", err)
	}
}

func TestGateFailsOnPoisonedVersion(t *testing.T) {
	env := newEnv(t, V4, true)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V4,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if !env.IsWorkflowCompleted() {
		t.Fatal("gate should finish rather than hang")
	}
	if env.GetWorkflowError() == nil {
		t.Fatal("gate should fail when its version is poisoned")
	}
}

func TestGatePassesOnHealthyVersion(t *testing.T) {
	env := newEnv(t, V4, true)
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("gate should pass on a healthy version, got: %v", err)
	}
}

func TestEachStepRunsAsItsOwnActivityType(t *testing.T) {
	var mu sync.Mutex
	seen := map[string]int{}

	env := newEnv(t, V4, false)
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		mu.Lock()
		defer mu.Unlock()
		seen[info.ActivityType.Name]++
	})
	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, nil))

	if err := env.GetWorkflowError(); err != nil {
		t.Fatalf("order failed: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()

	want := StepsFor(V4)
	if len(seen) != len(want) {
		t.Fatalf("saw %d distinct Activity types (%v), want %d", len(seen), seen, len(want))
	}
	for _, step := range want {
		if seen[step.ActivityName()] != 1 {
			t.Errorf("Activity %q ran %d times, want 1", step.ActivityName(), seen[step.ActivityName()])
		}
	}
	if seen["PerformStep"] != 0 {
		t.Error("shared PerformStep Activity type should not appear")
	}
}

func TestVersionsRunDifferentActivityTypes(t *testing.T) {
	names := func(v Version) map[string]bool {
		out := map[string]bool{}
		for _, s := range StepsFor(v) {
			out[s.ActivityName()] = true
		}
		return out
	}

	v3, v4 := names(V3), names(V4)
	if !v4["DispatchMobilePickup"] {
		t.Error("v4 should run a DispatchMobilePickup Activity")
	}
	if v3["DispatchMobilePickup"] {
		t.Error("v3 should not have that Activity — it is what v4 adds")
	}
	if !names(V5)["HandOffAtDriveThru"] || names(V1)["HandOffAtDriveThru"] {
		t.Error("the drive-thru handoff should be unique to v5")
	}

	all := map[string]Step{}
	for _, v := range AllVersions {
		for _, s := range StepsFor(v) {
			if prior, ok := all[s.ActivityName()]; ok && prior != s {
				t.Errorf("steps %q and %q share the Activity name %q", prior, s, s.ActivityName())
			}
			all[s.ActivityName()] = s
		}
	}
}

func TestFaultedOrderKeepsRetrying(t *testing.T) {
	var mu sync.Mutex
	attempts := 0

	env := newEnv(t, V2, false)
	env.SetOnActivityStartedListener(func(info *activity.Info, _ context.Context, _ converter.EncodedValues) {
		if info.ActivityType.Name == StepPayment.ActivityName() {
			mu.Lock()
			attempts++
			mu.Unlock()
		}
	})

	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V2,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if env.IsWorkflowCompleted() {
		if err := env.GetWorkflowError(); err == nil {
			t.Fatal("a faulted order should not complete successfully")
		}
	}

	mu.Lock()
	defer mu.Unlock()
	if attempts <= gateAttempts {
		t.Errorf("payment ran %d times, want more than the gate's %d attempts — "+
			"a real order should keep retrying until Rescue", attempts, gateAttempts)
	}
}

func TestAnUnclearedFaultNeverFailsTheOrder(t *testing.T) {
	env := newEnv(t, V2, false)

	env.ExecuteWorkflow(WorkflowTypeName, NewOrderInput(1, &ChaosSpec{
		TargetVersion: V2,
		Step:          StepPayment,
		Mode:          ChaosFail,
	}))

	if !env.IsWorkflowCompleted() {
		return // still retrying — intended
	}
	if err := env.GetWorkflowError(); err == nil {
		t.Fatal("an order should not complete successfully while its fault is live")
	}
}
