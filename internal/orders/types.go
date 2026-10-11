package orders

const (
	TaskQueue            = "orders"
	WorkflowTypeName     = "CustomerOrder"
	GateWorkflowTypeName = "OrderGate" // same queue so the candidate worker runs it
)

type ChaosMode string

const (
	ChaosFail ChaosMode = "fail" // Activity errors; Temporal retries
	ChaosSlow ChaosMode = "slow" // exceeds the step execution budget
)

// ChaosSpec is stamped into OrderInput at start (no shared mutable state, no
// Workflow randomness). Only TargetVersion reacts.
type ChaosSpec struct {
	TargetVersion Version   `json:"targetVersion"`
	Step          Step      `json:"step"`
	Mode          ChaosMode `json:"mode"`
}

func (c *ChaosSpec) appliesTo(v Version, s Step) bool {
	return c != nil && c.TargetVersion == v && c.Step == s
}

type OrderInput struct {
	OrderID string     `json:"orderId"`
	Store   string     `json:"store"`
	Items   []Item     `json:"items"`
	Chaos   *ChaosSpec `json:"chaos,omitempty"`
}

type Item struct {
	Name string `json:"name"`
	Qty  int    `json:"qty"`
}

type OrderResult struct {
	OrderID string  `json:"orderId"`
	Version Version `json:"version"`
	Steps   []Step  `json:"steps"`
}

// OrderState is served by getState (single-order drill-down / Temporal UI).
// Aggregates use search attributes instead.
type OrderState struct {
	OrderID     string  `json:"orderId"`
	Version     Version `json:"version"`
	Steps       []Step  `json:"steps"`
	CurrentStep int     `json:"currentStep"`
	Degraded    bool    `json:"degraded"`
	Done        bool    `json:"done"`
}

// StepInput is the Activity payload. Fail/Slow come from ChaosSpec on the order.
type StepInput struct {
	OrderID string  `json:"orderId"`
	Version Version `json:"version"`
	Step    Step    `json:"step"`
	Fail    bool    `json:"fail,omitempty"`
	Slow    bool    `json:"slow,omitempty"`
}
