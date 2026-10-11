package orders

import (
	"fmt"

	"go.temporal.io/sdk/workflow"
)

// Registry is the registration surface shared by process workers and
// lambdaworker (which is not a full worker.Registry).
type Registry interface {
	RegisterWorkflowWithOptions(w any, options workflow.RegisterOptions)
	RegisterActivity(a any)
}

// Register wires this worker's code bundle as Pinned CustomerOrder / OrderGate.
//
// ORDER_VERSION selects which bundle (workflow_vN.go). The client starts
// WorkflowTypeName with no version argument — Worker Deployment routes to this
// Build ID. In production each Build ID is a separate upload; here five bundles
// live in one repo so the demo can run them side by side.
func Register(w Registry, v Version, profile Profile) {
	order, gate := bundleFor(v)

	w.RegisterWorkflowWithOptions(order, workflow.RegisterOptions{
		Name:               WorkflowTypeName,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})
	w.RegisterWorkflowWithOptions(gate, workflow.RegisterOptions{
		Name:               GateWorkflowTypeName,
		VersioningBehavior: workflow.VersioningBehaviorPinned,
	})

	activities := &Activities{Profile: profile}
	for _, fn := range activitiesFor(activities, v) {
		w.RegisterActivity(fn)
	}
}

func bundleFor(v Version) (order, gate any) {
	switch v {
	case V1:
		return OrderV1, GateV1
	case V2:
		return OrderV2, GateV2
	case V3:
		return OrderV3, GateV3
	case V4:
		return OrderV4, GateV4
	case V5:
		return OrderV5, GateV5
	default:
		return OrderV1, GateV1
	}
}

// Menu / Stores are cosmetic labels for the dashboard.
var Menu = []Item{
	{Name: "House Blend", Qty: 1},
	{Name: "Large Dark Roast", Qty: 1},
	{Name: "Chocolate Donut", Qty: 2},
	{Name: "Honey Glazed", Qty: 1},
	{Name: "Iced Latte", Qty: 1},
	{Name: "Breakfast Sandwich", Qty: 1},
	{Name: "Donut Holes 10-pack", Qty: 1},
	{Name: "Herbal Tea", Qty: 2},
	{Name: "Breakfast Wrap", Qty: 1},
	{Name: "Soup + Bread", Qty: 1},
}

var Stores = []string{
	"Toronto — Front St",
	"Toronto — Yonge & Eg",
	"Mississauga — Square One",
	"Ottawa — Bank St",
	"Calgary — 17th Ave",
	"Vancouver — Robson",
	"Halifax — Spring Garden",
	"Montreal — Sainte-Catherine",
}

func NewOrderInput(seq int, chaos *ChaosSpec) OrderInput {
	first := Menu[seq%len(Menu)]
	second := Menu[(seq*7+3)%len(Menu)]

	items := []Item{first}
	if second.Name != first.Name {
		items = append(items, second)
	}

	return OrderInput{
		OrderID: fmt.Sprintf("ord-%06d", seq),
		Store:   Stores[seq%len(Stores)],
		Items:   items,
		Chaos:   chaos,
	}
}
