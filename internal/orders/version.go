package orders

// Version is a Worker Deployment Build ID label (and which workflow_vN.go bundle
// this worker registers). All bundles share WorkflowTypeName; routing is by Build ID.
type Version string

const (
	V1 Version = "v1"
	V2 Version = "v2"
	V3 Version = "v3"
	V4 Version = "v4"
	V5 Version = "v5"
)

var AllVersions = []Version{V1, V2, V3, V4, V5}

func AllVersionLabels() []string {
	out := make([]string, 0, len(AllVersions))
	for _, v := range AllVersions {
		out = append(out, string(v))
	}
	return out
}

// Step is one stage of an order; each becomes its own Activity type.
type Step string

const (
	StepReceived       Step = "Received"
	StepFraudCheck     Step = "Fraud check"
	StepPayment        Step = "Payment"
	StepLoyalty        Step = "Loyalty accrual"
	StepPrep           Step = "Prep"
	StepMobileDispatch Step = "Mobile pickup dispatch"
	StepDriveThru      Step = "Drive-thru handoff"
	StepHandoff        Step = "Handoff"
)

// ActivityName is the Temporal Activity type for this step (method name).
func (s Step) ActivityName() string {
	switch s {
	case StepReceived:
		return "ReceiveOrder"
	case StepFraudCheck:
		return "ScreenForFraud"
	case StepPayment:
		return "ChargePayment"
	case StepLoyalty:
		return "AccrueLoyalty"
	case StepPrep:
		return "PrepareOrder"
	case StepMobileDispatch:
		return "DispatchMobilePickup"
	case StepDriveThru:
		return "HandOffAtDriveThru"
	case StepHandoff:
		return "HandOffToCustomer"
	default:
		return ""
	}
}

// StepsFor is the pipeline catalog for the dashboard and Activity registration.
// Executable sequences are the explicit calls in workflow_vN.go — keep aligned.
func StepsFor(v Version) []Step {
	switch v {
	case V1:
		return []Step{StepReceived, StepPayment, StepPrep, StepHandoff}
	case V2:
		return []Step{StepReceived, StepPayment, StepLoyalty, StepPrep, StepHandoff}
	case V3:
		return []Step{StepReceived, StepFraudCheck, StepPayment, StepLoyalty, StepPrep, StepHandoff}
	case V4:
		return []Step{
			StepReceived, StepFraudCheck, StepPayment, StepLoyalty,
			StepPrep, StepMobileDispatch, StepHandoff,
		}
	case V5:
		return []Step{StepReceived, StepPayment, StepLoyalty, StepPrep, StepDriveThru}
	default:
		return nil
	}
}

func ParseVersion(s string) (Version, bool) {
	for _, v := range AllVersions {
		if Version(s) == v {
			return v, true
		}
	}
	return "", false
}
