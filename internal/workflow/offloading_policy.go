package workflow

type OffloadingDecision struct {
	Offload    bool   `json:"offload"`
	RemoteHost string `json:"remote_host"`
	OffloadingPlan
}

type OffloadingPolicy interface {
	Init()
	Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error)
}

type OffloadingPlan struct {
	ToExecute []TaskId
}

type NoOffloadingPolicy struct{}

func (policy *NoOffloadingPolicy) Init() {
}

func (policy *NoOffloadingPolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {

	return []OffloadingDecision{{Offload: false}}, nil
}

// SetOffloadingPolicy allows replacing the global offloading policy.
// NOTE: This function should only be used in tests to inject a mock policy.
func SetOffloadingPolicy(policy OffloadingPolicy) {
	offloadingPolicy = policy
}
