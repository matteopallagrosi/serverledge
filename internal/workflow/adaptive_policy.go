package workflow

import (
	"log"
)

// AdaptiveOffloadingPolicy acts as a router. It checks the cache first,
// and if there's a miss, it routes the request to either a static or dynamic policy
// based on the real-time execution state of the workflow.
type AdaptiveOffloadingPolicy struct {
	StaticPolicy  OffloadingPolicy
	DynamicPolicy OffloadingPolicy
}

func (policy *AdaptiveOffloadingPolicy) Init() {
	policy.StaticPolicy = &HEFTlessPolicy{}
	policy.StaticPolicy.Init()

	policy.DynamicPolicy = &DynamicAwarePolicy{}
	policy.DynamicPolicy.Init()
}

func (policy *AdaptiveOffloadingPolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {

	completed := 0

	for _, s := range p.Status {
		if s == Executed {
			completed++
		}
	}

	if completed > 0 {
		placement, found := getCachedSolution(r)
		if found {
			log.Printf("Reusing cached placement\n")
			return ComputeDecisionFromPlacement(*placement, p, r), nil
		}
	}

	if len(runningTasks) == 0 {
		// Case 1: No running tasks.
		// Ideal for policies like ILP/HEFTless that statically optimize the full DAG.
		log.Printf("[AdaptivePolicy] System at rest (No running tasks). Triggering Static Policy.")
		return policy.StaticPolicy.Evaluate(r, p, runningTasks)

	} else {
		// Case 2: Tasks are currently in-flight.
		// A dynamic policy will handle this by considering real-time system state.
		log.Printf("[AdaptivePolicy] System busy (Tasks in flight). Triggering Dynamic Policy.")
		return policy.DynamicPolicy.Evaluate(r, p, runningTasks)
	}
}
