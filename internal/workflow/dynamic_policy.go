package workflow

import (
	"log"
)

// DynamicAwarePolicy is a placeholder for the future dynamic scheduling policy.
// It is designed to handle placement calculation while other parallel branches are actively executing.
type DynamicAwarePolicy struct{}

func (policy *DynamicAwarePolicy) Init() {
	log.Println("Initializing DynamicAwarePolicy...")
}

func (policy *DynamicAwarePolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {

	log.Printf("[DynamicPolicy] Evaluating dynamic placement for workflow %s...", r.W.Name)

	// TODO: Implementare qui la logica del risolutore dinamico che tiene conto di runningTasks.

	// Per ora, restituisce un piano locale vuoto per i task ready
	var localTasks []TaskId
	for _, tid := range p.ReadyToExecute {
		localTasks = append(localTasks, tid)
	}

	r.Plan = &OffloadingPlan{ToExecute: localTasks}
	return []OffloadingDecision{{Offload: false}}, nil
}
