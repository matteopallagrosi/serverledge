package workflow

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// ParallelTask receives an input, propagates it to each of the parallel branches, and produces a result for each of them
type ParallelTask struct {
	baseTask
	Next     TaskId      // task to transition to after all branches successfully complete
	Branches []*Workflow // workflows to be executed in parallel
}

func NewParallelTask(branches []*Workflow) *ParallelTask {
	return &ParallelTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Parallel},
		Branches: branches,
	}
}

func (p *ParallelTask) GetNext() TaskId {
	return p.Next
}

func (p *ParallelTask) SetNext(nextTask Task) error {
	p.Next = nextTask.GetId()
	return nil
}

func copyMap(m map[string]interface{}) map[string]interface{} {
	cp := make(map[string]interface{})
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func (p *ParallelTask) execute(input *TaskData, r *Request) (map[string]interface{}, error) {
	var wg sync.WaitGroup
	var reportMutex sync.Mutex

	results := make([]interface{}, len(p.Branches))
	errors := make([]error, len(p.Branches))

	for i, branchWflow := range p.Branches {
		wg.Add(1)

		go func(idx int, bw *Workflow) {
			defer wg.Done()

			branchReqId := fmt.Sprintf("%s_branch_%s_%d", r.Id, p.Id, idx)

			branchParams := copyMap(input.Data)

			//
			var branchParamsSize uint64 = 0
			if payloadBytes, err := json.Marshal(branchParams); err == nil {
				branchParamsSize = uint64(len(payloadBytes))
			} else {
				branchParamsSize = uint64(len(fmt.Sprintf("%v", branchParams)))
			}

			branchReq := NewRequest(branchReqId, bw, branchParams, branchParamsSize)

			elapsed := time.Since(r.Arrival).Seconds()
			remainingGlobalTime := r.QoS.MaxRespT - elapsed

			branchQoS := r.QoS

			// Allocate 80% of the remaining time to the branch, reserving a 20% safety buffer for subsequent tasks
			if remainingGlobalTime > 0.100 {
				branchQoS.MaxRespT = remainingGlobalTime * 0.8
			} else {
				branchQoS.MaxRespT = remainingGlobalTime
			}

			branchReq.QoS = branchQoS

			branchReq.CanDoOffloading = r.CanDoOffloading
			branchReq.Plan = nil
			//

			// Invoke the branch workflow
			err := bw.Invoke(branchReq)
			if err != nil {
				errors[idx] = err
				return
			}

			// Store the branch's final result
			results[idx] = branchReq.ExecReport.Result

			// Multiple goroutines can access this map concurrently. The mutex prevents race conditions.
			reportMutex.Lock()

			for reportId, report := range branchReq.ExecReport.Reports {
				// // Store the report in the original Request
				r.ExecReport.Reports[reportId] = report
			}
			reportMutex.Unlock()
		}(i, branchWflow)
	}

	// Wait for all branches to complete (implicit Fan-In)
	wg.Wait()

	for i, err := range errors {
		if err != nil {
			return nil, fmt.Errorf("branch %d execution failed: %v", i, err)
		}
	}

	mergedResult := make(map[string]interface{})
	mergedResult["parallel_results"] = results

	return mergedResult, nil
}

func (p *ParallelTask) String() string {
	branchesStr := "<"
	for i, branch := range p.Branches {
		branchesStr += branch.Name
		if i < len(p.Branches)-1 {
			branchesStr += " | "
		}
	}
	branchesStr += ">"

	return fmt.Sprintf("[ParallelTask(%d): %s] ", len(p.Branches), branchesStr)
}

func (p *ParallelTask) Equals(cmp types.Comparable) bool {
	p2, ok := cmp.(*ParallelTask)
	if !ok {
		return false
	}
	if p.Id != p2.Id || p.Next != p2.Next || len(p.Branches) != len(p2.Branches) {
		return false
	}
	for i, b := range p.Branches {
		if !b.Equals(p2.Branches[i]) {
			return false
		}
	}
	return true
}
