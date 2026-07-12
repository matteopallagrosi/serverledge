package workflow

import (
	"fmt"

	"github.com/lithammer/shortuuid"
	"github.com/serverledge-faas/serverledge/internal/types"
)

// ParallelTask receives an input and propagates it to each of the parallel branches
type ParallelTask struct {
	baseTask
	Branches []TaskId // starting TaskId for each branch to be executed in parallel
}

func NewParallelTask(branches []TaskId) *ParallelTask {
	return &ParallelTask{
		baseTask: baseTask{Id: TaskId(shortuuid.New()), Type: Parallel},
		Branches: branches,
	}
}

func (p *ParallelTask) AddBranch(nextTask Task) error {
	p.Branches = append(p.Branches, nextTask.GetId())
	return nil
}

func (p *ParallelTask) GetBranches() []TaskId {
	return p.Branches
}

func (p *ParallelTask) String() string {
	branchesStr := "<"
	for i, branchId := range p.Branches {
		branchesStr += string(branchId)
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
	if p.Id != p2.Id || len(p.Branches) != len(p2.Branches) {
		return false
	}
	for i, b := range p.Branches {
		if b != p2.Branches[i] {
			return false
		}
	}
	return true
}
