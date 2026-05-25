package asl

import (
	"fmt"

	"github.com/buger/jsonparser"
	"github.com/serverledge-faas/serverledge/internal/types"
)

type ParallelState struct {
	Type     StateType
	Branches []*StateMachine
	Next     string
	End      bool
}

func (p *ParallelState) Validate(stateNames []string) error {
	//TODO implement me
	panic("implement me")
}

func (p *ParallelState) IsEndState() bool {
	return p.End
}

func (p *ParallelState) GetResources() []string {
	funcs := make([]string, 0)
	for _, branchStateMachine := range p.Branches {
		funcs = append(funcs, branchStateMachine.GetFunctionNames()...)
	}
	return funcs
}

func (p *ParallelState) Equals(cmp types.Comparable) bool {
	p2 := cmp.(*ParallelState)
	return p.Type == p2.Type
}

func NewEmptyParallel() *ParallelState {
	return &ParallelState{
		Type:     Parallel,
		Branches: make([]*StateMachine, 0),
		Next:     "",
		End:      false,
	}
}

func (p *ParallelState) ParseFrom(jsonData []byte) (State, error) {

	p.Next = JsonExtractStringOrDefault(jsonData, "Next", "")

	p.End = JsonExtractBool(jsonData, "End")

	branchesData, errBranches := JsonExtract(jsonData, "Branches")
	if errBranches != nil {
		return nil, fmt.Errorf("failed to parse Branches %v", branchesData)
	}

	var parseErr error

	_, err := jsonparser.ArrayEach(branchesData, func(value []byte, dataType jsonparser.ValueType, offset int, cbErr error) {
		if parseErr != nil {
			return
		}

		if cbErr != nil {
			parseErr = fmt.Errorf("malformed JSON at offset %d: %w", offset, cbErr)
			return
		}

		branchSM := &StateMachine{}

		branchSM.StartAt = JsonExtractStringOrDefault(value, "StartAt", "")
		if branchSM.StartAt == "" {
			parseErr = fmt.Errorf("missing StartAt field within a branch")
			return
		}

		statesData := JsonExtractStringOrDefault(value, "States", "")

		statesMap, errStates := parseStates(statesData)
		if errStates != nil {
			parseErr = fmt.Errorf("failed to parse States key: %v", errStates)
			return
		}

		branchSM.States = statesMap

		p.Branches = append(p.Branches, branchSM)
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read the branches array: %w", err)
	}

	if parseErr != nil {
		return nil, fmt.Errorf("error parsing a branch: %w", parseErr)
	}

	return p, nil
}

func (p *ParallelState) GetNext() (string, bool) {
	if p.End == false {
		return p.Next, true
	}
	return "", false
}

func (p *ParallelState) GetType() StateType {
	return Parallel
}

// FIXME: improve
func (p *ParallelState) String() string {
	return "Parallel"
}
