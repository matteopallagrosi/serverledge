package workflow

import (
	"log"

	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/node"
	"github.com/serverledge-faas/serverledge/internal/registration"
)

type ThresholdBasedPolicy struct{}

var utilizationThreshold float64
var maxOffloadedTasks int

func (policy *ThresholdBasedPolicy) Init() {
	utilizationThreshold = config.GetFloat(config.WORKFLOW_THRESHOLD_BASED_POLICY_THRESHOLD, 0.75)
	maxOffloadedTasks = config.GetInt(config.WORKFLOW_THRESHOLD_BASED_POLICY_MAX_OFFLOADED, 5)
}

func (policy *ThresholdBasedPolicy) Evaluate(r *Request, p *Progress, runningTasks map[TaskId]bool) ([]OffloadingDecision, error) {

	if p == nil || !r.CanDoOffloading || len(p.ReadyToExecute) == 0 {
		return []OffloadingDecision{{Offload: false}}, nil
	}

	usedMemory := float64(node.LocalResources.UsedMemory())
	totalMemory := float64(node.LocalResources.TotalMemory())

	var localTasks []TaskId
	var offloadedTasks []TaskId
	offloadedMemory := int64(0)
	var nextTasks []TaskId

	for _, tid := range p.ReadyToExecute {
		task := r.W.Tasks[tid]

		switch typedTask := task.(type) {
		case *FunctionTask:
			f, found := function.GetFunction(typedTask.Func)
			if !found {
				log.Printf("Could not find function for task %s", tid)
				localTasks = append(localTasks, tid)
				continue
			}

			taskMem := float64(f.MemoryMB)
			if (usedMemory+taskMem)/totalMemory <= utilizationThreshold {
				log.Printf("Threshold OK...executing locally %v", tid)
				// execute locally next task
				localTasks = append(localTasks, tid)
				usedMemory += taskMem
			} else {
				log.Printf("Threshold violated...must offload %v", tid)
				// Must offload
				offloadedTasks = append(offloadedTasks, tid)
				offloadedMemory += f.MemoryMB

				nextTasks = append(nextTasks, typedTask.NextTask)
			}

		default:
			localTasks = append(localTasks, tid)
		}
	}

	if len(offloadedTasks) == 0 {
		r.Plan = &OffloadingPlan{ToExecute: localTasks}
		return []OffloadingDecision{{Offload: false}}, nil
	}

	for len(offloadedTasks) <= maxOffloadedTasks && len(nextTasks) > 0 {
		// pop one candidate
		nextTaskId := nextTasks[0]
		nextTasks = nextTasks[1:]

		nextTask := r.W.Tasks[nextTaskId]
		switch typedTask := nextTask.(type) {
		case *FunctionTask:
			f, found := function.GetFunction(typedTask.Func)
			if !found {
				log.Printf("Could not find function for task %s", nextTaskId)
				continue
			}
			if (usedMemory+float64(f.MemoryMB))/totalMemory > utilizationThreshold {
				log.Printf("%v also violates threshold", nextTaskId)
				offloadedMemory += f.MemoryMB
				offloadedTasks = append(offloadedTasks, nextTaskId)

				// add successors to candidates
				nextTasks = append(nextTasks, typedTask.NextTask)

			} else {
				log.Printf("%v does not violate threshold and will be executed locally", nextTaskId)
			}
		case ConditionalTask:
			log.Printf("%v being added to offloaded group (ConditionalTask)", nextTaskId)
			offloadedTasks = append(offloadedTasks, nextTaskId)
			for _, tid := range typedTask.GetAlternatives() {
				nextTasks = append(nextTasks, tid)
			}
		case FanOutTask:
			log.Printf("%v being added to offloaded group (ParallelTask)", nextTaskId)
			offloadedTasks = append(offloadedTasks, nextTaskId)
			for _, tid := range typedTask.GetBranches() {
				nextTasks = append(nextTasks, tid)
			}
		default:
			// execute locally
		}
	}

	// Search for a node that can accept offloading
	nearbyServers := registration.GetFullNeighborInfo()
	offloadingTarget := ""
	offloadingTargetMem := int64(0)

	log.Printf("Total memory for offloading: %d", offloadedMemory)

	if nearbyServers != nil {
		for k, v := range nearbyServers {
			// TODO: apply a threshold here ?
			if (v.AvailableMemory) >= offloadedMemory { // TODO: should look at free memory for ranking (ignoring warm containers)
				if offloadingTarget == "" || v.AvailableMemory > offloadingTargetMem {
					offloadingTarget = k
					offloadingTargetMem = v.AvailableMemory
				}
			} else {
				log.Printf("Not enough memory to offload to %v", k)
			}
		}
	}

	var targetNode *registration.NodeRegistration = nil

	if offloadingTarget == "" {
		// Cloud
		targetNode = registration.GetRemoteOffloadingTarget()
	} else {
		targetNode = registration.GetPeerFromKey(offloadingTarget)
	}

	if targetNode == nil {
		log.Printf("No target available for offloading")
		localTasks = append(localTasks, offloadedTasks...)
		r.Plan = &OffloadingPlan{ToExecute: localTasks}
		return []OffloadingDecision{{Offload: false}}, nil
	}

	r.Plan = &OffloadingPlan{ToExecute: localTasks}

	log.Printf("Offloading %v to %v", offloadedTasks, targetNode)
	return []OffloadingDecision{{Offload: true, RemoteHost: targetNode.APIUrl(), OffloadingPlan: OffloadingPlan{ToExecute: offloadedTasks}}}, nil
}
