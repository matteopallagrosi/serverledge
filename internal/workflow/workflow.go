package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
	"time"

	"github.com/serverledge-faas/serverledge/internal/node"

	"github.com/serverledge-faas/serverledge/internal/client"
	"github.com/serverledge-faas/serverledge/internal/config"
	"github.com/serverledge-faas/serverledge/internal/metrics"
	"golang.org/x/exp/slices"

	"github.com/serverledge-faas/serverledge/internal/cache"
	"github.com/serverledge-faas/serverledge/utils"

	"github.com/serverledge-faas/serverledge/internal/asl"
	"github.com/serverledge-faas/serverledge/internal/function"
	"github.com/serverledge-faas/serverledge/internal/types"
)

var offloadingPolicy OffloadingPolicy = nil

func CreateOffloadingPolicy() {
	policyConf := config.GetString(config.WORKFLOW_OFFLOADING_POLICY, "disable")
	log.Printf("Configured offloading policy: %s\n", policyConf)
	if policyConf == "ilp" {
		offloadingPolicy = &IlpOffloadingPolicy{}
	} else if policyConf == "heftless" {
		offloadingPolicy = &HEFTlessPolicy{}
	} else if policyConf == "threshold" {
		offloadingPolicy = &ThresholdBasedPolicy{}
	} else { // default, disable offloading
		offloadingPolicy = &NoOffloadingPolicy{}
	}

	offloadingPolicy.Init()
}

// Workflow is a Workflow to drive the execution of the workflow
type Workflow struct {
	Name  string // identifier of the Workflow
	Start *StartTask
	Tasks map[TaskId]Task
	End   *EndTask

	prevTasks map[TaskId][]TaskId
}

func newWorkflow() Workflow {
	start := NewStartTask()
	end := NewEndTask()
	tasks := make(map[TaskId]Task)
	tasks[start.Id] = start
	tasks[end.Id] = end

	workflow := Workflow{
		Start: start,
		End:   end,
		Tasks: tasks,
	}
	return workflow
}

func (wflow *Workflow) Find(taskId TaskId) (Task, bool) {
	task, found := wflow.Tasks[taskId]
	return task, found
}

// add can be used to add a new task to the Workflow. Does not chain anything, but updates Workflow width
func (wflow *Workflow) add(task Task) {
	wflow.Tasks[task.GetId()] = task // if already exists, overwrites!
}

func (wflow *Workflow) GetPreviousTasks(task TaskId) []TaskId {
	if wflow.prevTasks == nil {
		wflow.computePreviousTasks()
	}

	return wflow.prevTasks[task]
}

func (wflow *Workflow) GetAllPreviousTasks() map[TaskId][]TaskId {
	if wflow.prevTasks == nil {
		wflow.computePreviousTasks()
	}

	return wflow.prevTasks
}

func (wflow *Workflow) computePreviousTasks() {
	wflow.prevTasks = make(map[TaskId][]TaskId)
	visited := make(map[TaskId]bool)
	for tid, _ := range wflow.Tasks {
		wflow.prevTasks[tid] = make([]TaskId, 0)
		visited[tid] = false
	}

	toVisit := []Task{wflow.Start}

	for len(toVisit) > 0 {
		task := toVisit[0]
		toVisit = toVisit[1:]
		visited[task.GetId()] = true

		// task -> nextTask
		var nextTasks []TaskId
		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks = typedTask.GetAlternatives()
		case *ParallelTask:
			nextTasks = append(nextTasks, typedTask.Branches...)
		case UnaryTask:
			nextTasks = append(nextTasks, typedTask.GetNext())
		case *EndTask:
			continue
		default:
			panic("unknown task type")
		}

		for _, nextTask := range nextTasks {
			if nextTask != "" {
				if !slices.Contains(wflow.prevTasks[nextTask], task.GetId()) {
					wflow.prevTasks[nextTask] = append(wflow.prevTasks[nextTask], task.GetId())
				}
				if !visited[nextTask] {
					toVisit = append(toVisit, wflow.Tasks[nextTask])
				}
			}
		}
	}
}

func Visit(workflow *Workflow, taskId TaskId, excludeEnd bool) []Task {

	task, ok := workflow.Find(taskId)
	if !ok {
		return []Task{}
	}

	tasks := make([]Task, 0)
	visited := make(map[TaskId]bool)
	toVisit := []Task{task}

	for len(toVisit) > 0 {
		task := toVisit[0]
		tasks = append(tasks, task)
		toVisit = toVisit[1:]
		visited[task.GetId()] = true

		var nextTasks []TaskId
		switch typedTask := task.(type) {
		case ConditionalTask:
			nextTasks = typedTask.GetAlternatives()
		case UnaryTask:
			nextTasks = append(nextTasks, typedTask.GetNext())
		case *ParallelTask:
			nextTasks = append(nextTasks, typedTask.Branches...)
		case *EndTask:
			continue
		default:
			panic("unknown task type: " + task.GetType())
		}

		for _, nt := range nextTasks {
			if _, ok := visited[nt]; !ok {
				nextTask, ok := workflow.Tasks[nt]
				if ok && (!excludeEnd || nextTask.GetType() != End) {
					if !slices.Contains(toVisit, nextTask) {
						toVisit = append(toVisit, nextTask)
					}
				}
			}
		}
	}

	return tasks
}

func (wflow *Workflow) IsTaskEligibleForExecution(id TaskId, p *Progress) bool {
	for _, prev := range wflow.prevTasks[id] {
		if p.Status[prev] == Pending {
			return false
		}
	}

	return true
}

func (wflow *Workflow) ExecuteTask(r *Request, taskToExecute TaskId, input *TaskData, progress *Progress) (*TaskData, error) {
	var outputData *TaskData

	n, ok := wflow.Find(taskToExecute)
	if !ok {
		return nil, fmt.Errorf("failed to find task %s", n.GetId())
	}

	// Check if the current task defines any pre-processing operations.
	// If present, execute them immediately to mutate the input data before the task execution begins.
	if ops := n.GetPreProcessors(); len(ops) > 0 {
		err := ApplyPreProcessors(input, ops)
		if err != nil {
			r.mu.Lock()
			progress.Fail(n.GetId())
			r.mu.Unlock()
			return nil, fmt.Errorf("failed to pre-process data for task %s: %w", n.GetId(), err)
		}
	}

	switch task := n.(type) {
	case UnaryTask:
		output, err := task.execute(input, r)

		r.mu.Lock()
		defer r.mu.Unlock()

		if err != nil {
			progress.Fail(n.GetId())
			return nil, err
		}

		outputData = NewTaskData(output)
		progress.Complete(task.GetId())

		if pTask, isParallel := task.(*ParallelTask); isParallel {
			for _, nextTask := range pTask.Branches {
				if wflow.IsTaskEligibleForExecution(nextTask, progress) {
					progress.ReadyToExecute = append(progress.ReadyToExecute, nextTask)
				} else {
					fmt.Printf("task %s complete, but %s not eligible for execution", task.GetId(), nextTask)
				}
			}
		} else {
			nextTask := task.GetNext()
			if wflow.IsTaskEligibleForExecution(nextTask, progress) {
				progress.ReadyToExecute = append(progress.ReadyToExecute, nextTask)
			} else {
				fmt.Printf("task %s complete, but %s not eligible for execution", task.GetId(), nextTask)
			}
		}

	case ConditionalTask:
		nextTaskId, err := task.Evaluate(input, r)

		r.mu.Lock()
		defer r.mu.Unlock()

		if err != nil {
			progress.Fail(n.GetId())
			return nil, err
		}

		// we skip all tasks that will not be executed
		toSkip := make([]Task, 0)
		toNotSkip := Visit(wflow, nextTaskId, false)
		for _, a := range task.GetAlternatives() {
			if a == nextTaskId {
				continue
			}
			branchTasks := Visit(wflow, a, false)
			for _, otherTask := range branchTasks {
				if !slices.Contains(toNotSkip, otherTask) {
					toSkip = append(toSkip, otherTask)
				}
			}
		}
		for _, t := range toSkip {
			progress.Skip(t.GetId())
		}
		progress.Complete(task.GetId())

		outputData = NewTaskData(input.Data)
		if wflow.IsTaskEligibleForExecution(nextTaskId, progress) {
			progress.ReadyToExecute = append(progress.ReadyToExecute, nextTaskId)
		}

		// Update metrics, if enabled
		if metrics.Enabled {
			metrics.AddBranchCount(string(task.GetId()), string(nextTaskId))
		}
	case *EndTask:
		r.mu.Lock()
		defer r.mu.Unlock()
		progress.Complete(task.GetId())
		outputData = input
	}

	return outputData, nil
}

// GetUniqueFunctions returns a list with the function names used in the Workflow. The returned function names are unique and in alphabetical order
func (wflow *Workflow) GetUniqueFunctions() []string {
	allFunctionsMap := make(map[string]interface{})
	for _, task := range wflow.Tasks {
		switch n := task.(type) {
		case *FunctionTask:
			allFunctionsMap[n.Func] = nil
		default:
			continue
		}
	}
	uniqueFunctions := make([]string, 0, len(allFunctionsMap))
	for fName := range allFunctionsMap {
		uniqueFunctions = append(uniqueFunctions, fName)
	}
	// we sort the list to always get the same result
	sort.Strings(uniqueFunctions)

	return uniqueFunctions
}

func (wflow *Workflow) getEtcdKey() string {
	return getEtcdKey(wflow.Name)
}

func getEtcdKey(workflowName string) string {
	return fmt.Sprintf("/workflow/%s", workflowName)
}

// GetAllWorkflows returns the workflow names
func GetAllWorkflows() ([]string, error) {
	return function.GetAllWithPrefix("/workflow")
}

func getFromCache(name string) (*Workflow, bool) {
	localCache := cache.GetCacheInstance()
	cachedObj, found := localCache.Get(name)
	if !found {
		return nil, false
	}
	//cache hit
	//return a safe copy of the workflow previously obtained
	fc := *cachedObj.(*Workflow)
	return &fc, true
}

func getFromEtcd(name string) (*Workflow, error) {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return nil, errors.New("failed to connect to ETCD")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	key := getEtcdKey(name)
	getResponse, err := cli.Get(ctx, key)
	if err != nil || len(getResponse.Kvs) < 1 {
		return nil, fmt.Errorf("failed to retrieve value for key %s", key)
	}

	var f Workflow
	err = json.Unmarshal(getResponse.Kvs[0].Value, &f)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal json: %v", err)
	}

	return &f, nil
}

// Get gets the Workflow from cache or from ETCD
func Get(name string) (*Workflow, bool) {
	val, found := getFromCache(name)
	if !found {
		// cache miss
		f, err := getFromEtcd(name)
		if err != nil {
			return nil, false
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(name, f, cache.DefaultExp)
		return f, true
	}

	return val, true
}

// Save creates and register the workflow in Serverledge
// It is like Save for a simple function
func (wflow *Workflow) Save() error {
	if len(wflow.Name) == 0 {
		return fmt.Errorf("cannot save an anonymous wflow (no name set)")
	}

	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	// marshal the wflow object into json
	payload, err := json.Marshal(*wflow)
	if err != nil {
		return fmt.Errorf("could not marshal wflow: %v", err)
	}
	// saves the json object into etcd
	_, err = cli.Put(ctx, wflow.getEtcdKey(), string(payload))
	if err != nil {
		return fmt.Errorf("failed etcd Put: %v", err)
	}

	// Add the wflow to the local cache
	cache.GetCacheInstance().Set(wflow.Name, wflow, cache.DefaultExp)

	return nil
}

func (wflow *Workflow) initializeOrRetrieveProgress(r *Request) (*Progress, bool, error) {
	var progress *Progress
	var err error
	requestId := ReqId(r.Id)

	if !r.Resuming {
		progress = InitProgress(requestId, wflow)
		return progress, false, nil
	} else {
		progress, err = RetrieveProgress(requestId)
		if err != nil {
			return nil, true, fmt.Errorf("failed to retrieve wflow progress: %v", err)
		}
		return progress, true, nil
	}
}

func (wflow *Workflow) savePartialDataForReadyTasks(requestId ReqId, progress *Progress, data map[TaskId]*TaskData) error {
	handledTasks := make(map[TaskId]bool)

	for _, task := range progress.ReadyToExecute {
		for _, prev := range wflow.GetPreviousTasks(task) {
			if _, found := handledTasks[prev]; found {
				continue
			}

			dataToSave, ok := data[prev]
			if ok {
				err := dataToSave.Save(requestId, prev)
				if err != nil {
					return fmt.Errorf("Could not save partial data: %v", err)
				}
			} else {
				// PD not available locally; they might be on Etcd already...
			}

			handledTasks[prev] = true
		}
	}

	return nil
}

// Invoke schedules each function of the workflow and invokes them
func (wflow *Workflow) Invoke(r *Request) error {

	alwaysSaveProgress := config.GetBool(config.WORKFLOW_ALWAYS_SAVE_PROGRESS, false)

	requestId := ReqId(r.Id)

	progress, isProgressOnEtcd, err := wflow.initializeOrRetrieveProgress(r)
	if err != nil {
		return err
	}

	// Initialize map of TaskData
	dataMap := make(map[TaskId]*TaskData)

	if len(progress.ReadyToExecute) == 0 {
		return fmt.Errorf("[Rq-%v] wflow resumed but no task is ready for execution", requestId)
	}

	log.Printf("[Rq-%v] Starting/resuming execution (%d to executed)", requestId, len(progress.ReadyToExecute))

	type taskResult struct {
		tid TaskId
		out *TaskData
		err error
	}

	resultChan := make(chan taskResult, len(wflow.Tasks))
	runningTasks := make(map[TaskId]bool)

	for len(progress.ReadyToExecute) > 0 || len(runningTasks) > 0 {
		r.mu.Lock()

		if len(runningTasks) == 0 && len(progress.ReadyToExecute) > 0 {
			t0 := time.Now()
			decision, err := offloadingPolicy.Evaluate(r, progress)
			policyTime := time.Since(t0).Seconds()
			r.ExecReport.SchedulingTime += policyTime

			if err != nil {
				r.mu.Unlock()
				return fmt.Errorf("an error occurred in policy evaluation: %v", err)
			}

			if decision.Offload || alwaysSaveProgress {
				err := progress.Save()
				if err != nil {
					r.mu.Unlock()
					return fmt.Errorf("Could not save progress: %v", err)
				}
				isProgressOnEtcd = true

				err = wflow.savePartialDataForReadyTasks(requestId, progress, dataMap)
				if err != nil {
					r.mu.Unlock()
					return fmt.Errorf("Could not save partial data: %v", err)
				}

			}

			if decision.Offload {
				r.mu.Unlock()
				err = offload(r, &decision)
				if err != nil {
					return err
				}

				if r.ExecReport.Result != nil {
					// Workflow execution has completed on remote node
					log.Printf("[Rq-%v] Workflow has completed on remote node", requestId)
					return nil
				}

				r.mu.Lock()
				progress, err = RetrieveProgress(requestId)
				if err != nil {
					r.mu.Unlock()
					return fmt.Errorf("Could not retrieve progress after offloading: %v", err)
				}

				r.mu.Unlock()
				log.Printf("[Rq-%v] Ready to execute after offloading: %v", requestId, progress.ReadyToExecute)
			}
		}

		var tasksToKeep []TaskId
		var tasksToLaunch []TaskId

		// pick next executable task
		var taskToExecute TaskId = ""
		for _, task := range progress.ReadyToExecute {
			if r.Plan == nil || slices.Contains(r.Plan.ToExecute, task) {
				tasksToLaunch = append(tasksToLaunch, task)
			} else {
				tasksToKeep = append(tasksToKeep, task)
			}
		}
		progress.ReadyToExecute = tasksToKeep

		if len(runningTasks) == 0 && len(tasksToLaunch) == 0 && len(progress.ReadyToExecute) > 0 {
			log.Printf("[Rq-%v] Workflow has not completed but there is nothing left to execute in the plan", requestId)
			r.mu.Unlock()
			break
		}

		type dispatchInfo struct {
			tid TaskId
			in  *TaskData
		}
		var toDispatch []dispatchInfo

		for _, taskToExecute = range tasksToLaunch {
			log.Printf("[Rq-%v] Now going to execute %s", requestId, taskToExecute)

			input, err := wflow.prepareInput(taskToExecute, progress, dataMap, r)
			if err != nil {
				r.mu.Unlock()
				return err
			}

			if input == nil {
				log.Printf("Nil input for task: %s", taskToExecute)
			}

			runningTasks[taskToExecute] = true
			toDispatch = append(toDispatch, dispatchInfo{tid: taskToExecute, in: input})
		}
		r.mu.Unlock()

		for _, item := range toDispatch {
			go func(tid TaskId, in *TaskData) {
				out, execErr := wflow.ExecuteTask(r, tid, in, progress)
				resultChan <- taskResult{tid: tid, out: out, err: execErr}
			}(item.tid, item.in)
		}

		if len(runningTasks) > 0 {
			res := <-resultChan

			r.mu.Lock()
			delete(runningTasks, res.tid)

			if res.err != nil {
				if errors.Is(err, node.OutOfResourcesErr) {
					// TODO
					log.Printf("[Rq-%v] Could not execute %s: out of resources", requestId, taskToExecute)
					r.mu.Unlock()
					return res.err
				} else {
					r.mu.Unlock()
					return fmt.Errorf("failed wflow execution: %v", err)
				}
			}

			log.Printf("[Rq-%v] Executed %s", requestId, res.tid)
			if res.out != nil {
				dataMap[res.tid] = res.out
			}

			if len(progress.ReadyToExecute) == 0 && len(runningTasks) == 0 && res.out != nil {
				r.ExecReport.Result = res.out.Data

				log.Printf("[Rq-%v] Workflow completed", requestId)

				if isProgressOnEtcd {
					err = DeleteProgress(requestId)
					if err != nil {
						log.Printf("Failed to delete progress: %v", err)
					}
					err = DeleteAllTaskData(requestId)
					if err != nil {
						log.Printf("Failed to delete task data: %v", err)
					}
				}

				r.mu.Unlock()
				return nil
			}

			r.mu.Unlock()
		}
	}

	r.mu.Lock()
	if len(progress.ReadyToExecute) > 0 {
		err = progress.Save()
		if err != nil {
			r.mu.Unlock()
			return err
		}
		err = wflow.savePartialDataForReadyTasks(requestId, progress, dataMap)
		if err != nil {
			r.mu.Unlock()
			return fmt.Errorf("Could not save partial data: %v", err)
		}
	}

	r.mu.Unlock()
	return nil
}

func offload(r *Request, policyDecision *OffloadingDecision) error {

	log.Printf("[Rq-%v] Offloading decision: %v", r.Id, policyDecision)

	request := WorkflowInvocationResumeRequest{
		ReqId: r.Id,
		WorkflowInvocationRequest: client.WorkflowInvocationRequest{
			Params:          r.Params,
			CanDoOffloading: false,
			Async:           false, // we force a synchronous request
			QoS:             r.QoS,
		},
		Plan: policyDecision.OffloadingPlan,
	}

	// Update slack for deadline satisfaction
	request.QoS.MaxRespT -= time.Now().Sub(r.Arrival).Seconds()

	invocationBody, err := json.Marshal(request)
	if err != nil {
		return fmt.Errorf("JSON marshaling failed: %v", err)
	}

	// Send invocation request
	url := fmt.Sprintf("%s/workflow/resume/%s", policyDecision.RemoteHost, r.W.Name)
	resp, err := utils.PostJson(url, invocationBody)
	if err != nil {
		if resp != nil && resp.StatusCode == http.StatusTooManyRequests {
			return node.OutOfResourcesErr
		} else {
			return fmt.Errorf("HTTP request for offloading failed: %v", err)
		}
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("failed offloaded workflow: %v", err)
	}

	var response InvocationResponse
	body, _ := io.ReadAll(resp.Body)
	err = json.Unmarshal(body, &response)
	if err != nil {
		return fmt.Errorf("Failed InvocationResponse unmarshaling: %v", err)
	}

	if !response.Success {
		return fmt.Errorf("failed offloaded workflow: %v", err)
	}

	for k, v := range response.Reports {
		r.ExecReport.Reports[k] = v
	}

	r.ExecReport.SchedulingTime += response.SchedulingTime

	if response.Result == nil {
		// workflow execution is not complete after offloading
		r.ExecReport.Result = nil
	} else {
		r.ExecReport.Result = response.Result
	}

	return nil
}

// Delete removes the Workflow from cache and from etcd, so it cannot be invoked anymore
func (wflow *Workflow) Delete() error {
	cli, err := utils.GetEtcdClient()
	if err != nil {
		return err
	}
	ctx := context.TODO()

	dresp, err := cli.Delete(ctx, wflow.getEtcdKey())
	if err != nil || dresp.Deleted != 1 {
		return fmt.Errorf("failed Delete: %v", err)
	}

	// Remove the function from the local cache
	cache.GetCacheInstance().Delete(wflow.Name)

	return nil
}

// Exists return true if the workflow exists either in etcd or in cache. If it only exists in Etcd, it saves the workflow also in caches
func (wflow *Workflow) Exists() bool {
	_, found := getFromCache(wflow.Name)
	if !found {
		// cache miss
		f, err := getFromEtcd(wflow.Name)
		if err != nil {
			if err.Error() == fmt.Sprintf("failed to retrieve value for key %s", getEtcdKey(wflow.Name)) {
				return false
			} else {
				log.Printf("ERROR: %v", err.Error())
				return false
			}
		}
		//insert a new element to the cache
		cache.GetCacheInstance().Set(f.Name, f, cache.DefaultExp)
		return true
	}
	return found
}

func (wflow *Workflow) Equals(comparer types.Comparable) bool {

	workflow2 := comparer.(*Workflow)

	if wflow.Name != workflow2.Name {
		return false
	}

	for k := range wflow.Tasks {
		if !wflow.Tasks[k].Equals(workflow2.Tasks[k]) {
			return false
		}
	}
	return wflow.Start.Equals(workflow2.Start) &&
		wflow.End.Equals(workflow2.End) &&
		len(wflow.Tasks) == len(workflow2.Tasks)
}

func (wflow *Workflow) String() string {
	return fmt.Sprintf(`Workflow{
		Name: %s,
		Start: %s,
		Tasks: %s,
		End:   %s,
	}`, wflow.Name, wflow.Start.String(), wflow.Tasks, wflow.End.String())
}

// MarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Marshal()
func (wflow *Workflow) MarshalJSON() ([]byte, error) {
	// Create a map to hold the JSON representation of the Workflow
	data := make(map[string]interface{})

	// Add the field to the map
	data["Name"] = wflow.Name
	data["Start"] = wflow.Start
	data["End"] = wflow.End
	tasks := make(map[TaskId]interface{})

	// Marshal the interface and store it as concrete task value in the map
	for taskId, task := range wflow.Tasks {
		tasks[taskId] = task
	}
	data["Tasks"] = tasks

	// Marshal the map to JSON
	return json.Marshal(data)
}

// UnmarshalJSON is needed because Task is an interface
// This is automatically used when calling json.Unmarshal()
func (wflow *Workflow) UnmarshalJSON(data []byte) error {
	// Create a temporary map to decode the JSON data
	var tempMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &tempMap); err != nil {
		return err
	}
	// extract simple fields
	if rawStart, ok := tempMap["Start"]; ok {
		if err := json.Unmarshal(rawStart, &wflow.Start); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Start' field in JSON")
	}

	if rawEnd, ok := tempMap["End"]; ok {
		if err := json.Unmarshal(rawEnd, &wflow.End); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'End' field in JSON")
	}

	if rawName, ok := tempMap["Name"]; ok {
		if err := json.Unmarshal(rawName, &wflow.Name); err != nil {
			return err
		}
	} else {
		return fmt.Errorf("missing 'Name' field in JSON")
	}

	// Cycle on each map entry and decode the type
	var tempTaskMap map[string]json.RawMessage
	if err := json.Unmarshal(tempMap["Tasks"], &tempTaskMap); err != nil {
		return err
	}
	wflow.Tasks = make(map[TaskId]Task)
	for taskId, value := range tempTaskMap {
		err := wflow.decodeTask(taskId, value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (wflow *Workflow) decodeTask(taskId string, value json.RawMessage) error {
	var tempTaskMap map[string]interface{}
	if err := json.Unmarshal(value, &tempTaskMap); err != nil {
		return err
	}
	taskType, ok := tempTaskMap["Type"].(string)
	if !ok {
		return fmt.Errorf("unknown taskType: %v", tempTaskMap["Type"])
	}
	var err error

	task := TaskFromType(TaskType(taskType))

	switch TaskType(taskType) {
	case Start:
		task := &StartTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Function:
		task := &FunctionTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && task.Func != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Choice:
		task := &ChoiceTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && len(task.AlternativeNextTasks) == len(task.Conditions) {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	case Parallel:
		task := &ParallelTask{}
		err = json.Unmarshal(value, task)
		if err == nil && task.Id != "" && len(task.Branches) > 0 {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	default:
		err = json.Unmarshal(value, task)
		if err == nil && task.GetId() != "" {
			wflow.Tasks[TaskId(taskId)] = task
			return nil
		}
	}
	var unmarshalTypeError *json.UnmarshalTypeError
	if err != nil && !errors.As(err, &unmarshalTypeError) {
		// abort if we have an error other than the wrong type
		return err
	}

	return fmt.Errorf("failed to decode task")
}

// IsEmpty returns true if the workflow has 0 tasks or exactly one StartTask and one EndTask.
func (wflow *Workflow) IsEmpty() bool {
	if len(wflow.Tasks) == 0 {
		return true
	}

	hasOnlyStartAndEnd := false
	if len(wflow.Tasks) == 2 {
		hasStart := 0
		hasEnd := 0
		for _, task := range wflow.Tasks {
			if task.GetType() == Start {
				hasStart++
			}
			if task.GetType() == End {
				hasEnd++
			}
		}
		hasOnlyStartAndEnd = (hasStart == 1) && (hasEnd == 1)
	}

	if hasOnlyStartAndEnd {
		return true
	}

	return false
}

// findNextOrTerminate returns the State, its name and if it is terminal or not
func findNextOrTerminate(state asl.CanEnd, sm *asl.StateMachine) (asl.State, string, bool) {
	isTerminal := state.IsEndState()
	var nextState asl.State = nil
	var nextStateName = ""

	if !isTerminal {
		nextName, ok := state.(asl.HasNext).GetNext()
		if !ok {
			return nil, "", true
		}
		nextStateName = nextName
		nextState = sm.States[nextStateName]
	}
	return nextState, nextStateName, isTerminal
}

// prepareInput resolves and prepares the input TaskData for a given task before its execution.
func (wflow *Workflow) prepareInput(taskToExecute TaskId, progress *Progress, dataMap map[TaskId]*TaskData, r *Request) (*TaskData, error) {
	requestId := ReqId(r.Id)

	if wflow.Tasks[taskToExecute].GetType() == Start {
		return NewTaskData(r.Params), nil
	}

	previousTasks := wflow.GetPreviousTasks(taskToExecute)
	keepIndex := 0
	for _, previousTask := range previousTasks {
		if progress.Status[previousTask] != Skipped {
			previousTasks[keepIndex] = previousTask
			keepIndex++
		}
	}
	previousTasks = previousTasks[:keepIndex]

	// Case 1: Initial task of a parallel branch
	if len(previousTasks) == 0 {
		var parentParallelId TaskId = ""

		// Find the parent ParallelTask that contains this branch task
		for _, t := range wflow.Tasks {
			if pTask, isParallel := t.(*ParallelTask); isParallel {
				for _, branchTask := range pTask.Branches {
					if branchTask == taskToExecute {
						parentParallelId = pTask.GetId()
						break
					}
				}
			}
			if parentParallelId != "" {
				break
			}
		}

		if parentParallelId != "" {
			input, found := dataMap[parentParallelId]
			if !found {
				var errRet error
				input, errRet = RetrievePartialData(requestId, parentParallelId)
				if errRet != nil {
					return nil, fmt.Errorf("could not retrieve partial data for parallel parent %s: %v", parentParallelId, errRet)
				}
			}
			return input, nil
		}
	}

	// Case 2: Multiple predecessors (logical Fan-In)
	if len(previousTasks) > 1 {
		mergedResult := make(map[string]interface{})
		parallelResults := make([]interface{}, len(previousTasks))

		for i, prevTask := range previousTasks {

			in, found := dataMap[prevTask]
			if !found {
				var err error
				in, err = RetrievePartialData(ReqId(r.Id), prevTask)
				if err != nil {
					return nil, fmt.Errorf("could not retrieve partial data for %s: %v", prevTask, err)
				}
			}
			parallelResults[i] = in.Data
		}

		mergedResult["parallel_results"] = parallelResults

		// If the next task is a FunctionTask, it applies a positional mapping where the output of the i-th parallel branch
		// is mapped directly to the i-th input parameter of the function. For any other task type, the output remains unchanged.
		currentTask, ok := wflow.Find(taskToExecute)
		if !ok {
			return nil, fmt.Errorf("failed to find next task %s", taskToExecute)
		}

		fTask, isFunc := currentTask.(*FunctionTask)
		if !isFunc {
			return NewTaskData(mergedResult), nil
		}

		funct, exists := function.GetFunction(fTask.Func)
		if !exists || funct.Signature == nil {
			return NewTaskData(mergedResult), nil
		}

		delete(mergedResult, "parallel_results")

		// Positional mapping
		expectedInputs := funct.Signature.GetInputs()
		for i, res := range parallelResults {
			if i < len(expectedInputs) {
				targetParamName := expectedInputs[i].Name
				if resMap, ok := res.(map[string]interface{}); ok {
					for _, val := range resMap {
						mergedResult[targetParamName] = val
						break
					}
				}
			}
		}

		return NewTaskData(mergedResult), nil
	}

	// Case 3: single predecessor
	if len(previousTasks) == 1 {
		previousTask := previousTasks[0]
		input, found := dataMap[previousTask]
		if !found {
			var err error
			input, err = RetrievePartialData(requestId, previousTask)
			if err != nil {
				return nil, fmt.Errorf("could not retrieve partial data: %v", err)
			}
		}
		return input, nil
	}

	return nil, fmt.Errorf("nessun predecessore valido per %s", taskToExecute)
}
