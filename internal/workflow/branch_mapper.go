package workflow

import (
	"github.com/serverledge-faas/serverledge/internal/function"
)

// MapParallelOutputToNextInput prepares the parallel task's output to be used as input for the next task.
// If the next task is a FunctionTask, it applies a positional mapping where the output of the i-th parallel branch
// is mapped directly to the i-th input parameter of the function. For any other task type, the output remains unchanged.
func MapParallelOutputToNextInput(output map[string]interface{}, nextTask Task) map[string]interface{} {
	resultsList, ok := output["parallel_results"].([]interface{})
	if !ok {
		return output
	}

	fTask, isFunc := nextTask.(*FunctionTask)
	if !isFunc {
		return output
	}

	delete(output, "parallel_results")

	funct, exists := function.GetFunction(fTask.Func)
	if !exists || funct.Signature == nil {
		return output
	}

	// Positional mapping
	expectedInputs := funct.Signature.GetInputs()
	for i, res := range resultsList {
		if i < len(expectedInputs) {
			targetParamName := expectedInputs[i].Name
			if resMap, ok := res.(map[string]interface{}); ok {
				for _, val := range resMap {
					output[targetParamName] = val
					break
				}
			}
		}
	}

	return output
}
