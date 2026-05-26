package workflow

import (
	"fmt"
	"strconv"
	"strings"
)

// DataOperation represents a generic, operation performed on data.
type DataOperation struct {
	OpType string
	Value  string
}

// ApplyPreProcessors sequentially executes a chain of data manipulation operations on the task input data.
func ApplyPreProcessors(input *TaskData, ops []DataOperation) error {
	for _, op := range ops {
		switch op.OpType {
		case "JSONPathFilter":
			extractedData, err := ApplyJSONPath(input.Data, op.Value)
			if err != nil {
				return fmt.Errorf("failed to apply JSONPath filter '%s': %w", op.Value, err)
			}

			if newMap, ok := extractedData.(map[string]interface{}); ok {
				input.Data = newMap
			} else {
				return fmt.Errorf("JSONPath filter '%s' did not return a valid JSON object", op.Value)
			}
		// TODO: add future data operations here
		default:
			return fmt.Errorf("unsupported pre-processor type: %s", op.OpType)
		}
	}
	return nil
}

// ApplyJSONPath evaluates a map structure using standard JSONPath syntax (e.g., "$.key[1].subkey").
// To ensure compliance with pure ASL, if the path targets a root array index (e.g., "$[1]")
// and the data contains a "parallel_results" key, the path is automatically rewritten
// to target the internal parallel results array (e.g., "$.parallel_results[1]").
func ApplyJSONPath(data map[string]interface{}, path string) (interface{}, error) {
	cleanPath := strings.TrimPrefix(path, "$.")

	// Check if the path attempts to access the root array directly (e.g., "$[")
	if strings.HasPrefix(cleanPath, "$[") {
		// If the input data wraps parallel results, transparently redirect the path
		if _, hasParallelResults := data["parallel_results"]; hasParallelResults {
			cleanPath = strings.Replace(cleanPath, "$[", "parallel_results[", 1)
		}
	}

	segments := strings.Split(cleanPath, ".")

	var current interface{} = data

	for _, segment := range segments {
		arrayIdx := -1
		key := segment

		// Check for array bracket notation
		if startBracket := strings.Index(segment, "["); startBracket != -1 {
			endBracket := strings.Index(segment, "]")
			if endBracket > startBracket {
				idxStr := segment[startBracket+1 : endBracket]
				if parsedIdx, err := strconv.Atoi(idxStr); err == nil {
					arrayIdx = parsedIdx
					key = segment[:startBracket]
				}
			}
		}

		// Navigate into the map
		if mapData, ok := current.(map[string]interface{}); ok {
			var exists bool
			current, exists = mapData[key]
			if !exists {
				return nil, fmt.Errorf("key '%s' not found", key)
			}
		} else {
			return nil, fmt.Errorf("cannot access key '%s' on a non-object data type", key)
		}

		// Extract the array element if an index is specified
		if arrayIdx != -1 {
			if arrayData, ok := current.([]interface{}); ok {
				if arrayIdx >= 0 && arrayIdx < len(arrayData) {
					current = arrayData[arrayIdx]
				} else {
					return nil, fmt.Errorf("index [%d] out of bounds for the array", arrayIdx)
				}
			} else {
				return nil, fmt.Errorf("field '%s' is not an array", key)
			}
		}
	}

	return current, nil
}
