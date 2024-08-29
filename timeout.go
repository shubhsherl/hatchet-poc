package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/cmdutils"
	"github.com/hatchet-dev/hatchet/pkg/worker"
	"github.com/joho/godotenv"
)

// Define a configuration for the workflow steps and their dependencies
type workflowConfig struct {
	Steps []workflowStepConfig
}

type workflowStepConfig struct {
	Name       string
	DependsOn  []string
	Timeout    string
	Executable func(ctx worker.HatchetContext, parentOutputs []*stepOutput) (result *stepOutput, err error)
}

// Define step output
type stepOutput struct {
	Message string
}

func main() {
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	c, err := client.New()
	if err != nil {
		panic(fmt.Sprintf("error creating client: %v", err))
	}

	w, err := worker.NewWorker(
		worker.WithClient(c),
		worker.WithMaxRuns(1),
	)
	if err != nil {
		panic(fmt.Sprintf("error creating worker: %v", err))
	}

	// Sample configuration for a complex DAG workflow with timeouts
	config := workflowConfig{
		Steps: []workflowStepConfig{
			{
				Name:    "step-one",
				Timeout: "5s", // Timeout after 5 seconds
				Executable: func(ctx worker.HatchetContext, parentOutputs []*stepOutput) (result *stepOutput, err error) {
					time.Sleep(2 * time.Second) // Simulate some work
					output := &stepOutput{
						Message: "output from step one",
					}
					fmt.Println(output.Message)
					return output, nil
				},
			},
			{
				Name:      "step-two",
				DependsOn: []string{"step-one"},
				Timeout:   "3s", // Timeout after 3 seconds
				Executable: func(ctx worker.HatchetContext, parentOutputs []*stepOutput) (result *stepOutput, err error) {
					time.Sleep(10 * time.Second) // Simulate timeout
					combinedOutput := combineParentOutputs(parentOutputs)
					output := &stepOutput{
						Message: fmt.Sprintf("step two, parent output: %s", combinedOutput),
					}
					fmt.Println(output.Message)
					return output, nil
				},
			},
			{
				Name:      "step-three",
				DependsOn: []string{"step-one"},
				Timeout:   "2s", // Timeout after 2 seconds
				Executable: func(ctx worker.HatchetContext, parentOutputs []*stepOutput) (result *stepOutput, err error) {
					time.Sleep(1 * time.Second) // Simulate some work
					combinedOutput := combineParentOutputs(parentOutputs)
					output := &stepOutput{
						Message: fmt.Sprintf("step three, parent output: %s", combinedOutput),
					}
					fmt.Println(output.Message)
					return output, nil
				},
			},
			{
				Name:      "step-four",
				DependsOn: []string{"step-two", "step-three"},
				Timeout:   "4s", // Timeout after 4 seconds
				Executable: func(ctx worker.HatchetContext, parentOutputs []*stepOutput) (result *stepOutput, err error) {
					combinedOutput := combineParentOutputs(parentOutputs)
					output := &stepOutput{
						Message: fmt.Sprintf("step four, parent output: %s", combinedOutput),
					}
					fmt.Println(output.Message)
					return output, nil
				},
			},
		},
	}

	// Register the workflow with the DAG structure
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			Name:        "complex-dag-workflow",
			Description: "A complex DAG workflow with multiple dependencies and timeouts.",
			On:          worker.Events("complex-dag-event-v3"),
			Steps:       createWorkflowSteps(config),
		},
	)
	if err != nil {
		panic(fmt.Sprintf("error registering workflow: %v", err))
	}

	interruptCtx, cancel := cmdutils.InterruptContextFromChan(cmdutils.InterruptChan())
	defer cancel()

	cleanup, err := w.Start()
	if err != nil {
		panic(fmt.Sprintf("error starting worker: %v", err))
	}

	go pushEvents(interruptCtx, c)

	<-interruptCtx.Done()
	if err := cleanup(); err != nil {
		panic(err)
	}
}

// Function to dynamically create workflow steps with dependencies and timeouts
func createWorkflowSteps(config workflowConfig) []*worker.WorkflowStep {
	stepMap := make(map[string]*worker.WorkflowStep)

	// Create all steps first
	for _, stepConfig := range config.Steps {
		stepConfig := stepConfig // capture range variable

		step := worker.Fn(func(ctx worker.HatchetContext) (result *stepOutput, err error) {
			parentOutputs := gatherParentOutputs(ctx, stepMap, stepConfig.DependsOn)
			return stepConfig.Executable(ctx, parentOutputs)
		}).
			SetName(stepConfig.Name).
			SetTimeout(stepConfig.Timeout).
			AddParents(stepConfig.DependsOn...)
		stepMap[stepConfig.Name] = step
	}

	// Collect all steps
	var steps []*worker.WorkflowStep
	for _, step := range stepMap {
		steps = append(steps, step)
	}

	return steps
}

// Function to gather parent outputs
func gatherParentOutputs(ctx worker.HatchetContext, stepMap map[string]*worker.WorkflowStep, parentNames []string) []*stepOutput {
	var parentOutputs []*stepOutput
	for _, parentName := range parentNames {
		parentStep := stepMap[parentName]
		if parentStep != nil {
			output := &stepOutput{}
			if err := ctx.StepOutput(parentStep.Name, output); err != nil {
				parentOutputs = append(parentOutputs, &stepOutput{Message: fmt.Sprintf("error getting output from parent step %s: %v", parentStep.Name, err)})
				fmt.Println(err)
			}

			parentOutputs = append(parentOutputs, output)
		}
	}
	return parentOutputs
}

// Function to combine parent outputs into a single string
func combineParentOutputs(parentOutputs []*stepOutput) string {
	var messages []string
	for _, output := range parentOutputs {
		messages = append(messages, output.Message)
	}
	return strings.Join(messages, "; ")
}

// Function to push events, with a 5-second delay before triggering the workflow
func pushEvents(ctx context.Context, c client.Client) {
	time.Sleep(5 * time.Second) // Delay for 5 seconds

	c.Event().Push(
		ctx,
		"complex-dag-event-v3",
		map[string]interface{}{
			"message": "hello complex DAG workflow",
		},
	)
}
