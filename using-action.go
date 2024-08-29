package main

import (
	"context"
	"fmt"
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

type stepOutput struct {
	Message string
}

func main() {
	// Load environment variables
	err := godotenv.Load()
	if err != nil {
		panic(err)
	}

	// Initialize the Hatchet client
	c, err := client.New()
	if err != nil {
		panic(fmt.Sprintf("error creating client: %v", err))
	}

	// Create a new worker instance
	w, err := worker.NewWorker(
		worker.WithClient(c),
		worker.WithMaxRuns(1),
	)
	if err != nil {
		panic(fmt.Sprintf("error creating worker: %v", err))
	}

	// Register predefined actions using RegisterAction
	err = w.RegisterAction("s:action-one", actionOne)
	if err != nil {
		panic(fmt.Sprintf("error registering action-one: %v", err))
	}

	err = w.RegisterAction("s:action-two", actionTwo)
	if err != nil {
		panic(fmt.Sprintf("error registering action-two: %v", err))
	}

	err = w.RegisterAction("s:action-three", actionThree)
	if err != nil {
		panic(fmt.Sprintf("error registering action-three: %v", err))
	}

	err = w.RegisterAction("s:action-four", actionFour)
	if err != nil {
		panic(fmt.Sprintf("error registering action-four: %v", err))
	}

	// Define the DAG workflow configuration using the registered actions
	config := workflowConfig{
		Steps: []workflowStepConfig{
			{
				Name:    "action-one",
				Timeout: "5s", // Timeout after 5 seconds
			},
			{
				Name:      "action-two",
				DependsOn: []string{"action-one"},
				Timeout:   "3s", // Timeout after 3 seconds
			},
			{
				Name:      "action-three",
				DependsOn: []string{"action-one"},
				Timeout:   "2s", // Timeout after 2 seconds
			},
			{
				Name:      "action-four",
				DependsOn: []string{"action-two", "action-three"},
				Timeout:   "4s", // Timeout after 4 seconds
			},
		},
	}

	// Register the workflow with the DAG structure
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			Name:        "complex-dag-workflow-v1",
			Description: "A complex DAG workflow with multiple dependencies using predefined actions.",
			On:          worker.Events("complex-dag-event-v4"),
			Steps:       createWorkflowSteps(w, config),
		},
	)
	if err != nil {
		panic(fmt.Sprintf("error registering workflow: %v", err))
	}

	// Start the worker
	interruptCtx, cancel := cmdutils.InterruptContextFromChan(cmdutils.InterruptChan())
	defer cancel()

	cleanup, err := w.Start()
	if err != nil {
		panic(fmt.Sprintf("error starting worker: %v", err))
	}

	// Push events to trigger the workflow
	go pushEvents(interruptCtx, c)

	<-interruptCtx.Done()
	if err := cleanup(); err != nil {
		panic(err)
	}
}

// Action functions that use context to pass data between steps
func actionOne(ctx worker.HatchetContext) (*stepOutput, error) {
	time.Sleep(2 * time.Second) // Simulate some work
	input := &stepOutput{}
	err := ctx.WorkflowInput(input)
	if err != nil {
		return nil, err
	}

	output := &stepOutput{
		Message: "output from action one; " + input.Message + " ; " + ctx.StepName() + " ; " + ctx.StepRunId() + " ; " + ctx.WorkflowRunId(),
	}

	// Store output in context
	ctx.SetContext(context.WithValue(ctx.GetContext(), "action-one-output", output))
	return output, nil
}

func actionTwo(ctx worker.HatchetContext) (*stepOutput, error) {
	time.Sleep(1 * time.Second) // Simulate some work
	input := &stepOutput{}
	err := ctx.WorkflowInput(input)
	if err != nil {
		return nil, err
	}

	output := &stepOutput{
		Message: "output from action two; " + input.Message + " ; " + ctx.StepName() + " ; " + ctx.StepRunId() + " ; " + ctx.WorkflowRunId(),
	}
	fmt.Println(output.Message)
	// Store output in context
	ctx.SetContext(context.WithValue(ctx.GetContext(), "action-two-output", output))
	return output, nil
}

func actionThree(ctx worker.HatchetContext) (*stepOutput, error) {
	time.Sleep(1 * time.Second) // Simulate some work
	// Retrieve output from action one
	input := ctx.GetContext().Value("action-one-output").(*stepOutput)
	output := &stepOutput{
		Message: "output from action three; " + input.Message + " ; " + ctx.StepName() + " ; " + ctx.StepRunId() + " ; " + ctx.WorkflowRunId(),
	}
	fmt.Println(output.Message)
	// Store output in context
	ctx.SetContext(context.WithValue(ctx.GetContext(), "action-three-output", output))
	return output, nil
}

func actionFour(ctx worker.HatchetContext) (*stepOutput, error) {
	time.Sleep(1 * time.Second) // Simulate some work
	// Retrieve outputs from action two and three
	output := &stepOutput{
		Message: "output from action four; " + ctx.StepName() + " ; " + ctx.StepRunId() + " ; " + ctx.WorkflowRunId(),
	}
	fmt.Println(output.Message)
	return output, nil
}

// Function to push events, with a 5-second delay before triggering the workflow
func pushEvents(ctx context.Context, c client.Client) {
	time.Sleep(5 * time.Second) // Delay for 5 seconds

	c.Event().Push(
		ctx,
		"complex-dag-event-v4",
		map[string]interface{}{
			"message": "hello complex DAG workflow",
		},
	)
}

// Function to dynamically create workflow steps with dependencies and timeouts
func createWorkflowSteps(w *worker.Worker, config workflowConfig) []*worker.WorkflowStep {
	stepMap := make(map[string]*worker.WorkflowStep)

	// Create all steps first
	for _, stepConfig := range config.Steps {
		stepConfig := stepConfig // Capture range variable

		step := w.Call("s:" + stepConfig.Name).
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
