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
	Executable func(ctx worker.HatchetContext) (result *stepOutput, err error)
}

// Define step output
type stepOutput struct{}

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

	// Sample configuration for a complex DAG workflow
	config := workflowConfig{
		Steps: []workflowStepConfig{
			{
				Name: "step-one",
				Executable: func(ctx worker.HatchetContext) (result *stepOutput, err error) {
					fmt.Println("executed step one")
					return &stepOutput{}, nil
				},
			},
			{
				Name:      "step-two",
				DependsOn: []string{"step-one"},
				Executable: func(ctx worker.HatchetContext) (result *stepOutput, err error) {
					fmt.Println("executed step two")
					return &stepOutput{}, nil
				},
			},
			{
				Name:      "step-three",
				DependsOn: []string{"step-one"},
				Executable: func(ctx worker.HatchetContext) (result *stepOutput, err error) {
					fmt.Println("executed step three")
					return &stepOutput{}, nil
				},
			},
			{
				Name:      "step-four",
				DependsOn: []string{"step-two", "step-three"},
				Executable: func(ctx worker.HatchetContext) (result *stepOutput, err error) {
					fmt.Println("executed step four")
					return &stepOutput{}, nil
				},
			},
		},
	}

	// Register the workflow with the DAG structure
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			Name:        "complex-dag-workflow",
			Description: "A complex DAG workflow with multiple dependencies.",
			On:          worker.Events("complex-dag-event"),
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

// Function to dynamically create workflow steps with dependencies
func createWorkflowSteps(config workflowConfig) []*worker.WorkflowStep {
	stepMap := make(map[string]*worker.WorkflowStep)

	// Create all steps first
	for _, stepConfig := range config.Steps {
		step := worker.Fn(stepConfig.Executable).
			SetName(stepConfig.Name).
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

// Function to push events, with a 5-second delay before triggering the workflow
func pushEvents(ctx context.Context, c client.Client) {
	time.Sleep(5 * time.Second) // Delay for 5 seconds

	c.Event().Push(
		ctx,
		"complex-dag-event",
		map[string]interface{}{
			"message": "hello complex DAG workflow",
		},
	)
}
