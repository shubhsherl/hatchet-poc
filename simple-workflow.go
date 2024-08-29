package main

import (
	"context"
	"fmt"
	"github.com/hatchet-dev/hatchet/pkg/client"
	"github.com/hatchet-dev/hatchet/pkg/cmdutils"
	"github.com/hatchet-dev/hatchet/pkg/worker"
	"time"

	"github.com/joho/godotenv"
)

// Define a configuration for the workflow steps
type workflowConfig struct {
	Steps []string
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

	// Sample configuration with step names
	config := workflowConfig{
		Steps: []string{"step-one", "step-two", "step-three"},
	}

	// Dynamically create workflow steps based on the configuration
	var workflowSteps []*worker.WorkflowStep
	for _, stepName := range config.Steps {
		stepName := stepName // capture range variable
		workflowSteps = append(workflowSteps, worker.Fn(func(ctx worker.HatchetContext) (result *stepOutput, err error) {
			fmt.Printf("executed %s\n", stepName)
			return &stepOutput{}, nil
		}).SetName(stepName))
	}

	// Register the workflow with dynamically created steps
	err = w.RegisterWorkflow(
		&worker.WorkflowJob{
			Name:        "dynamic-workflow",
			Description: "Workflow with dynamic steps from config.",
			On:          worker.Events("dynamic-event"),
			Steps:       workflowSteps,
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

// Function to push events, with a 5-second delay before triggering the workflow
func pushEvents(ctx context.Context, c client.Client) {
	time.Sleep(5 * time.Second) // Delay for 5 seconds

	c.Event().Push(
		ctx,
		"dynamic-event",
		map[string]interface{}{
			"message": "hello dynamic workflow",
		},
	)
}
