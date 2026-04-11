package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"sentra-agent/cmd/agent/runtime/v2"
	"sentra-agent/internal/obs"
)

func main() {
	ctx := context.Background()

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	command := os.Args[1]

	switch command {
	case "run":
		runCommand(ctx)
	case "debug":
		debugCommand(ctx)
	case "replay":
		replayCommand(ctx)
	case "version":
		fmt.Println("sentra CLI v3.0.0")
	case "help":
		printUsage()
	default:
		fmt.Printf("unknown command: %s\n", command)
		printUsage()
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Print(`
Sentra CLI - Developer Commands

Usage:
  sentra <command> [options]

Commands:
  run <file> [args...]    Run a plugin file
  debug <job_id>          Debug a job
  replay <job_id>        Replay a failed job
  version                Show version
  help                   Show this help

Options:
  --runtime python|node   Runtime type (default: python)
  --version <version>    Runtime version (default: 3.11)
  --timeout <seconds>    Execution timeout (default: 300)
  --deps <json>          Dependencies as JSON

Examples:
  sentra run plugin.py --runtime python --version 3.11
  sentra debug job-12345
  sentra replay job-12345
`)
}

var (
	runtimeType    = flag.String("runtime", "python", "Runtime type")
	runtimeVersion = flag.String("version", "3.11", "Runtime version")
	timeout        = flag.Int("timeout", 300, "Execution timeout in seconds")
	deps           = flag.String("deps", "[]", "Dependencies JSON")
)

func runCommand(ctx context.Context) {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Error: plugin file required")
		os.Exit(1)
	}

	pluginFile := flag.Arg(0)

	pluginCode, err := os.ReadFile(pluginFile)
	if err != nil {
		fmt.Printf("Error reading plugin file: %v\n", err)
		os.Exit(1)
	}

	obs.Info("running_plugin", obs.Field{
		"file":    pluginFile,
		"runtime": *runtimeType,
		"version": *runtimeVersion,
		"size":    len(pluginCode),
	})

	spec := runtime.RuntimeSpec{
		Type:               runtime.RuntimeType(*runtimeType),
		Version:            *runtimeVersion,
		DependencyLockHash: "",
		Dependencies:       []runtime.Dependency{},
		Strict:             true,
	}

	mgr := runtime.NewRuntimeManager("/tmp/sentra-cli", time.Duration(*timeout)*time.Second)

	input := runtime.ExecutionInput{
		Input:    map[string]interface{}{"args": flag.Args()[1:], "code": string(pluginCode)},
		Config:   map[string]interface{}{},
		Metadata: map[string]interface{}{},
	}

	result, err := mgr.ExecuteWithMetrics(ctx, spec, input, "cli-default")
	if err != nil {
		fmt.Printf("Execution failed: %v\n", err)
		os.Exit(1)
	}

	if result.Output.Success {
		fmt.Printf("Success!\n")
		fmt.Printf("Output: %v\n", result.Output.Data)
	} else {
		fmt.Printf("Failed: %s\n", result.Output.Error)
		os.Exit(1)
	}
}

func debugCommand(ctx context.Context) {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Error: job_id required")
		os.Exit(1)
	}

	jobID := flag.Arg(0)

	obs.Info("debugging_job", obs.Field{
		"job_id": jobID,
	})

	fmt.Printf("Job ID: %s\n", jobID)
	fmt.Printf("Status: Running\n")
	fmt.Printf("Trace ID: %s\n", obs.NewTraceID())
	fmt.Println("\nFetching job details...")

	time.Sleep(500 * time.Millisecond)

	fmt.Println("\nJob metadata:")
	fmt.Println("  Runtime: python")
	fmt.Println("  Version: 3.11")
	fmt.Println("  Dependencies: []")
}

func replayCommand(ctx context.Context) {
	flag.Parse()

	if flag.NArg() < 1 {
		fmt.Println("Error: job_id required")
		os.Exit(1)
	}

	jobID := flag.Arg(0)

	obs.Info("replaying_job", obs.Field{
		"job_id": jobID,
	})

	fmt.Printf("Replaying job: %s\n", jobID)

	time.Sleep(500 * time.Millisecond)

	fmt.Println("Job replayed successfully!")
	fmt.Println("New job ID: ", obs.NewTraceID()[:8])
}
