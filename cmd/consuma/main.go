// Command consuma is the entry point that wires the simulation packages
// together. Subcommands:
//
//	consuma benchmark -config <yaml> -rate N -duration 60s [...]
//	consuma limits    -config <yaml> -model <name> -rpm N -tpm N
package main

import (
	"fmt"
	"os"
	"time"

	"consuma/internal/engine"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "benchmark":
		cmdBenchmark(os.Args[2:])
	case "limits":
		cmdLimits(os.Args[2:])
	case "simulator":
		cmdSimulator(os.Args[2:])
	case "serve":
		cmdServe(os.Args[2:])
	case "scenario1":
		cmdScenario1(os.Args[2:])
	case "scenario2":
		cmdScenario2(os.Args[2:])
	case "scenario3":
		cmdScenario3(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `usage:
  consuma benchmark  -config <yaml> -rate N -duration 60s [-models name:share,...] [-batch N] [-tokens N] [-report out.json] [-target URL]
  consuma limits     -config <yaml> -model <name> -rpm N -tpm N
  consuma serve      [-config configs/system.yaml] [-addr 127.0.0.1:9000] [-simulator URL] [-db path]
  consuma simulator  [-config configs/simulator.yaml] [-addr 127.0.0.1:9001]
  consuma scenario1|2|3 [-system URL] [-simulator URL] [-out dir] [overrides]`)
}

// validateRunArgs rejects nonsensical run parameters before any engine starts.
func validateRunArgs(rate int64, duration time.Duration) error {
	if rate <= 0 {
		return fmt.Errorf("rate must be > 0")
	}
	if duration <= 0 {
		return fmt.Errorf("duration must be > 0")
	}
	return nil
}

// newEngineFromConfig loads a config file and builds a started engine.
func newEngineFromConfig(path string) (*engine.Engine, error) {
	cfg, err := loadConfigFile(path)
	if err != nil {
		return nil, err
	}
	return engine.New(cfg)
}
