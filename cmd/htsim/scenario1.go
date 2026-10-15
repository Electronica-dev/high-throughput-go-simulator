// scenario1.go: sustained load at the configured limit — utilization,
// window enforcement, request accounting.
package main

import (
	"htsim/internal/scenarios"
)

func cmdScenario1(argv []string) {
	runScenarioN("1", argv, func(systemURL, _ string, sc scenarios.Config, out string) (*scenarios.Result, error) {
		return scenarios.RunScenario1(systemURL, sc, out)
	}, func(sc *scenarios.Config) {
		scenarioDefaults(sc, 465, 300, 50, "model-a", "")
		sc.WarmupSec = 60
	})
}
