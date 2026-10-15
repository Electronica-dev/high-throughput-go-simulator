// scenario2.go: live limit change 30K -> 5K -> 30K RPM with an untouched
// second model; configured-vs-observed time series as evidence.
package main

import (
	"consuma/internal/scenarios"
)

func cmdScenario2(argv []string) {
	runScenarioN("2", argv, func(systemURL, _ string, sc scenarios.Config, out string) (*scenarios.Result, error) {
		return scenarios.RunScenario2(systemURL, sc, out)
	}, func(sc *scenarios.Config) {
		scenarioDefaults(sc, 30000, 0, 50, "model-a", "model-b")
		sc.Phase1RPM = 30000
		sc.Phase2RPM = 5000
		sc.Phase1Sec = 75
		sc.Phase2Sec = 75
		sc.Phase3Sec = 75
	})
}
