// scenario2.go: live limit change 30K -> 5K -> 30K RPM with an untouched
// second model; configured-vs-observed time series as evidence.
package main

import (
	"htsim/internal/scenarios"
)

func cmdScenario2(argv []string) {
	runScenarioN("2", argv, func(systemURL, _ string, sc scenarios.Config, out string) (*scenarios.Result, error) {
		return scenarios.RunScenario2(systemURL, sc, out)
	}, func(sc *scenarios.Config) {
		scenarioDefaults(sc, 30000, 0, 50, "model-a", "model-b")
		sc.Phase1RPM = 30000
		sc.Phase1TPM = 60000000 // spec: model A 30K RPM / 60M TPM
		sc.Phase2RPM = 5000
		sc.SecondRPM = 20000 // spec: model B 20K RPM / 40M TPM, unchanged all run
		sc.SecondTPM = 40000000
		sc.Tokens = 2000 // 2000-token requests make the TPM limit the binding constraint at 94% of 60M
		sc.Phase1Sec = 75
		sc.Phase2Sec = 75
		sc.Phase3Sec = 75
	})
}
