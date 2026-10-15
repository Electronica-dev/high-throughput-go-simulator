// scenario3.go: durable 10K batches with callbacks — managed reject-first
// sink (or -callback URL), delivery recovery, id uniqueness, callback/API
// status agreement.
package main

import (
	"htsim/internal/scenarios"
)

func cmdScenario3(argv []string) {
	runScenarioN("3", argv, scenarios.RunScenario3, func(sc *scenarios.Config) {
		scenarioDefaults(sc, 0, 0, 0, "model-a", "model-b")
		sc.BatchCount = 10000
		if sc.CallbackFails == 0 {
			sc.CallbackFails = 2
		}
	})
}
