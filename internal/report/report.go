// Package report renders the end-of-run report with MEASURED numbers in one
// section and projections/assumptions in another, never mixed.
package report

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"consuma/internal/config"
	"consuma/internal/engine"
)

// LimitsRow is one model's configured-vs-observed limits.
type LimitsRow struct {
	Model         string  `json:"model"`
	ConfiguredRPM int64   `json:"configured_rpm"`
	ObservedRPM   int64   `json:"observed_rpm"`
	ConfiguredTPM int64   `json:"configured_tpm"`
	ObservedTPM   int64   `json:"observed_tpm"`
	AdmittedTPM   int64   `json:"admitted_tpm"`
	ProviderRPM   int64   `json:"provider_observed_rpm"`
	ProviderTPM   int64   `json:"provider_observed_tpm"`
	Latency       Latency `json:"-"`
}

// Latency holds end-to-end percentiles in ms for one model.
type Latency struct {
	P50MS float64 `json:"p50_ms"`
	P95MS float64 `json:"p95_ms"`
	P99MS float64 `json:"p99_ms"`
}

// measured is everything that was actually observed in the run.
type measured struct {
	Run struct {
		Rate     int64         `json:"rate_per_sec"`
		Duration time.Duration `json:"duration"`
	} `json:"run"`
	Counts struct {
		Submitted int64 `json:"submitted"`
		Completed int64 `json:"completed"`
		OK        int64 `json:"successful"`
		Failed    int64 `json:"failed"`
		Expired   int64 `json:"expired"`
		Rejected  int64 `json:"rejected"`
	} `json:"counts"`
	Rates struct {
		SteadyStatePerSec float64 `json:"steady_state_per_sec"`
		WholeRunAvgPerSec float64 `json:"whole_run_avg_per_sec"`
		DrainSec          float64 `json:"drain_sec"`
	} `json:"rates"`
	Latency map[string]Latency  `json:"latency_ms"`
	Limits  []LimitsRow         `json:"limits"`
	Extra   map[string]int64    `json:"extra_counters,omitempty"`
	summary *engine.Summary     `json:"-"`
}

// Report is the full report: measured + labeled projections.
type Report struct {
	Measured    measured `json:"measured"`
	Projections []string `json:"projections"`
}

// Build assembles a Report from a drained engine.
func Build(s engine.Summary, eng *engine.Engine, cfg *config.Config) *Report {
	r := &Report{}
	r.Measured.Run.Duration = 0 // caller may set via WithRun
	r.Measured.Counts.Submitted = s.Submitted
	r.Measured.Counts.Completed = s.Completed
	r.Measured.Counts.OK = s.OK
	r.Measured.Counts.Failed = s.Failed
	r.Measured.Counts.Expired = s.Expired
	r.Measured.Counts.Rejected = s.Rejected
	r.Measured.Rates.SteadyStatePerSec = s.SteadyStatePerSec
	r.Measured.Rates.WholeRunAvgPerSec = s.WholeRunAvgPerSec
	r.Measured.Rates.DrainSec = s.DrainSec

	r.Measured.Latency = map[string]Latency{}
	now := time.Now()
	for i := range cfg.Models {
		m := &cfg.Models[i]
		row := LimitsRow{Model: m.Name, ConfiguredRPM: m.RPM, ConfiguredTPM: m.TPM}
		if lim := eng.Limiter(m.Name); lim != nil {
			row.ObservedRPM = lim.ObservedRPM(now)
			row.ObservedTPM = lim.ObservedTPM(now)
			row.AdmittedTPM = lim.AdmittedTPM()
		}
		if pv := eng.Provider(m.Name); pv != nil {
			row.ProviderRPM = pv.ObservedRPM(now)
			row.ProviderTPM = pv.ObservedTPM(now)
		}
		if p, ok := s.Percentiles[m.Name+"/p50"]; ok {
			row.Latency = Latency{
				P50MS: p,
				P95MS: s.Percentiles[m.Name+"/p95"],
				P99MS: s.Percentiles[m.Name+"/p99"],
			}
			r.Measured.Latency[m.Name] = row.Latency
		}
		r.Measured.Limits = append(r.Measured.Limits, row)
	}
	r.Measured.Extra = eng.Metrics().Snapshot()

	// Projections and assumptions are LABELLED separately from measurements.
	// The first projection is always the scope caveat (spec requirement).
	r.Projections = append(r.Projections,
		"assumption: benchmark numbers are achieved with the in-process pipeline; "+
			"a per-request hop over localhost HTTP caps throughput at roughly 100-200K req/sec "+
			"(tuned round-trip cost ~5-10us CPU per side plus queuing), below the required 300K/sec bar regardless of language")
	r.Projections = append(r.Projections,
		"assumption: provider responses are simulated on a timer wheel (configurable latency/outcome mix), not real LLM calls")
	return r
}

// WithRun records the requested load parameters.
func (r *Report) WithRun(rate int64, d time.Duration) *Report {
	r.Measured.Run.Rate = rate
	r.Measured.Run.Duration = d
	return r
}

// WriteJSON renders the report as JSON with measured/projections separation.
func WriteJSON(w io.Writer, rep *Report) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(rep)
}

// WriteText renders a human-readable report with section separation.
func WriteText(w io.Writer, rep *Report) error {
	m := rep.Measured
	fmt.Fprintln(w, "=== MEASURED ===")
	fmt.Fprintf(w, "run: rate=%d/sec duration=%s\n", m.Run.Rate, m.Run.Duration)
	fmt.Fprintf(w, "counts: submitted=%d completed=%d ok=%d failed=%d expired=%d rejected=%d\n",
		m.Counts.Submitted, m.Counts.Completed, m.Counts.OK, m.Counts.Failed, m.Counts.Expired, m.Counts.Rejected)
	fmt.Fprintf(w, "rates: steady-state=%.0f/sec whole-run-avg=%.0f/sec drain=%.2fs\n",
		m.Rates.SteadyStatePerSec, m.Rates.WholeRunAvgPerSec, m.Rates.DrainSec)
	for _, row := range m.Limits {
		fmt.Fprintf(w, "limits[%s]: configured rpm=%d tpm=%d | observed rpm=%d tpm=%d | provider rpm=%d tpm=%d\n",
			row.Model, row.ConfiguredRPM, row.ConfiguredTPM, row.ObservedRPM, row.ObservedTPM, row.ProviderRPM, row.ProviderTPM)
		if l, ok := m.Latency[row.Model]; ok {
			fmt.Fprintf(w, "latency[%s]: p50=%.1fms p95=%.1fms p99=%.1fms\n", row.Model, l.P50MS, l.P95MS, l.P99MS)
		}
	}
	fmt.Fprintln(w, "=== PROJECTIONS (not measurements) ===")
	for _, p := range rep.Projections {
		fmt.Fprintln(w, "-", p)
	}
	return nil
}
