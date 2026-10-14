// Package config loads and validates simulation configuration.
package config

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// LatencyConfig describes how the provider simulator draws response latency.
type LatencyConfig struct {
	Kind   string  `yaml:"kind"`
	MeanMS float64 `yaml:"mean_ms"`
	MinMS  float64 `yaml:"min_ms"`
	MaxMS  float64 `yaml:"max_ms"`
	Sigma  float64 `yaml:"sigma"`
}

// Draw returns a latency sample for one simulated provider call.
func (l LatencyConfig) Draw(rng *rand.Rand) time.Duration {
	var ms float64
	switch l.Kind {
	case "fixed":
		ms = l.MeanMS
	case "uniform":
		ms = l.MinMS + rng.Float64()*(l.MaxMS-l.MinMS)
	case "lognormal":
		// Sigma is the sigma of the underlying normal; MeanMS is the median.
		ms = math.Exp(math.Log(l.MeanMS) + rng.NormFloat64()*l.Sigma)
	default:
		ms = l.MeanMS
	}
	return time.Duration(ms * float64(time.Millisecond))
}

// MaxMS returns the largest latency this config can plausibly draw (used for
// horizon validation; lognormal capped at 6 sigma above the median).
func (l LatencyConfig) maxMS() float64 {
	switch l.Kind {
	case "fixed":
		return l.MeanMS
	case "uniform":
		return l.MaxMS
	case "lognormal":
		return math.Exp(math.Log(l.MeanMS) + 6*l.Sigma)
	default:
		return math.Inf(1)
	}
}

// OutcomeConfig is the per-call outcome mix; percentages must sum to 100.
type OutcomeConfig struct {
	SuccessPct    float64 `yaml:"success_pct"`
	TransientPct  float64 `yaml:"transient_pct"`
	PermanentPct  float64 `yaml:"permanent_pct"`
}

// ModelConfig is one simulated model and its limits.
type ModelConfig struct {
	Name            string        `yaml:"name"`
	RPM             int64         `yaml:"rpm"`
	TPM             int64         `yaml:"tpm"`
	Latency         LatencyConfig `yaml:"latency"`
	EstimateTokens  int64         `yaml:"estimate_tokens"`
	ActualTokenMean float64       `yaml:"actual_token_mean"`
	Outcome         OutcomeConfig `yaml:"outcome"`
}

// Config is the top-level simulation configuration.
type Config struct {
	Models       []ModelConfig `yaml:"models"`
	MaxWaitMS    int64         `yaml:"max_wait_ms"`
	QueueDepth   int           `yaml:"queue_depth"`
	WheelShards  int           `yaml:"wheel_shards"`
	WheelBuckets int           `yaml:"wheel_buckets"`
	WheelTickMS  int64         `yaml:"wheel_tick_ms"`
	MaxRetries   int           `yaml:"max_retries"`
}

// Load reads and unmarshals a YAML config file.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("config: read %s: %w", path, err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("config: parse %s: %w", path, err)
	}
	// Fail fast at startup: validate against the wheel horizon the config
	// itself declares. Callers that build a different horizon call Validate
	// again with their own value.
	horizon := time.Duration(cfg.WheelBuckets) * time.Duration(cfg.WheelTickMS) * time.Millisecond
	if err := cfg.Validate(horizon); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// Validate checks limits, outcome mixes, latency kinds, and that no model's
// worst-case latency exceeds the wheel horizon.
func (c *Config) Validate(horizon time.Duration) error {
	if len(c.Models) == 0 {
		return fmt.Errorf("config: no models defined")
	}
	seen := map[string]bool{}
	for i := range c.Models {
		m := &c.Models[i]
		if m.Name == "" {
			return fmt.Errorf("config: model %d has no name", i)
		}
		if seen[m.Name] {
			return fmt.Errorf("config: duplicate model name %q", m.Name)
		}
		seen[m.Name] = true
		if m.RPM <= 0 || m.TPM <= 0 {
			return fmt.Errorf("config: model %s must have rpm>0 and tpm>0", m.Name)
		}
		if m.EstimateTokens <= 0 {
			return fmt.Errorf("config: model %s must have estimate_tokens>0", m.Name)
		}
		sum := m.Outcome.SuccessPct + m.Outcome.TransientPct + m.Outcome.PermanentPct
		if math.Abs(sum-100) > 0.01 {
			return fmt.Errorf("config: model %s outcome pcts sum to %.2f, want 100", m.Name, sum)
		}
		switch m.Latency.Kind {
		case "fixed":
			if m.Latency.MeanMS <= 0 {
				return fmt.Errorf("config: model %s fixed latency needs mean_ms>0", m.Name)
			}
		case "uniform":
			if m.Latency.MinMS < 0 || m.Latency.MaxMS < m.Latency.MinMS {
				return fmt.Errorf("config: model %s uniform latency needs 0<=min_ms<=max_ms", m.Name)
			}
		case "lognormal":
			if m.Latency.MeanMS <= 0 || m.Latency.Sigma <= 0 {
				return fmt.Errorf("config: model %s lognormal latency needs mean_ms>0 and sigma>0", m.Name)
			}
		default:
			return fmt.Errorf("config: model %s has unknown latency kind %q", m.Name, m.Latency.Kind)
		}
		if max := m.Latency.maxMS(); max > float64(horizon)/float64(time.Millisecond) {
			return fmt.Errorf("config: model %s latency (max %.0fms) exceeds wheel horizon (%v)", m.Name, max, horizon)
		}
	}
	return nil
}

// Model returns the named model or nil.
func Model(cfg *Config, name string) *ModelConfig {
	for i := range cfg.Models {
		if cfg.Models[i].Name == name {
			return &cfg.Models[i]
		}
	}
	return nil
}
