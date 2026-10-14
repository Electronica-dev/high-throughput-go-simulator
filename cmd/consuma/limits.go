// limits.go: the `consuma limits` subcommand (live limit change via config
// file rewrite) and the config file watcher the benchmark uses to pick up
// limit changes without a restart.
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"consuma/internal/config"
	"consuma/internal/engine"
)

func cmdLimits(argv []string) {
	fs := flag.NewFlagSet("limits", flag.ExitOnError)
	cfgPath := fs.String("config", "configs/benchmark.yaml", "config file to update")
	model := fs.String("model", "", "model name to change")
	rpm := fs.Int64("rpm", 0, "new RPM limit")
	tpm := fs.Int64("tpm", 0, "new TPM limit")
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}
	if *model == "" || *rpm <= 0 || *tpm <= 0 {
		fmt.Fprintln(os.Stderr, "error: -model, -rpm and -tpm are all required")
		os.Exit(2)
	}
	if err := applyLimitToFile(*cfgPath, *model, *rpm, *tpm); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	fmt.Printf("updated %s: %s rpm=%d tpm=%d\n", *cfgPath, *model, *rpm, *tpm)
}

// applyLimitToFile loads the config, sets the model's limits, and writes the
// file back atomically (tmp + rename) so the running benchmark's watcher sees
// a complete file.
func applyLimitToFile(path, model string, rpm, tpm int64) error {
	cfg, err := loadConfigFile(path)
	if err != nil {
		return err
	}
	m := config.Model(cfg, model)
	if m == nil {
		return fmt.Errorf("applyLimitToFile: unknown model %q", model)
	}
	m.RPM = rpm
	m.TPM = tpm
	out, err := marshalConfig(cfg)
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, out, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// watchConfig polls the config file's mtime; on change it reloads and applies
// every model's limits to the live engine. Runs until stop closes.
func watchConfig(path string, eng *engine.Engine, stop <-chan struct{}) {
	var lastMod int64
	if st, err := os.Stat(path); err == nil {
		lastMod = st.ModTime().UnixNano()
	}
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			st, err := os.Stat(path)
			if err != nil || st.ModTime().UnixNano() == lastMod {
				continue
			}
			lastMod = st.ModTime().UnixNano()
			cfg, err := loadConfigFile(path)
			if err != nil {
				continue // malformed intermediate write; skip until valid
			}
			for i := range cfg.Models {
				m := &cfg.Models[i]
				eng.SetLimit(m.Name, m.RPM, m.TPM)
			}
		}
	}
}
