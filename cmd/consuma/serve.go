// serve.go: the `consuma serve` subcommand — the real-mode system: engine +
// batch manager + HTTP API (sync requests, 202 batches, live limits) with
// crash recovery at startup and graceful shutdown on SIGINT/SIGTERM.
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"consuma/internal/api"
	"consuma/internal/batch"
	"consuma/internal/config"
	"consuma/internal/engine"
	"consuma/internal/httpprovider"
	"consuma/internal/ledger"
	"consuma/internal/store"
)

// systemDeps adapts engine + batch manager to api.Deps.
type systemDeps struct {
	eng *engine.Engine
	mgr *batch.Manager
}

func (s *systemDeps) SubmitAndWait(r *ledger.Request, timeout time.Duration) (string, int64, error) {
	return s.eng.SubmitAndWait(r, timeout)
}

func (s *systemDeps) Submit(r *ledger.Request) { s.eng.Submit(r) }

func (s *systemDeps) SetLimit(model string, rpm, tpm int64) error {
	return s.eng.SetLimit(model, rpm, tpm)
}

func (s *systemDeps) Limits(model string) (int64, int64, error) {
	l := s.eng.Limiter(model)
	if l == nil {
		return 0, 0, fmt.Errorf("unknown model %q", model)
	}
	rpm, tpm := l.Limits()
	return rpm, tpm, nil
}

func (s *systemDeps) CreateBatch(model string, tokens, count int64, callbackURL string) (string, error) {
	return s.mgr.Submit(model, tokens, count, callbackURL)
}

func (s *systemDeps) ObservedWindows(model string) (api.WindowSample, error) {
	l := s.eng.Limiter(model)
	if l == nil {
		return api.WindowSample{}, fmt.Errorf("unknown model %q", model)
	}
	now := time.Now()
	cfgRPM, cfgTPM := l.Limits()
	return api.WindowSample{
		ObservedRPM:   l.ObservedRPM(now),
		ObservedTPM:   l.ObservedTPM(now),
		ConfiguredRPM: cfgRPM,
		ConfiguredTPM: cfgTPM,
	}, nil
}

func (s *systemDeps) BatchStatus(id string) (api.BatchStatus, bool) {
	b, ok := s.mgr.Status(id)
	if !ok {
		return api.BatchStatus{}, false
	}
	return api.BatchStatus{
		ID: b.ID, Status: b.Status,
		Successful: b.Counts.Successful, Failed: b.Counts.Failed,
		Expired: b.Counts.Expired, Rejected: b.Counts.Rejected, Total: b.Counts.Total,
	}, true
}

func (s *systemDeps) BatchResults(id string) ([]api.ResultRow, bool) {
	rows, ok := s.mgr.Results(id)
	if !ok {
		return nil, false
	}
	return rows, true
}

func cmdServe(argv []string) {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("config", "configs/system.yaml", "system config")
	addr := fs.String("addr", "127.0.0.1:9000", "listen address")
	simURL := fs.String("simulator", "http://127.0.0.1:9001", "simulator service URL")
	dbPath := fs.String("db", "consuma.db", "SQLite database path")
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	st, err := store.Open(*dbPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer st.Close()

	prov := httpprovider.New(*simURL, nil)
	eng, err := engine.NewReal(cfg, prov, st)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	mgr := batch.New(st, eng, cfg.Callbacks)
	deps := &systemDeps{eng: eng, mgr: mgr}

	names := make([]string, len(cfg.Models))
	for i, m := range cfg.Models {
		names[i] = m.Name
	}
	apiSrv := startHTTP(*addr, api.New(deps, names).Handler())
	fmt.Printf("consuma serve listening on %s (simulator %s, db %s)\n", *addr, *simURL, *dbPath)

	// crash recovery: re-register mid-flight batches FIRST (their batchState
	// must exist before the recovered requests start completing — Recover
	// re-enqueues while Restore is still reading the DB, and every terminal
	// that lands before registration is a lost count), then re-enqueue.
	if err := mgr.Restore(); err != nil {
		fmt.Fprintln(os.Stderr, "batch restore error:", err)
	}
	n, err := eng.Recover()
	if err != nil {
		fmt.Fprintln(os.Stderr, "recovery error:", err)
		os.Exit(1)
	}
	if n > 0 {
		fmt.Printf("recovered %d in-flight request(s) from the last run\n", n)
	}
	if err := mgr.Resume(); err != nil {
		fmt.Fprintln(os.Stderr, "callback resume error:", err)
	}

	// graceful shutdown: stop accepting, drain, flush, close
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down: draining in-flight work")
	apiSrv.Close() // stop accepting new HTTP work
	_ = eng.Drain()
	fmt.Println("drained; closing")
	eng.Stop()
}
