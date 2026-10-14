// simulator.go: the `consuma simulator` subcommand — the standalone provider
// simulator service (rolling limits, latency sleeps, idempotency seen-keys).
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"consuma/internal/config"
	"consuma/internal/simserver"
	"consuma/internal/store"
)

func cmdSimulator(argv []string) {
	fs := flag.NewFlagSet("simulator", flag.ExitOnError)
	cfgPath := fs.String("config", "configs/simulator.yaml", "simulator model config")
	addr := fs.String("addr", "127.0.0.1:9001", "listen address")
	if err := fs.Parse(argv); err != nil {
		os.Exit(2)
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	st, err := store.Open("simulator.db")
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	defer st.Close()

	srv := simserver.New(cfg, st)
	httpSrv := startHTTP(*addr, srv.Handler())
	fmt.Printf("simulator listening on %s (db simulator.db)\n", *addr)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	<-sig
	fmt.Println("shutting down")
	_ = httpSrv.Close()
}
