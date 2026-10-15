// Shared HTTP-server helper for the serve/simulator subcommands.
package main

import "net/http"

// startHTTP runs an http.Server in a goroutine and returns it for shutdown.
func startHTTP(addr string, h http.Handler) *http.Server {
	srv := &http.Server{Addr: addr, Handler: h}
	go func() { _ = srv.ListenAndServe() }()
	return srv
}
