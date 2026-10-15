package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestCheckReachable: the scenario pre-flight must accept any HTTP response
// (even 404 — the simulator has no /health) and reject a dead URL with an
// actionable error.
func TestCheckReachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	defer srv.Close()
	if err := checkReachable(srv.URL, "system"); err != nil {
		t.Fatalf("reachable server must pass: %v", err)
	}
	err := checkReachable("http://127.0.0.1:1", "system")
	if err == nil {
		t.Fatal("dead URL must fail")
	}
	if !strings.Contains(err.Error(), "not reachable") {
		t.Fatalf("error should say what happened: %v", err)
	}
}
