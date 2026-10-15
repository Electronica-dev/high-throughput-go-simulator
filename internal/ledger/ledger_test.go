package ledger

import (
	"sync"
	"testing"
)

func TestSubmitIncrements(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m", EstimateTokens: 10}
	ledger.Submit(r)
	if ledger.Submitted() != 1 {
		t.Fatalf("Submitted want 1, got %d", ledger.Submitted())
	}
	if ledger.Count(Submitted) != 1 {
		t.Fatalf("Count(Submitted) want 1, got %d", ledger.Count(Submitted))
	}
}

func TestLegalPath(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m"}
	ledger.Submit(r)
	steps := []State{Queued, Admitted, InFlight, Completed}
	for _, s := range steps {
		if err := ledger.Transition(r, s); err != nil {
			t.Fatalf("transition to %d: %v", s, err)
		}
	}
	if ledger.Count(Completed) != 1 {
		t.Fatalf("Count(Completed) want 1, got %d", ledger.Count(Completed))
	}
}

func TestLegalDirectAdmitPath(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m"}
	ledger.Submit(r)
	for _, s := range []State{Admitted, InFlight, Completed} {
		if err := ledger.Transition(r, s); err != nil {
			t.Fatalf("transition to %d: %v", s, err)
		}
	}
}

func TestIllegalTransition(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m"}
	ledger.Submit(r)
	if err := ledger.Transition(r, InFlight); err == nil {
		t.Fatal("Submitted->InFlight should be illegal")
	}
}

func TestTerminalOnce(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m"}
	ledger.Submit(r)
	if err := ledger.Transition(r, Admitted); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := ledger.Transition(r, InFlight); err != nil {
		t.Fatalf("inflight: %v", err)
	}
	if err := ledger.Transition(r, Completed); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := ledger.Transition(r, Completed); err == nil {
		t.Fatal("second terminal transition should error")
	}
	if err := ledger.Transition(r, Expired); err == nil {
		t.Fatal("transition out of terminal state should error")
	}
}

func TestTerminalBalance(t *testing.T) {
	ledger := New()
	r := &Request{ID: 1, Model: "m"}
	ledger.Submit(r)
	if ledger.TerminalBalance() {
		t.Fatal("balance should be false with one in-flight request")
	}
	if err := ledger.Transition(r, Rejected); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if !ledger.TerminalBalance() {
		t.Fatal("balance should be true after request reaches a terminal state")
	}
}

func TestConcurrentLegalPaths(t *testing.T) {
	ledger := New()
	const goroutines, perG = 8, 1000
	errCh := make(chan error, goroutines*perG)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				r := &Request{ID: uint64(g*perG + i + 1), Model: "m"}
				ledger.Submit(r)
				for _, s := range []State{Admitted, InFlight, Completed} {
					if err := ledger.Transition(r, s); err != nil {
						errCh <- err
						return
					}
				}
			}
		}(g)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Fatalf("concurrent transition error: %v", err)
	}
	if got := ledger.Submitted(); got != goroutines*perG {
		t.Fatalf("Submitted want %d, got %d", goroutines*perG, got)
	}
	if !ledger.TerminalBalance() {
		t.Fatal("balance should hold after all goroutines finish")
	}
}
