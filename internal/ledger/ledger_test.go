package ledger

import (
	"sync"
	"testing"
)

func TestSubmitIncrements(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m", EstimateTokens: 10}
	l.Submit(r)
	if l.Submitted() != 1 {
		t.Fatalf("Submitted want 1, got %d", l.Submitted())
	}
	if l.Count(Submitted) != 1 {
		t.Fatalf("Count(Submitted) want 1, got %d", l.Count(Submitted))
	}
}

func TestLegalPath(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m"}
	l.Submit(r)
	steps := []State{Queued, Admitted, InFlight, Completed}
	for _, s := range steps {
		if err := l.Transition(r, s); err != nil {
			t.Fatalf("transition to %d: %v", s, err)
		}
	}
	if l.Count(Completed) != 1 {
		t.Fatalf("Count(Completed) want 1, got %d", l.Count(Completed))
	}
}

func TestLegalDirectAdmitPath(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m"}
	l.Submit(r)
	for _, s := range []State{Admitted, InFlight, Completed} {
		if err := l.Transition(r, s); err != nil {
			t.Fatalf("transition to %d: %v", s, err)
		}
	}
}

func TestIllegalTransition(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m"}
	l.Submit(r)
	if err := l.Transition(r, InFlight); err == nil {
		t.Fatal("Submitted->InFlight should be illegal")
	}
}

func TestTerminalOnce(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m"}
	l.Submit(r)
	if err := l.Transition(r, Admitted); err != nil {
		t.Fatalf("admit: %v", err)
	}
	if err := l.Transition(r, InFlight); err != nil {
		t.Fatalf("inflight: %v", err)
	}
	if err := l.Transition(r, Completed); err != nil {
		t.Fatalf("complete: %v", err)
	}
	if err := l.Transition(r, Completed); err == nil {
		t.Fatal("second terminal transition should error")
	}
	if err := l.Transition(r, Expired); err == nil {
		t.Fatal("transition out of terminal state should error")
	}
}

func TestTerminalBalance(t *testing.T) {
	l := New()
	r := &Request{ID: 1, Model: "m"}
	l.Submit(r)
	if l.TerminalBalance() {
		t.Fatal("balance should be false with one in-flight request")
	}
	if err := l.Transition(r, Rejected); err != nil {
		t.Fatalf("reject: %v", err)
	}
	if !l.TerminalBalance() {
		t.Fatal("balance should be true after request reaches a terminal state")
	}
}

func TestConcurrentLegalPaths(t *testing.T) {
	l := New()
	const goroutines, perG = 8, 1000
	errCh := make(chan error, goroutines*perG)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				r := &Request{ID: uint64(g*perG + i + 1), Model: "m"}
				l.Submit(r)
				for _, s := range []State{Admitted, InFlight, Completed} {
					if err := l.Transition(r, s); err != nil {
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
	if got := l.Submitted(); got != goroutines*perG {
		t.Fatalf("Submitted want %d, got %d", goroutines*perG, got)
	}
	if !l.TerminalBalance() {
		t.Fatal("balance should hold after all goroutines finish")
	}
}
