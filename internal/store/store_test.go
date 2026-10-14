package store

import (
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	s, err := Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func sampleBatch(id string) Batch {
	return Batch{
		ID:          id,
		Status:      "processing",
		CallbackURL: "http://client/cb",
		Counts:      Counts{Total: 100},
		CreatedAt:   time.Unix(1700000000, 0),
	}
}

func TestBatchRoundTrip(t *testing.T) {
	s := openTest(t)
	b := sampleBatch("b-1")
	if err := s.CreateBatch(b); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	got, ok, err := s.GetBatch("b-1")
	if err != nil || !ok {
		t.Fatalf("GetBatch: ok=%v err=%v", ok, err)
	}
	if got.ID != "b-1" || got.Status != "processing" || got.CallbackURL != "http://client/cb" || got.Counts.Total != 100 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, ok, _ := s.GetBatch("nope"); ok {
		t.Fatal("unknown batch should not be found")
	}
}

func TestUpdateBatchCountsAndStatus(t *testing.T) {
	s := openTest(t)
	s.CreateBatch(sampleBatch("b-2"))
	s.UpdateBatchCounts("b-2", Counts{Total: 10, Successful: 7, Failed: 1, Expired: 1, Rejected: 1})
	s.SetBatchStatus("b-2", "completed")
	got, _, _ := s.GetBatch("b-2")
	if got.Status != "completed" || got.Counts.Successful != 7 || got.Counts.Rejected != 1 {
		t.Fatalf("update mismatch: %+v", got)
	}
}

func TestRequestLifecycle(t *testing.T) {
	s := openTest(t)
	r := RequestRow{ID: "r-1", BatchID: "b-1", Model: "m", State: "in_flight", AdmittedSec: 1700000000}
	if err := s.InsertRequest(r); err != nil {
		t.Fatalf("InsertRequest: %v", err)
	}
	need, err := s.RequestsNeedingRecovery()
	if err != nil || len(need) != 1 || need[0].ID != "r-1" {
		t.Fatalf("recovery should find the in_flight request, got %+v err=%v", need, err)
	}
	if err := s.UpdateRequestTerminal("r-1", "success", 950); err != nil {
		t.Fatalf("UpdateRequestTerminal: %v", err)
	}
	need, _ = s.RequestsNeedingRecovery()
	if len(need) != 0 {
		t.Fatalf("terminal request must leave the recovery set, got %d", len(need))
	}
}

func TestRequestsNeedingRecoveryStatesOnly(t *testing.T) {
	s := openTest(t)
	states := map[string]string{"r-q": "queued", "r-f": "in_flight", "r-c": "completed", "r-e": "expired", "r-s": "submitted"}
	for id, st := range states {
		if err := s.InsertRequest(RequestRow{ID: id, BatchID: "b", Model: "m", State: st}); err != nil {
			t.Fatal(err)
		}
	}
	need, _ := s.RequestsNeedingRecovery()
	if len(need) != 3 {
		t.Fatalf("want submitted+queued+in_flight for recovery, got %d: %+v", len(need), need)
	}
	for _, r := range need {
		if r.State != "queued" && r.State != "in_flight" && r.State != "submitted" {
			t.Fatalf("recovery set contains %s in state %s", r.ID, r.State)
		}
	}
}

func TestCallbackAttempts(t *testing.T) {
	s := openTest(t)
	s.CreateBatch(sampleBatch("b-3"))
	at := time.Unix(1700000100, 0)
	s.RecordCallbackAttempt("b-3", at, 500, "rejected")
	s.RecordCallbackAttempt("b-3", at.Add(time.Second), 200, "delivered")
	att, err := s.CallbackAttempts("b-3")
	if err != nil || len(att) != 2 {
		t.Fatalf("want 2 attempts, got %d err=%v", len(att), err)
	}
	if att[0].StatusCode != 500 || att[1].StatusCode != 200 {
		t.Fatalf("attempts out of order: %+v", att)
	}
}

func TestUndeliveredBatches(t *testing.T) {
	s := openTest(t)
	s.CreateBatch(sampleBatch("b-delivered"))
	s.CreateBatch(sampleBatch("b-undelivered"))
	s.SetBatchStatus("b-delivered", "completed")
	s.SetBatchStatus("b-undelivered", "completed")
	s.RecordCallbackAttempt("b-delivered", time.Now(), 200, "delivered")
	ids, err := s.UndeliveredBatches()
	if err != nil {
		t.Fatalf("UndeliveredBatches: %v", err)
	}
	found := false
	for _, id := range ids {
		if id == "b-undelivered" {
			found = true
		}
		if id == "b-delivered" {
			t.Fatal("delivered batch must not be listed")
		}
	}
	if !found {
		t.Fatalf("undelivered batch missing from %v", ids)
	}
}

func TestSeenKeys(t *testing.T) {
	s := openTest(t)
	if _, ok, _ := s.SeenKey("k-1"); ok {
		t.Fatal("unknown key should miss")
	}
	s.PutSeenKey("k-1", "success:900")
	res, ok, _ := s.SeenKey("k-1")
	if !ok || res != "success:900" {
		t.Fatalf("seen key round trip: ok=%v res=%q", ok, res)
	}
}
