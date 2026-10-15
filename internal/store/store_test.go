package store

import (
	"testing"
	"time"
)

func openTest(t *testing.T) *Store {
	t.Helper()
	store, err := Open(t.TempDir() + "/test.db")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
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
	store := openTest(t)
	b := sampleBatch("b-1")
	if err := store.CreateBatch(b); err != nil {
		t.Fatalf("CreateBatch: %v", err)
	}
	got, ok, err := store.GetBatch("b-1")
	if err != nil || !ok {
		t.Fatalf("GetBatch: ok=%v err=%v", ok, err)
	}
	if got.ID != "b-1" || got.Status != "processing" || got.CallbackURL != "http://client/cb" || got.Counts.Total != 100 {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if _, ok, _ := store.GetBatch("nope"); ok {
		t.Fatal("unknown batch should not be found")
	}
}

func TestUpdateBatchCountsAndStatus(t *testing.T) {
	store := openTest(t)
	store.CreateBatch(sampleBatch("b-2"))
	store.UpdateBatchCounts("b-2", Counts{Total: 10, Successful: 7, Failed: 1, Expired: 1, Rejected: 1})
	store.SetBatchStatus("b-2", "completed")
	got, _, _ := store.GetBatch("b-2")
	if got.Status != "completed" || got.Counts.Successful != 7 || got.Counts.Rejected != 1 {
		t.Fatalf("update mismatch: %+v", got)
	}
}

func TestRequestLifecycle(t *testing.T) {
	store := openTest(t)
	r := RequestRow{ID: "r-1", BatchID: "b-1", Model: "m", State: "in_flight", AdmittedSec: 1700000000}
	if err := store.InsertRequest(r); err != nil {
		t.Fatalf("InsertRequest: %v", err)
	}
	need, err := store.RequestsNeedingRecovery()
	if err != nil || len(need) != 1 || need[0].ID != "r-1" {
		t.Fatalf("recovery should find the in_flight request, got %+v err=%v", need, err)
	}
	if err := store.SetRequestFinal("r-1", "completed", "success", 950); err != nil {
		t.Fatalf("SetRequestFinal: %v", err)
	}
	need, _ = store.RequestsNeedingRecovery()
	if len(need) != 0 {
		t.Fatalf("terminal request must leave the recovery set, got %d", len(need))
	}
}

func TestRequestsNeedingRecoveryStatesOnly(t *testing.T) {
	store := openTest(t)
	states := map[string]string{"r-q": "queued", "r-f": "in_flight", "r-c": "completed", "r-e": "expired", "r-s": "submitted"}
	for id, st := range states {
		if err := store.InsertRequest(RequestRow{ID: id, BatchID: "b", Model: "m", State: st}); err != nil {
			t.Fatal(err)
		}
	}
	need, _ := store.RequestsNeedingRecovery()
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
	store := openTest(t)
	store.CreateBatch(sampleBatch("b-3"))
	at := time.Unix(1700000100, 0)
	store.RecordCallbackAttempt("b-3", at, 500, "rejected")
	store.RecordCallbackAttempt("b-3", at.Add(time.Second), 200, "delivered")
	att, err := store.CallbackAttempts("b-3")
	if err != nil || len(att) != 2 {
		t.Fatalf("want 2 attempts, got %d err=%v", len(att), err)
	}
	if att[0].StatusCode != 500 || att[1].StatusCode != 200 {
		t.Fatalf("attempts out of order: %+v", att)
	}
}

func TestUndeliveredBatches(t *testing.T) {
	store := openTest(t)
	store.CreateBatch(sampleBatch("b-delivered"))
	store.CreateBatch(sampleBatch("b-undelivered"))
	store.SetBatchStatus("b-delivered", "completed")
	store.SetBatchStatus("b-undelivered", "completed")
	store.RecordCallbackAttempt("b-delivered", time.Now(), 200, "delivered")
	ids, err := store.UndeliveredBatches()
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
	store := openTest(t)
	if _, ok, _ := store.SeenKey("k-1"); ok {
		t.Fatal("unknown key should miss")
	}
	store.PutSeenKey("k-1", "success:900")
	res, ok, _ := store.SeenKey("k-1")
	if !ok || res != "success:900" {
		t.Fatalf("seen key round trip: ok=%v res=%q", ok, res)
	}
}
