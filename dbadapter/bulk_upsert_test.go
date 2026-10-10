// SPDX-FileCopyrightText: 2026 Benjamin Grewell
// SPDX-License-Identifier: Apache-2.0

package dbadapter

import (
	"context"
	"errors"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

type putOneRecorder struct {
	DBInterface
	puts []UpsertOperation
	err  error
}

func (r *putOneRecorder) RestfulAPIPutOne(collName string, filter bson.M, putData map[string]any) (bool, error) {
	r.puts = append(r.puts, UpsertOperation{Filter: filter, Data: putData})
	return true, r.err
}

type bulkRecorder struct {
	putOneRecorder
	bulks [][]UpsertOperation
}

func (r *bulkRecorder) BulkUpsert(_ context.Context, _ string, ops []UpsertOperation) error {
	r.bulks = append(r.bulks, ops)
	return nil
}

func testOps(n int) []UpsertOperation {
	ops := make([]UpsertOperation, n)
	for i := range ops {
		ops[i] = UpsertOperation{Filter: bson.M{"ueId": i}, Data: map[string]any{"n": i}}
	}
	return ops
}

func TestBulkUpsertUsesTheClientsBulkWrite(t *testing.T) {
	client := &bulkRecorder{}
	if err := BulkUpsert(context.Background(), client, "coll", testOps(3)); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.bulks) != 1 || len(client.bulks[0]) != 3 {
		t.Fatalf("expected one bulk write of 3 documents, got %v", client.bulks)
	}
	if len(client.puts) != 0 {
		t.Fatalf("expected no single writes, got %d", len(client.puts))
	}
}

func TestBulkUpsertFallsBackToOneWriteAtATime(t *testing.T) {
	client := &putOneRecorder{}
	ops := testOps(3)
	if err := BulkUpsert(context.Background(), client, "coll", ops); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.puts) != len(ops) {
		t.Fatalf("expected %d single writes, got %d", len(ops), len(client.puts))
	}
	for i, op := range ops {
		if client.puts[i].Filter["ueId"] != op.Filter["ueId"] {
			t.Errorf("expected write %d for %v, got %v", i, op.Filter, client.puts[i].Filter)
		}
	}
}

func TestBulkUpsertReportsAFailedSingleWrite(t *testing.T) {
	writeErr := errors.New("not primary")
	client := &putOneRecorder{err: writeErr}
	if err := BulkUpsert(context.Background(), client, "coll", testOps(2)); !errors.Is(err, writeErr) {
		t.Fatalf("expected the write error, got %v", err)
	}
	if len(client.puts) != 1 {
		t.Fatalf("expected the writes to stop at the first failure, got %d", len(client.puts))
	}
}

func TestBulkUpsertWithNothingToWriteTouchesNothing(t *testing.T) {
	client := &bulkRecorder{}
	if err := BulkUpsert(context.Background(), client, "coll", nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(client.bulks) != 0 || len(client.puts) != 0 {
		t.Fatal("expected no writes")
	}
}

func TestBulkUpsertRefusesANilClient(t *testing.T) {
	if err := BulkUpsert(context.Background(), nil, "coll", testOps(1)); err == nil {
		t.Fatal("expected an error for a nil client")
	}
}
