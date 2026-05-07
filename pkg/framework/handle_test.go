/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package framework

import (
	"context"
	"testing"
	"time"

	"github.com/llm-d/llm-d-inference-payload-processor/pkg/framework/datastore"
)

// testCloneableValue is a test implementation of Cloneable interface
type testCloneableValue struct {
	Value string
}

func (t testCloneableValue) Clone() datastore.Cloneable {
	return testCloneableValue{Value: t.Value}
}

// TestGetDatastoreSnapshot_CacheLifetime tests that the snapshot cache respects the configured lifetime.
func TestGetDatastoreSnapshot_CacheLifetime(t *testing.T) {
	// Create datastore and handle with 100ms snapshot lifetime
	datastores := datastore.NewDatastores()
	handle := &payloadProcessorHandle{
		ctx:              context.Background(),
		mgr:              nil,
		datastores:       datastores,
		snapshotLifetime: 100 * time.Millisecond,
	}

	// Add topic with three keys
	topic, _ := datastores.GetOrCreateStore("test-snapshot")
	topic.Put("key1", testCloneableValue{Value: "value1"})
	topic.Put("key2", testCloneableValue{Value: "value2"})
	topic.Put("key3", testCloneableValue{Value: "value3"})

	cycleState := NewCycleState()

	// First call: snapshot should have 3 keys
	snapshot1, err := handle.GetDatastoreSnapshot("test-snapshot", cycleState)
	if err != nil {
		t.Fatalf("first snapshot failed: %v", err)
	}
	if len(snapshot1.Keys()) != 3 {
		t.Errorf("expected 3 keys in first snapshot, got %d", len(snapshot1.Keys()))
	}

	// Delete one key from datastore
	topic.Delete("key3")

	// Second call (within lifetime): cached snapshot should still have 3 keys
	snapshot2, err := handle.GetDatastoreSnapshot("test-snapshot", cycleState)
	if err != nil {
		t.Fatalf("second snapshot failed: %v", err)
	}
	if len(snapshot2.Keys()) != 3 {
		t.Errorf("expected 3 keys in cached snapshot, got %d", len(snapshot2.Keys()))
	}

	// Wait for cache expiration
	time.Sleep(110 * time.Millisecond)

	// Third call (after expiration): refreshed snapshot should have 2 keys
	snapshot3, err := handle.GetDatastoreSnapshot("test-snapshot", cycleState)
	if err != nil {
		t.Fatalf("third snapshot failed: %v", err)
	}
	if len(snapshot3.Keys()) != 2 {
		t.Errorf("expected 2 keys in refreshed snapshot, got %d", len(snapshot3.Keys()))
	}
}
