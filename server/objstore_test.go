// Copyright 2025 Seth Itow
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package server

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// mockObjectStorageClient implements ObjectStorageClient interface for testing
type mockObjectStorageClient struct {
	mu      sync.RWMutex
	objects map[string][]byte
	errors  map[string]error // For simulating errors
}

func newMockObjectStorageClient() *mockObjectStorageClient {
	return &mockObjectStorageClient{
		objects: make(map[string][]byte),
		errors:  make(map[string]error),
	}
}

func (m *mockObjectStorageClient) PutObject(ctx context.Context, key string, data []byte) (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err, exists := m.errors[key]; exists {
		return "", err
	}

	m.objects[key] = make([]byte, len(data))
	copy(m.objects[key], data)
	return fmt.Sprintf("etag-%s", key), nil
}

func (m *mockObjectStorageClient) GetObject(ctx context.Context, key string) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err, exists := m.errors[key]; exists {
		return nil, err
	}

	data, exists := m.objects[key]
	if !exists {
		return nil, ErrStoreMsgNotFound
	}

	result := make([]byte, len(data))
	copy(result, data)
	return result, nil
}

func (m *mockObjectStorageClient) GetObjectRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err, exists := m.errors[key]; exists {
		return nil, err
	}

	data, exists := m.objects[key]
	if !exists {
		return nil, ErrStoreMsgNotFound
	}

	if offset >= int64(len(data)) {
		return nil, fmt.Errorf("offset beyond data")
	}

	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	result := make([]byte, end-offset)
	copy(result, data[offset:end])
	return result, nil
}

func (m *mockObjectStorageClient) DeleteObject(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err, exists := m.errors[key]; exists {
		return err
	}

	delete(m.objects, key)
	return nil
}

func (m *mockObjectStorageClient) DeleteObjects(ctx context.Context, prefix string) error {
	return nil
}

func (m *mockObjectStorageClient) ListObjects(ctx context.Context, prefix string) ([]StoredObject, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var objects []StoredObject
	for key, data := range m.objects {
		if len(prefix) == 0 || key[:len(prefix)] == prefix {
			objects = append(objects, StoredObject{
				Key:          key,
				Size:         int64(len(data)),
				LastModified: time.Now(),
				ETag:         fmt.Sprintf("etag-%s", key),
			})
		}
	}
	return objects, nil
}

func (m *mockObjectStorageClient) HeadObject(ctx context.Context, key string) (*StoredObject, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if err, exists := m.errors[key]; exists {
		return nil, err
	}

	data, exists := m.objects[key]
	if !exists {
		return nil, ErrStoreMsgNotFound
	}

	return &StoredObject{
		Size:         int64(len(data)),
		LastModified: time.Now(),
		ETag:         fmt.Sprintf("etag-%s", key),
	}, nil
}

func (m *mockObjectStorageClient) setError(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errors[key] = err
}

func (m *mockObjectStorageClient) clearError(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.errors, key)
}

func (m *mockObjectStorageClient) getObjectCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.objects)
}

// newTestObjectStore creates an object store with a mock client for testing
func newTestObjectStore(cfg StreamConfig) (*objectStore, *mockObjectStorageClient, error) {
	mockClient := newMockObjectStorageClient()

	oscfg := ObjectStreamConfig{
		ObjectStoreConfig: ObjectStoreConfig{
			Bucket:     "test-bucket",
			PathPrefix: "test/streams/" + cfg.Name,
		},
		BlockSize:     1024, // Small block size for testing
		BlockTimeout:  100 * time.Millisecond,
		UploadTimeout: 5 * time.Second,
		MaxRetries:    3,
	}

	os := &objectStore{
		cfg:             cfg,
		oscfg:           oscfg,
		uploadingBlocks: make(map[uint32]*memoryBlock),
		client:          mockClient,
		uploadQueue:     make(chan *uploadRequest, 100),
		stopCh:          make(chan struct{}),
	}

	// Create upload manager
	ctx, cancel := context.WithCancel(context.Background())
	os.uploader = &blockUploader{
		uploadQueue: os.uploadQueue,
		workers:     1,
		ctx:         ctx,
		cancel:      cancel,
	}

	// Create initial memory block
	os.currentBlock = newMemoryBlock(1)

	// Start upload worker
	go os.uploader.start()

	return os, mockClient, nil
}

func TestObjectStoreBasics(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Test Type
	if os.Type() != ObjectStorage {
		t.Fatalf("Expected ObjectStorage type, got %v", os.Type())
	}

	// Test initial state
	state := os.State()
	if state.Msgs != 0 || state.Bytes != 0 || state.FirstSeq != 0 || state.LastSeq != 0 {
		t.Fatalf("Expected empty initial state, got %+v", state)
	}
}

func TestObjectStoreStoreAndRetrieveMsg(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, mockStore, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Store a message
	subj, msg := "test.subject", []byte("Hello World")
	seq, ts, err := os.StoreMsg(subj, nil, msg, 0)
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	if seq != 1 {
		t.Fatalf("Expected sequence 1, got %d", seq)
	}

	if ts <= 0 {
		t.Fatalf("Expected positive timestamp, got %d", ts)
	}

	// Check state
	state := os.State()
	if state.Msgs != 1 {
		t.Fatalf("Expected 1 message, got %d", state.Msgs)
	}
	if state.FirstSeq != 1 || state.LastSeq != 1 {
		t.Fatalf("Expected first/last seq 1, got first=%d last=%d", state.FirstSeq, state.LastSeq)
	}

	// Verify block was uploaded
	if mockStore.getObjectCount() == 0 {
		t.Fatal("Expected at least one object")
	}

	// Retrieve the message
	storeMsg, err := os.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Failed to load message: %v", err)
	}

	if storeMsg.subj != subj {
		t.Fatalf("Expected subject %s, got %s", subj, storeMsg.subj)
	}
	if string(storeMsg.msg) != string(msg) {
		t.Fatalf("Expected data %s, got %s", string(msg), string(storeMsg.msg))
	}
	if storeMsg.seq != seq {
		t.Fatalf("Expected sequence %d, got %d", seq, storeMsg.seq)
	}
}

func TestObjectStoreMultipleMessages(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Store multiple messages
	numMsgs := 10
	for i := 1; i <= numMsgs; i++ {
		subj := fmt.Sprintf("test.%d", i)
		msg := []byte(fmt.Sprintf("Message %d", i))

		seq, _, err := os.StoreMsg(subj, nil, msg, 0)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}

		if seq != uint64(i) {
			t.Fatalf("Expected sequence %d, got %d", i, seq)
		}
	}

	// Check final state
	state := os.State()
	if state.Msgs != uint64(numMsgs) {
		t.Fatalf("Expected %d messages, got %d", numMsgs, state.Msgs)
	}
	if state.FirstSeq != 1 || state.LastSeq != uint64(numMsgs) {
		t.Fatalf("Expected first/last seq 1/%d, got first=%d last=%d", numMsgs, state.FirstSeq, state.LastSeq)
	}

	// Retrieve all messages
	for i := 1; i <= numMsgs; i++ {
		storeMsg, err := os.LoadMsg(uint64(i), nil)
		if err != nil {
			t.Fatalf("Failed to load message %d: %v", i, err)
		}

		expectedSubj := fmt.Sprintf("test.%d", i)
		expectedData := fmt.Sprintf("Message %d", i)

		if storeMsg.subj != expectedSubj {
			t.Fatalf("Message %d: expected subject %s, got %s", i, expectedSubj, storeMsg.subj)
		}
		if string(storeMsg.msg) != expectedData {
			t.Fatalf("Message %d: expected data %s, got %s", i, expectedData, string(storeMsg.msg))
		}
	}
}

func TestObjectStoreLoadLastMsg(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Store messages with different subjects
	subjects := []string{"foo.1", "foo.2", "bar.1", "foo.3"}
	for i, subj := range subjects {
		msg := []byte(fmt.Sprintf("Message %d", i+1))
		_, _, err := os.StoreMsg(subj, nil, msg, 0)
		if err != nil {
			t.Fatalf("Failed to store message for subject %s: %v", subj, err)
		}
	}

	// Test LoadLastMsg for "foo.3" (should be sequence 4)
	storeMsg, err := os.LoadLastMsg("foo.3", nil)
	if err != nil {
		t.Fatalf("Failed to load last message for foo.3: %v", err)
	}
	if storeMsg.subj != "foo.3" || storeMsg.seq != 4 {
		t.Fatalf("Expected subject foo.3 seq 4, got subject %s seq %d", storeMsg.subj, storeMsg.seq)
	}

	// Test LoadLastMsg for "bar.1" (should be sequence 3)
	storeMsg, err = os.LoadLastMsg("bar.1", nil)
	if err != nil {
		t.Fatalf("Failed to load last message for bar.1: %v", err)
	}
	if storeMsg.subj != "bar.1" || storeMsg.seq != 3 {
		t.Fatalf("Expected subject bar.1 seq 3, got subject %s seq %d", storeMsg.subj, storeMsg.seq)
	}

	// Test LoadLastMsg for non-existent subject
	_, err = os.LoadLastMsg("nonexistent", nil)
	if err != ErrStoreMsgNotFound {
		t.Fatalf("Expected ErrStoreMsgNotFound for nonexistent subject, got %v", err)
	}
}

func TestObjectStoreConsumerStore(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Create consumer store
	consumerCfg := &ConsumerConfig{
		Durable:   "test-consumer",
		AckPolicy: AckExplicit,
	}

	cs, err := os.ConsumerStore("test-consumer", consumerCfg)
	if err != nil {
		t.Fatalf("Failed to create consumer store: %v", err)
	}
	defer cs.Stop()

	// Test Type
	if cs.Type() != ObjectStorage {
		t.Fatalf("Expected ObjectStorage type, got %v", cs.Type())
	}

	// Test initial state
	state, err := cs.State()
	if err != nil {
		t.Fatalf("Failed to get consumer state: %v", err)
	}

	if state.Delivered.Consumer != 0 || state.Delivered.Stream != 0 {
		t.Fatalf("Expected empty initial state, got %+v", state)
	}

	// Test UpdateDelivered
	err = cs.UpdateDelivered(1, 1, 1, time.Now().UnixNano())
	if err != nil {
		t.Fatalf("Failed to update delivered: %v", err)
	}

	// Check updated state
	state, err = cs.State()
	if err != nil {
		t.Fatalf("Failed to get updated consumer state: %v", err)
	}

	if state.Delivered.Consumer != 1 || state.Delivered.Stream != 1 {
		t.Fatalf("Expected delivered 1/1, got %d/%d", state.Delivered.Consumer, state.Delivered.Stream)
	}

	// For AckExplicit, should have pending
	if len(state.Pending) != 1 {
		t.Fatalf("Expected 1 pending message, got %d", len(state.Pending))
	}

	// Test UpdateAcks
	err = cs.UpdateAcks(1, 1)
	if err != nil {
		t.Fatalf("Failed to update acks: %v", err)
	}

	// Check acked state
	state, err = cs.State()
	if err != nil {
		t.Fatalf("Failed to get acked consumer state: %v", err)
	}

	if state.AckFloor.Consumer != 1 || state.AckFloor.Stream != 1 {
		t.Fatalf("Expected ack floor 1/1, got %d/%d", state.AckFloor.Consumer, state.AckFloor.Stream)
	}

	// Should no longer have pending
	if len(state.Pending) != 0 {
		t.Fatalf("Expected 0 pending messages, got %d", len(state.Pending))
	}
}

func TestObjectStoreErrors(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, mockStore, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Simulate upload error
	blockKey := "test/streams/TEST/blocks/0000000001.blk"
	mockStore.setError(blockKey, fmt.Errorf("upload failed"))

	// Try to store a message - should fail due to upload error
	subj, msg := "test.subject", []byte("Hello World")
	_, _, err = os.StoreMsg(subj, nil, msg, 0)
	if err == nil {
		t.Fatal("Expected error due to upload failure")
	}

	// Clear the error and try again
	mockStore.clearError(blockKey)
	seq, _, err := os.StoreMsg(subj, nil, msg, 0)
	if err != nil {
		t.Fatalf("Failed to store message after clearing error: %v", err)
	}
	if seq != 1 {
		t.Fatalf("Expected sequence 1, got %d", seq)
	}
}

func TestObjectStoreBlockSealing(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, mockStore, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Store messages that should fill one block and start another
	// With block size of 1024 bytes, should need multiple messages
	initialCount := mockStore.getObjectCount()

	// Store enough messages to trigger block sealing
	for i := 1; i <= 20; i++ {
		subj := "test"
		msg := make([]byte, 100) // 100 bytes per message
		for j := range msg {
			msg[j] = byte(i % 256)
		}

		_, _, err := os.StoreMsg(subj, nil, msg, 0)
		if err != nil {
			t.Fatalf("Failed to store message %d: %v", i, err)
		}
	}

	// Should have created at least one block
	finalCount := mockStore.getObjectCount()
	if finalCount <= initialCount {
		t.Fatalf("Expected more objects after storing messages, got initial=%d final=%d", initialCount, finalCount)
	}

	// All messages should be retrievable
	for i := 1; i <= 20; i++ {
		storeMsg, err := os.LoadMsg(uint64(i), nil)
		if err != nil {
			t.Fatalf("Failed to load message %d: %v", i, err)
		}
		if storeMsg.seq != uint64(i) {
			t.Fatalf("Message %d: expected sequence %d, got %d", i, i, storeMsg.seq)
		}
	}
}

func TestObjectStoreStop(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}

	// Store a message
	_, _, err = os.StoreMsg("test", nil, []byte("test message"), 0)
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Stop the store
	err = os.Stop()
	if err != nil {
		t.Fatalf("Failed to stop object store: %v", err)
	}

	// Verify store is closed
	if !os.isClosed() {
		t.Fatal("Expected store to be closed after Stop()")
	}

	// Should not be able to store messages after stop
	_, _, err = os.StoreMsg("test2", nil, []byte("should fail"), 0)
	if err != ErrStoreClosed {
		t.Fatalf("Expected ErrStoreClosed after stop, got %v", err)
	}
}

func TestObjectStoreInvalidSequence(t *testing.T) {
	cfg := StreamConfig{Name: "TEST", Storage: ObjectStorage}
	os, _, err := newTestObjectStore(cfg)
	if err != nil {
		t.Fatalf("Failed to create object store: %v", err)
	}
	defer os.Stop()

	// Try to load non-existent message
	_, err = os.LoadMsg(1, nil)
	if err != ErrStoreMsgNotFound {
		t.Fatalf("Expected ErrStoreMsgNotFound for non-existent message, got %v", err)
	}

	// Store a message
	_, _, err = os.StoreMsg("test", nil, []byte("test message"), 0)
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Try to load message beyond range
	_, err = os.LoadMsg(2, nil)
	if err != ErrStoreMsgNotFound {
		t.Fatalf("Expected ErrStoreMsgNotFound for out of range message, got %v", err)
	}

	// Load valid message
	storeMsg, err := os.LoadMsg(1, nil)
	if err != nil {
		t.Fatalf("Failed to load valid message: %v", err)
	}
	if storeMsg.seq != 1 {
		t.Fatalf("Expected sequence 1, got %d", storeMsg.seq)
	}
}
