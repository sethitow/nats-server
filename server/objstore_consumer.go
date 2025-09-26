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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrMultiConsumerNotSupported = errors.New("multisubject consumers not supported on object stream")
	ErrPushConsumerNotSupported  = errors.New("push consumers not supported on object stream")
)

type consumerObjectStore struct {
	mu       sync.Mutex
	os       *objectStore
	cfg      *FileConsumerInfo
	name     string
	state    ConsumerState
	metaKey  string
	stateKey string

	// state management (similar to filestore)
	dirty     bool          // state needs uploading
	uploading atomic.Bool   // upload in progress
	flusher   atomic.Bool   // flusher is running
	fch       chan struct{} // trigger channel
	qch       chan struct{} // quit channel

	closed bool
}

// ConsumerStore creates a new consumer
// TODO: what is the difference between the name argument here and cfg.Name?
func (os *objectStore) ConsumerStore(name string, cfg *ConsumerConfig) (ConsumerStore, error) {
	if os.isClosed() {
		return nil, ErrStoreClosed
	}
	if cfg == nil || name == "" {
		return nil, fmt.Errorf("bad consumer config")
	}

	if len(cfg.FilterSubjects) > 1 {
		return nil, ErrMultiConsumerNotSupported
	}

	if cfg.DeliverSubject != _EMPTY_ {
		return nil, ErrPushConsumerNotSupported
	}

	if cfg.MemoryStorage {
		o := &consumerMemStore{ms: os, cfg: *cfg}
		os.AddConsumer(o)
		return o, nil
	}

	csi := &FileConsumerInfo{Name: name, Created: time.Now().UTC(), ConsumerConfig: *cfg}
	cos := &consumerObjectStore{
		os:       os,
		cfg:      csi,
		name:     name,
		metaKey:  fmt.Sprintf(consumerMetaKeyPattern, name),
		stateKey: fmt.Sprintf(consumerStateKeyPattern, name),
		fch:      make(chan struct{}, 1),
		qch:      make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	if _, err := os.client.HeadObject(ctx, cos.metaKey); err != nil && isObjNotFound(err) {
		csi.Created = time.Now().UTC()
		if err := cos.uploadConsumerMeta(); err != nil {
			// TODO: do some cleanup like the filestore does
			return nil, err
		}
	}

	cos.loadState()
	go cos.flushLoop()

	os.AddConsumer(cos)
	return cos, nil
}

// Type returns the storage type
func (cos *consumerObjectStore) Type() StorageType {
	return ObjectStorage
}

func (cos *consumerObjectStore) Update(state *ConsumerState) error {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	if cos.closed {
		return ErrStoreClosed
	}

	// silly checks
	if state.AckFloor.Consumer > state.Delivered.Consumer {
		return fmt.Errorf("bad ack floor for consumer")
	}
	if state.AckFloor.Stream > state.Delivered.Stream {
		return fmt.Errorf("bad ack floor for stream")
	}

	var pending map[uint64]*Pending
	if state.Pending != nil {
		pending = make(map[uint64]*Pending, len(state.Pending))
		for seq, p := range state.Pending {
			pending[seq] = &Pending{
				Sequence:  p.Sequence,
				Timestamp: p.Timestamp,
			}
		}
	}

	var redelivered map[uint64]uint64
	if state.Redelivered != nil {
		redelivered = make(map[uint64]uint64, len(state.Redelivered))
		for seq, count := range state.Redelivered {
			redelivered[seq] = count
		}
	}

	// Update state
	cos.state = ConsumerState{
		Delivered:   state.Delivered,
		AckFloor:    state.AckFloor,
		Pending:     pending,
		Redelivered: redelivered,
	}

	cos.markDirty()
	return nil
}

// UpdateDelivered is called whenever a new message has been delivered.
// largely copied from filestore
func (cos *consumerObjectStore) UpdateDelivered(dseq, sseq, dc uint64, ts int64) error {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	if dc != 1 && cos.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= cos.state.AckFloor.Consumer {
		return nil
	}

	// See if we expect an ack for this.
	if cos.cfg.AckPolicy != AckNone {
		// Need to create pending records here.
		if cos.state.Pending == nil {
			cos.state.Pending = make(map[uint64]*Pending)
		}
		var p *Pending
		// Check for an update to a message already delivered.
		if sseq <= cos.state.Delivered.Stream {
			if p = cos.state.Pending[sseq]; p != nil {
				// Do not update p.Sequence, that should be the original delivery sequence.
				p.Timestamp = ts
			}
		} else {
			// Add to pending.
			cos.state.Pending[sseq] = &Pending{dseq, ts}
		}
		// Update delivered as needed.
		if dseq > cos.state.Delivered.Consumer {
			cos.state.Delivered.Consumer = dseq
		}
		if sseq > cos.state.Delivered.Stream {
			cos.state.Delivered.Stream = sseq
		}

		if dc > 1 {
			if maxdc := uint64(cos.cfg.MaxDeliver); maxdc > 0 && dc > maxdc {
				// Make sure to remove from pending.
				delete(cos.state.Pending, sseq)
			}
			if cos.state.Redelivered == nil {
				cos.state.Redelivered = make(map[uint64]uint64)
			}
			// Only update if greater than what we already have.
			if cos.state.Redelivered[sseq] < dc-1 {
				cos.state.Redelivered[sseq] = dc - 1
			}
		}
	} else {
		// For AckNone just update delivered and ackfloor at the same time.
		if dseq > cos.state.Delivered.Consumer {
			cos.state.Delivered.Consumer = dseq
			cos.state.AckFloor.Consumer = dseq
		}
		if sseq > cos.state.Delivered.Stream {
			cos.state.Delivered.Stream = sseq
			cos.state.AckFloor.Stream = sseq
		}
	}

	cos.markDirty()
	return nil
}

// UpdateAcks is called whenever a consumer with explicit ack or ack all acks a message.
// largely copied from filestore
func (cos *consumerObjectStore) UpdateAcks(dseq, sseq uint64) error {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	if cos.cfg.AckPolicy == AckNone {
		return ErrNoAckPolicy
	}

	// On restarts the old leader may get a replay from the raft logs that are old.
	if dseq <= cos.state.AckFloor.Consumer {
		return nil
	}

	if len(cos.state.Pending) == 0 || cos.state.Pending[sseq] == nil {
		delete(cos.state.Redelivered, sseq)
		return ErrStoreMsgNotFound
	}

	// Check for AckAll here.
	if cos.cfg.AckPolicy == AckAll {
		sgap := sseq - cos.state.AckFloor.Stream
		cos.state.AckFloor.Consumer = dseq
		cos.state.AckFloor.Stream = sseq
		if sgap > uint64(len(cos.state.Pending)) {
			for seq := range cos.state.Pending {
				if seq <= sseq {
					delete(cos.state.Pending, seq)
					delete(cos.state.Redelivered, seq)
				}
			}
		} else {
			for seq := sseq; seq > sseq-sgap && len(cos.state.Pending) > 0; seq-- {
				delete(cos.state.Pending, seq)
				delete(cos.state.Redelivered, seq)
			}
		}
		cos.markDirty()
		return nil
	}

	// AckExplicit

	// First delete from our pending state.
	if p, ok := cos.state.Pending[sseq]; ok {
		delete(cos.state.Pending, sseq)
		if dseq > p.Sequence && p.Sequence > 0 {
			dseq = p.Sequence // Use the original.
		}
	}
	if len(cos.state.Pending) == 0 {
		cos.state.AckFloor.Consumer = cos.state.Delivered.Consumer
		cos.state.AckFloor.Stream = cos.state.Delivered.Stream
	} else if dseq == cos.state.AckFloor.Consumer+1 {
		cos.state.AckFloor.Consumer = dseq
		cos.state.AckFloor.Stream = sseq

		if cos.state.Delivered.Consumer > dseq {
			for ss := sseq + 1; ss <= cos.state.Delivered.Stream; ss++ {
				if p, ok := cos.state.Pending[ss]; ok {
					if p.Sequence > 0 {
						cos.state.AckFloor.Consumer = p.Sequence - 1
						cos.state.AckFloor.Stream = ss - 1
					}
					break
				}
			}
		}
	}
	// We do these regardless.
	delete(cos.state.Redelivered, sseq)

	cos.markDirty()
	return nil
}

// State returns a copy of the current consumer state
func (cos *consumerObjectStore) State() (*ConsumerState, error) {
	return cos.stateWithCopy(true)
}

// BorrowState returns the consumer state without copying (for read-only access)
func (cos *consumerObjectStore) BorrowState() (*ConsumerState, error) {
	return cos.stateWithCopy(false)
}

// stateWithCopy returns the consumer state with optional copying
func (cos *consumerObjectStore) stateWithCopy(doCopy bool) (*ConsumerState, error) {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	if cos.closed {
		return nil, ErrStoreClosed
	}

	if !doCopy {
		return &cos.state, nil
	}

	// alloc a new state, and populate it explicitly
	state := &ConsumerState{
		Delivered: cos.state.Delivered,
		AckFloor:  cos.state.AckFloor,
	}

	// Copy pending map
	if cos.state.Pending != nil {
		state.Pending = make(map[uint64]*Pending, len(cos.state.Pending))
		for seq, p := range cos.state.Pending {
			state.Pending[seq] = &Pending{
				Sequence:  p.Sequence,
				Timestamp: p.Timestamp,
			}
		}
	}

	// Copy redelivered map
	if cos.state.Redelivered != nil {
		state.Redelivered = make(map[uint64]uint64, len(cos.state.Redelivered))
		for seq, count := range cos.state.Redelivered {
			state.Redelivered[seq] = count
		}
	}

	return state, nil
}

// markDirty sets dirty to true and kicks the flusher
// lock should be taken
func (cos *consumerObjectStore) markDirty() {
	if !cos.dirty {
		cos.dirty = true
		select {
		case cos.fch <- struct{}{}:
		default:
		}
	}
}

func (cos *consumerObjectStore) flushLoop() {
	cos.flusher.Store(true)
	defer cos.flusher.Store(false)
	// assume roundtrip put latency is approx 200ms
	// maintain approximately 5 updates per second
	minTime := 200 * time.Millisecond

	var lastWrite time.Time
	var dt *time.Timer

	setDelayTimer := func(addWait time.Duration) {
		if dt == nil {
			dt = time.NewTimer(addWait)
			return
		}
		if !dt.Stop() {
			select {
			case <-dt.C:
			default:
			}
		}
		dt.Reset(addWait)
	}

	for {
		select {
		case <-cos.fch:
			if ts := time.Since(lastWrite); ts < minTime {
				setDelayTimer(minTime - ts)
				select {
				case <-dt.C:
				case <-cos.qch:
					return
				}
			}
			cos.mu.Lock()
			if cos.closed {
				cos.mu.Unlock()
				return
			}
			if err := cos.uploadState(); err == nil {
				lastWrite = time.Now()
			} else {
				cos.os.srv.Errorf("failed to upload consumer state: stream=%s consumer=%s err=%v",
					cos.os.cfg.Name,
					cos.name,
					err)
			}
			cos.mu.Unlock()
		case <-cos.qch:
			return
		}
	}
}

// lock should be taken
func (cos *consumerObjectStore) uploadState() error {
	if !cos.dirty || cos.uploading.Load() {
		return nil
	}
	cos.uploading.Store(true)
	defer cos.uploading.Store(false)

	stateData, err := cos.encodeState()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cos.os.oscfg.UploadTimeout)
	defer cancel()

	_, err = cos.os.client.PutObject(ctx, cos.stateKey, stateData)
	if err != nil {
		return err
	}
	cos.dirty = false

	return err
}

// consumer lock must be taken
func (cos *consumerObjectStore) encodeState() ([]byte, error) {
	return encodeConsumerState(&cos.state), nil
}

func (cos *consumerObjectStore) loadState() {
	ctx, cancel := context.WithTimeout(context.Background(), cos.os.oscfg.UploadTimeout)
	defer cancel()

	data, err := cos.os.client.GetObject(ctx, cos.stateKey)
	if err != nil {
		if isObjNotFound(err) {
			// no existing state, start with empty state
			cos.state = ConsumerState{}
			return
		}
		cos.os.srv.Errorf("failed to download consumer state: stream=%s consumer=%s err=%v",
			cos.os.cfg.Name,
			cos.name,
			err)
		return
	}

	state, err := decodeConsumerState(data)
	if err != nil {
		cos.os.srv.Errorf("failed to decode consumer state: stream=%s consumer=%s err=%v",
			cos.os.cfg.Name,
			cos.name,
			err)
		return
	}
	cos.state.Delivered = state.Delivered
	cos.state.AckFloor = state.AckFloor
	if len(state.Pending) > 0 {
		cos.state.Pending = state.Pending
	}
	if len(state.Redelivered) > 0 {
		cos.state.Redelivered = state.Redelivered
	}
}

// Stop stops the consumer store and flushes any pending state
func (cos *consumerObjectStore) Stop() error {
	cos.mu.Lock()
	if cos.closed {
		cos.mu.Unlock()
		return nil
	}
	cos.closed = true
	cos.mu.Unlock()

	close(cos.qch)
	cos.waitOnFlusher()

	cos.mu.Lock()
	cos.uploadState()
	cos.mu.Unlock()

	return nil
}

func (cos *consumerObjectStore) Delete() error {
	if err := cos.Stop(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cos.os.oscfg.UploadTimeout)
	defer cancel()

	if err := cos.os.client.DeleteObject(ctx, cos.stateKey); err != nil && !isObjNotFound(err) {
		return fmt.Errorf("failed to delete consumer state: %w", err)
	}

	if err := cos.os.client.DeleteObject(ctx, cos.metaKey); err != nil && !isObjNotFound(err) {
		return fmt.Errorf("failed to delete consumer metadata: %w", err)
	}

	return nil
}

// StreamDelete is called when the parent stream is deleted
func (cos *consumerObjectStore) StreamDelete() error {
	return cos.Delete()
}

// HasState checks if the consumer has any stored state
func (cos *consumerObjectStore) HasState() bool {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	// Check if we have meaningful state
	return cos.state.Delivered.Consumer != 0 || cos.state.Delivered.Stream != 0 ||
		len(cos.state.Pending) > 0 || len(cos.state.Redelivered) > 0
}

// SetStarting sets the starting sequence for the consumer
func (cos *consumerObjectStore) SetStarting(sseq uint64) error {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	cos.state.Delivered.Stream = sseq
	cos.dirty = true
	return cos.uploadState()
}

// UpdateStarting updates the starting sequence if it's higher
// TODO: why does SetStarting save synchronously but UpdateStarting is async?
func (cos *consumerObjectStore) UpdateStarting(sseq uint64) {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	if sseq > cos.state.Delivered.Stream {
		cos.state.Delivered.Stream = sseq
		// For AckNone just update delivered and ackfloor at the same time.
		if cos.cfg.AckPolicy == AckNone {
			cos.state.AckFloor.Stream = sseq
		}
		cos.markDirty()
	}
}

// EncodedState returns the encoded consumer state
func (cos *consumerObjectStore) EncodedState() ([]byte, error) {
	cos.mu.Lock()
	defer cos.mu.Unlock()
	return cos.encodeState()
}

// UpdateConfig updates the consumer configuration
func (cos *consumerObjectStore) UpdateConfig(cfg *ConsumerConfig) error {
	cos.mu.Lock()
	defer cos.mu.Unlock()

	cos.cfg.ConsumerConfig = *cfg
	return cos.uploadConsumerMeta()
}

func (cos *consumerObjectStore) uploadConsumerMeta() error {
	b, err := json.Marshal(cos.cfg)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), cos.os.oscfg.UploadTimeout)
	defer cancel()
	_, err = cos.os.client.PutObject(ctx, cos.metaKey, b)
	return err
}

// waits up to 100ms for the flusher to exit
func (cos *consumerObjectStore) waitOnFlusher() {
	if !cos.flusher.Load() {
		return
	}

	timeout := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(timeout) {
		if !cos.flusher.Load() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}
