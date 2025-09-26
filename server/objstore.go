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
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/nats-io/nats-server/v2/server/gsl"
	"github.com/nats-io/nats-server/v2/server/stree"
)

// ObjectStreamConfig configures the object store backend
type ObjectStreamConfig struct {
	ObjectStoreConfig

	// Block Configuration
	BlockSize            uint64        // Target block size before upload
	BlockTimeout         time.Duration // Max time to wait before uploading partial block
	UploadTimeout        time.Duration
	MaxRetries           int // Upload retry limit
	MaxConcurrentUploads int // number of upload workers to run

	// Memory Configuration
	MaxMemoryBlocks int   // Max blocks to buffer in memory
	MemoryLimit     int64 // Max memory usage for buffering

	// Internal reference
	srv *Server
}

// objectStore implements the StreamStore interface for remote object storage
type objectStore struct {
	srv    *Server
	mu     sync.RWMutex
	cfg    StreamConfig
	oscfg  ObjectStreamConfig
	state  StreamState
	blocks []*blockMeta
	psim   *stree.SubjectTree[psi]
	fss    *stree.SubjectTree[SimpleState]

	// block management
	currentBlock    *memoryBlock            // Active block for new messages
	uploadingBlocks map[uint32]*memoryBlock // Blocks being uploaded

	client      ObjectStorageClient
	uploader    *blockUploader      // Upload manager
	uploadQueue chan *uploadRequest // Upload request queue

	// lifecycle
	stopCh chan struct{} // Shutdown signal
	closed bool          // Closed flag
}

// memoryBlock represents a block of messages in memory before upload
type memoryBlock struct {
	meta *blockMeta              // mu inside blockMeta protects whole block
	fss  map[string]*SimpleState // Per-subject info for this block

	// message storage
	messages []*storedMessage          // Messages in this block
	msgIndex map[uint64]*storedMessage // Sequence to message mapping
}

type blockMeta struct {
	blockRef
	mu sync.RWMutex

	first msgId  // first message seq and timestamp
	last  msgId  // last message seq and timestamp
	bytes uint64 // bytes as-stored, excluding block-level metadata
	count uint64 // message count
}

type blockRef struct {
	index uint32
	key   string
	etag  string
}

// storedMessage represents a single message within a block
type storedMessage struct {
	seq  uint64 // Sequence number
	subj string // Subject
	hdr  []byte // Message headers
	msg  []byte // Message payload
	ts   int64  // Timestamp (nanoseconds)
}

const (
	//  default configs
	defaultBlockSize       = 10 * 1024 * 1024 // 10MB
	defaultBlockTimeout    = 5 * time.Second
	defaultUploadTimeout   = 300 * time.Second
	defaultMaxRetries      = 3
	defaultMaxMemoryBlocks = 10
	defaultMemoryLimit     = 100 * 1024 * 1024 // 100MB

	// key patterns
	blockKeyPrefix          = "blocks"
	blockKeyPattern         = "blocks/%010d.blk" // blocks/0000000001.blk
	consumerKeyPrefix       = "consumers"
	consumerStateKeyPattern = "consumers/%s/c.dat"
	consumerMetaKeyPattern  = "consumers/%s/meta.json"
)

var (
	ErrObjectStoreNotConfigured = NewJSStreamStoreFailedError(fmt.Errorf("object storage not configured"))
)

// newObjectStore creates a new object store with the given configuration
func newObjectStore(cfg StreamConfig, oscfg ObjectStreamConfig) (*objectStore, error) {
	if oscfg.BlockSize == 0 {
		oscfg.BlockSize = defaultBlockSize
	}
	if oscfg.BlockTimeout == 0 {
		oscfg.BlockTimeout = defaultBlockTimeout
	}
	if oscfg.UploadTimeout == 0 {
		oscfg.UploadTimeout = defaultUploadTimeout
	}
	if oscfg.MaxRetries == 0 {
		oscfg.MaxRetries = defaultMaxRetries
	}
	if oscfg.MaxMemoryBlocks == 0 {
		oscfg.MaxMemoryBlocks = defaultMaxMemoryBlocks
	}
	if oscfg.MemoryLimit == 0 {
		oscfg.MemoryLimit = defaultMemoryLimit
	}
	if oscfg.MaxConcurrentUploads == 0 {
		oscfg.MaxConcurrentUploads = 1 // default to 1 for now
	}

	client, err := newObjectStorageClient(ObjectStoreConfig{
		Bucket:          oscfg.Bucket,
		Endpoint:        oscfg.Endpoint,
		AccessKeyID:     oscfg.AccessKeyID,
		SecretAccessKey: oscfg.SecretAccessKey,
		Region:          oscfg.Region,
		PathPrefix:      oscfg.PathPrefix,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create object storage client: %w", err)
	}

	// initialize object store
	os := &objectStore{
		cfg:             cfg,
		oscfg:           oscfg,
		srv:             oscfg.srv,
		uploadingBlocks: make(map[uint32]*memoryBlock),
		currentBlock:    newMemoryBlock(1),
		client:          client,
		uploadQueue:     make(chan *uploadRequest, 1024), // TODO: chan size configurable?
		stopCh:          make(chan struct{}),
		psim:            stree.NewSubjectTree[psi](),
	}

	// TODO: only write if it doesn't exist since we may have just downloaded it
	if err := os.writeObjectStoreMeta(); err != nil {
		return nil, err
	}

	if err := os.recover(); err != nil {
		if os.srv != nil {
			os.srv.Warnf("Object store recovery failed: %v", err)
		}
		return nil, err
	}

	resultCh := make(chan uploadResult, oscfg.MaxConcurrentUploads)

	// start the uploader
	ctx, cancel := context.WithCancel(context.Background())
	os.uploader = &blockUploader{
		uploadQueue: os.uploadQueue,
		workers:     oscfg.MaxConcurrentUploads,
		ctx:         ctx,
		cancel:      cancel,
		timeout:     oscfg.UploadTimeout,
		client:      client,
		resultCh:    resultCh,
	}
	go os.uploader.start()

	go func() {
		for r := range resultCh {
			os.onBlockUploadResult(r)
		}
	}()

	return os, nil
}

func newMemoryBlock(index uint32) *memoryBlock {
	return &memoryBlock{
		meta: &blockMeta{
			blockRef: blockRef{
				index: index,
			},
		},
		messages: make([]*storedMessage, 0),
		msgIndex: make(map[uint64]*storedMessage),
		fss:      make(map[string]*SimpleState),
	}
}

func (os *objectStore) writeObjectStoreMeta() error {
	// TODO: make this a real type
	streamInfo := struct {
		Created time.Time `json:"created"`
		StreamConfig
	}{
		Created:      time.Now().UTC(),
		StreamConfig: os.cfg,
	}

	b, err := json.Marshal(streamInfo)
	if err != nil {
		return err
	}

	// TODO: write meta.sum

	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	_, err = os.client.PutObject(ctx, JetStreamMetaFile, b)
	if err != nil {
		return fmt.Errorf("failed to upload stream metadata: %w", err)
	}

	return nil
}

// shouldSealBlock determines if the current block should be sealed for upload
// lock must be held
func (os *objectStore) shouldSealBlock(block *memoryBlock) bool {
	block.meta.mu.RLock()
	defer block.meta.mu.RUnlock()

	if block.meta.count < 1 {
		return false
	}

	// if there are too many in-flight blocks, don't seal.
	// keep buffering in the current block. hopefully this is a burst
	// and the uploader will catch up when the burst is over
	// TODO: is mechanic a good idea?
	uploadingCount := len(os.uploadingBlocks)
	if uploadingCount >= os.oscfg.MaxMemoryBlocks {
		return false
	}

	if block.meta.bytes >= os.oscfg.BlockSize {
		return true
	}
	return false
}

func (os *objectStore) Type() StorageType {
	return ObjectStorage
}

func (os *objectStore) isClosed() bool {
	os.mu.RLock()
	defer os.mu.RUnlock()
	return os.closed
}

// Stop gracefully stops the object store
func (os *objectStore) Stop() error {
	os.mu.Lock()
	if os.closed {
		os.mu.Unlock()
		return nil
	}
	os.closed = true
	os.mu.Unlock()

	// Signal shutdown
	close(os.stopCh)
	os.flushCurrentBlock()

	// TODO: wait for all uploads to finish

	os.uploader.stop()

	return nil
}

// State returns the current stream state
func (os *objectStore) State() StreamState {
	os.mu.RLock()
	defer os.mu.RUnlock()
	return os.state
}

// FastState will fill in state with only the following:
// Msgs, Bytes, First and Last Sequence and Time and NumDeleted.
func (os *objectStore) FastState(state *StreamState) {
	os.mu.RLock()
	state.Msgs = os.state.Msgs
	state.Bytes = os.state.Bytes
	state.FirstSeq = os.state.FirstSeq
	state.FirstTime = os.state.FirstTime
	state.LastSeq = os.state.LastSeq
	state.LastTime = os.state.LastTime
	state.NumDeleted = os.state.NumDeleted
	os.mu.RUnlock()
}

// StoreMsg stores a message in the object store
func (os *objectStore) StoreMsg(subj string, hdr, msg []byte, ttl int64) (uint64, int64, error) {
	os.mu.Lock()
	defer os.mu.Unlock()
	seq := os.state.LastSeq + 1
	ts := time.Now().UnixNano()
	err := os.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
	if err != nil {
		return 0, 0, err
	}
	return seq, ts, nil
}

// StoreRawMsg stores a raw message with explicit sequence and timestamp
func (os *objectStore) StoreRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64, ttl int64) error {
	os.mu.Lock()
	defer os.mu.Unlock()
	return os.storeRawMsg(subj, hdr, msg, seq, ts, ttl)
}

// storeRawMsg stores a message with the provides seq and timestamp
// lock must be held
func (os *objectStore) storeRawMsg(subj string, hdr, msg []byte, seq uint64, ts int64, ttl int64) error {
	if os.closed {
		return ErrStoreClosed
	}

	if seq != os.state.LastSeq+1 {
		if seq > 0 {
			return ErrSequenceMismatch
		}
		seq = os.state.LastSeq + 1
	}

	// Create stored message
	storedMsg := &storedMessage{
		seq:  seq,
		subj: subj,
		hdr:  make([]byte, len(hdr)),
		msg:  make([]byte, len(msg)),
		ts:   ts,
	}
	copy(storedMsg.hdr, hdr)
	copy(storedMsg.msg, msg)

	block := os.currentBlock
	block.meta.mu.Lock()
	if block.meta.count == 0 && os.cfg.PersistMode == AsyncPersistMode {
		time.AfterFunc(os.oscfg.BlockTimeout, func() {
			os.mu.Lock()
			defer os.mu.Unlock()
			if os.currentBlock != block {
				// bail if the block has already been rotated
				return
			}
			os.srv.Tracef("blk %d timeout expired", block.meta.index)
			_ = os.sealAndUploadCurrentBlock()
		})
	}
	msgSize := block.addMessage(storedMsg)
	block.meta.mu.Unlock()

	// update stream state
	os.state.Msgs++
	os.state.Bytes += uint64(msgSize)
	os.state.LastSeq = storedMsg.seq
	os.state.LastTime = time.Unix(0, storedMsg.ts).UTC()

	if os.state.Msgs == 1 {
		os.state.FirstSeq = storedMsg.seq
		os.state.FirstTime = time.Unix(0, storedMsg.ts).UTC()
	}

	if os.cfg.PersistMode == AsyncPersistMode {
		if os.shouldSealBlock(os.currentBlock) {
			// when in async mode, the puback that the client receives is not a guarantee of persistence
			// we don't return the error here to align with this
			err := os.sealAndUploadCurrentBlock()
			if err != nil {
				os.srv.Errorf("failed to seal upload block %d: %v", os.currentBlock.meta.index, err)
			}
		}
	} else {
		err := os.sealAndUploadCurrentBlock() // if not async, one message per block.
		// TODO: await upload completion
		return err
	}
	return nil
}

// addMessage adds a message to the block and updates the block-level accounting
// block lock must be held
func (block *memoryBlock) addMessage(msg *storedMessage) uint32 {
	block.messages = append(block.messages, msg)
	block.msgIndex[msg.seq] = msg

	if block.meta.count == 0 {
		block.meta.first = msgId{seq: msg.seq, ts: msg.ts}
	}
	block.meta.last = msgId{seq: msg.seq, ts: msg.ts}
	block.meta.count++
	msgSize := sizeOfMsg(msg)
	block.meta.bytes += uint64(msgSize)

	// update per-subject info
	if msg.subj != "" {
		subjectInfo, exists := block.fss[msg.subj]
		if !exists {
			subjectInfo = &SimpleState{Msgs: 1, First: msg.seq, Last: msg.seq}
			block.fss[msg.subj] = subjectInfo
		} else {
			subjectInfo.Last = msg.seq
			subjectInfo.Msgs++
		}
	}
	return msgSize
}

// SkipMsg skips a message sequence number
func (os *objectStore) SkipMsg(seq uint64) (uint64, error) {
	os.mu.Lock()
	defer os.mu.Unlock()

	// Check sequence matches our last sequence.
	if seq != os.state.LastSeq+1 {
		if seq > 0 {
			return 0, ErrSequenceMismatch
		}
		seq = os.state.LastSeq + 1
	}

	os.state.LastSeq = seq
	if os.state.Msgs == 0 {
		os.state.FirstSeq = seq
	}

	// TODO: write a message to reserve the sequence

	return seq, nil
}

// SkipMsgs skips multiple message sequence numbers
func (os *objectStore) SkipMsgs(seq uint64, num uint64) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	// Skip the specified number of sequences
	for i := uint64(0); i < num; i++ {
		nextSeq := os.state.LastSeq + 1
		os.state.LastSeq = nextSeq
		if os.state.Msgs == 0 && i == 0 {
			os.state.FirstSeq = nextSeq
		}
	}

	return nil
}

// RemoveMsg removes a message (not supported in object store - messages are immutable)
func (os *objectStore) RemoveMsg(seq uint64) (bool, error) {
	return false, fmt.Errorf("message removal not supported in object store")
}

// EraseMsg erases a message (not supported in object store - messages are immutable)
func (os *objectStore) EraseMsg(seq uint64) (bool, error) {
	return false, fmt.Errorf("message erasure not supported in object store")
}

// Purge purges messages (not supported in object store - messages are immutable)
func (os *objectStore) Purge() (uint64, error) {
	return 0, fmt.Errorf("purge not supported in object store")
}

// PurgeEx purges messages with options (not supported in object store - messages are immutable)
func (os *objectStore) PurgeEx(subject string, sequence, keep uint64) (uint64, error) {
	return 0, fmt.Errorf("purge not supported in object store")
}

// Compact compacts the store
// no-op for object store
func (os *objectStore) Compact(seq uint64) (uint64, error) {
	return 0, nil
}

// FlushAllPending flushes all pending operations
func (os *objectStore) FlushAllPending() {
	os.flushCurrentBlock()
}

// Truncate truncates the stream (not supported in object store - messages are immutable)
func (os *objectStore) Truncate(seq uint64) error {
	return fmt.Errorf("truncate not supported in object store")
}

// RegisterStorageUpdates registers a callback for storage updates
func (os *objectStore) RegisterStorageUpdates(cb StorageUpdateHandler) {
	// TODO: what is this for?
}

// RegisterStorageRemoveMsg registers a callback for message removal
func (os *objectStore) RegisterStorageRemoveMsg(cb StorageRemoveMsgHandler) {
	// Store the callback - for object store this would never be called
	// since messages cannot be removed
}

// RegisterProcessJetStreamMsg registers a callback for processing JetStream messages
func (os *objectStore) RegisterProcessJetStreamMsg(cb ProcessJetStreamMsgHandler) {
	// TODO: what is this for?
}

// Utilization returns storage utilization information
func (os *objectStore) Utilization() (total, reported uint64, err error) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	// "total" includes deleted messages, which objstore doesn't support
	return os.state.Bytes, os.state.Bytes, nil
}

// StreamSnapshot creates a stream snapshot (not implemented yet)
func (os *objectStore) StreamSnapshot(deadline time.Duration, checkMsgs, includeHealthz bool) (*StreamReplicatedState, error) {
	return nil, fmt.Errorf("stream snapshot not implemented for object storage")
}

// SyncDeleted synchronizes deleted message information (no-op for object store)
func (os *objectStore) SyncDeleted(dbs DeleteBlocks) {
	// No-op for object store since messages cannot be deleted
}

// EncodedStreamState returns the encoded stream state
func (os *objectStore) EncodedStreamState(failed uint64) ([]byte, error) {
	return nil, fmt.Errorf("encoded stream state not implemented for object store")
}

// UpdateConfig updates the stream configuration
func (os *objectStore) UpdateConfig(cfg *StreamConfig) error {
	os.mu.Lock()
	defer os.mu.Unlock()

	if os.closed {
		return ErrStoreClosed
	}

	os.cfg = *cfg
	os.writeObjectStoreMeta()
	return nil
}

// ResetState resets "temporary" state. no-op for object store
func (os *objectStore) ResetState() {
}

// Snapshot creates a snapshot of the stream
func (os *objectStore) Snapshot(deadline time.Duration, includeConsumers, checkMsgs bool) (*SnapshotResult, error) {
	return nil, fmt.Errorf("snapshot not implemented for object storage")
}

// AddConsumer adds a consumer (not supported in object store - consumers managed separately)
func (os *objectStore) AddConsumer(o ConsumerStore) error {
	os.mu.Lock()
	defer os.mu.Unlock()
	os.state.Consumers++
	return nil
}

// RemoveConsumer removes a consumer (not supported in object store - consumers managed separately)
func (os *objectStore) RemoveConsumer(o ConsumerStore) error {
	os.mu.Lock()
	defer os.mu.Unlock()
	os.state.Consumers--
	return nil
}

// GetSeqFromTime looks for the first sequence number that has
// the message with >= timestamp.
func (os *objectStore) GetSeqFromTime(t time.Time) uint64 {
	if os.state.LastTime.Before(t) {
		return os.state.LastSeq + 1
	}

	ts := t.UnixNano()
	for _, blk := range os.blocks {
		blk.mu.RLock()
		found := ts <= blk.last.ts
		blk.mu.RUnlock()
		if found {
			// TODO: same dumb linear search as the filestore
			block, err := os.downloadAndUnmarshalBlock(blk.index)
			if err != nil {
				return 0
			}
			for _, msg := range block.messages {
				if msg.ts >= ts {
					return msg.seq
				}
			}
		}
	}
	return 0
}

// FilteredState returns filtered state for a sequence and subject
func (os *objectStore) FilteredState(seq uint64, subject string) SimpleState {
	// not implemented
	return SimpleState{}
}

// SubjectsState returns state for all subjects matching the filter
func (os *objectStore) SubjectsState(filterSubject string) map[string]SimpleState {
	// not implemented
	return make(map[string]SimpleState)
}

// SubjectsTotals returns totals for all subjects matching the filter
func (os *objectStore) SubjectsTotals(filter string) map[string]uint64 {
	os.mu.RLock()
	defer os.mu.RUnlock()

	if os.psim.Size() == 0 {
		return nil
	}
	// Match all if no filter given.
	if filter == _EMPTY_ {
		filter = fwcs
	}
	fst := make(map[string]uint64)
	os.psim.Match(stringToBytes(filter), func(subj []byte, psi *psi) {
		fst[string(subj)] = psi.total
	})
	return fst
}

// AllLastSeqs returns all last sequences
func (os *objectStore) AllLastSeqs() ([]uint64, error) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	if os.state.Msgs == 0 {
		return nil, nil
	}

	numSubjects := os.psim.Size()
	seqs := make([]uint64, 0, numSubjects)
	subs := make(map[string]struct{}, numSubjects)

	os.fss.IterFast(func(bsubj []byte, ss *SimpleState) bool {
		// Check if already been processed and accounted.
		if _, ok := subs[string(bsubj)]; !ok {
			seqs = append(seqs, ss.Last)
			subs[string(bsubj)] = struct{}{}
		}
		return true
	})

	slices.Sort(seqs)
	return seqs, nil
}

// MultiLastSeqs returns last sequences for multiple filters
// TODO: no idea if this is implemented correctly
func (os *objectStore) MultiLastSeqs(filters []string, maxSeq uint64, maxAllowed int) ([]uint64, error) {
	return nil, errors.New("not implemented")
}

// SubjectForSeq returns the subject for a given sequence number
func (os *objectStore) SubjectForSeq(seq uint64) (string, error) {
	msg, err := os.LoadMsg(seq, nil)
	if err != nil {
		return "", err
	}

	return msg.subj, nil
}

// NumPending returns the number of pending messages
func (os *objectStore) NumPending(sseq uint64, filter string, lastPerSubject bool) (total, validThrough uint64) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	if os.state.Msgs == 0 || sseq > os.state.LastSeq {
		return 0, os.state.LastSeq
	}

	// TODO: respect the filter
	total = os.state.LastSeq - sseq + 1
	if sseq < os.state.FirstSeq {
		total = os.state.LastSeq - os.state.FirstSeq + 1
	}

	return total, os.state.LastSeq
}

// NumPendingMulti returns the number of pending messages using multiple filters
// TODO: no idea if this is implemented correctly
func (os *objectStore) NumPendingMulti(sseq uint64, sl *gsl.SimpleSublist, lastPerSubject bool) (total, validThrough uint64) {
	if sl == nil {
		return os.NumPending(sseq, "", lastPerSubject)
	}

	os.mu.RLock()
	defer os.mu.RUnlock()

	if os.state.Msgs == 0 || sseq > os.state.LastSeq {
		return 0, os.state.LastSeq
	}

	// TODO: respect the filter

	total = os.state.LastSeq - sseq + 1
	if sseq < os.state.FirstSeq {
		total = os.state.LastSeq - os.state.FirstSeq + 1
	}

	return total, os.state.LastSeq
}

// sealAndUploadCurrentBlock seals the current block and starts upload
// lock must be held
func (os *objectStore) sealAndUploadCurrentBlock() error {
	block := os.currentBlock
	block.meta.mu.Lock()
	defer block.meta.mu.Unlock()

	if os.currentBlock.meta.count == 0 {
		block.meta.mu.Unlock()
		return nil
	}

	// move current block to uploading state
	os.uploadingBlocks[block.meta.index] = block
	nextIndex := block.meta.index + 1
	os.currentBlock = newMemoryBlock(nextIndex)

	uploadReq := &uploadRequest{
		block: block,
		try:   1,
	}

	select {
	case os.uploadQueue <- uploadReq:
	default:
		os.srv.Errorf("upload queue full. dropping block %d", block.meta.index)
		return errors.New("upload queue full")
	}
	return nil
}

func (os *objectStore) onBlockUploadResult(result uploadResult) {
	if result.err == nil {
		os.mu.Lock()
		defer os.mu.Unlock()
		blk := os.uploadingBlocks[result.index]
		os.addBlockToMetadata(blk)
		delete(os.uploadingBlocks, result.index)
	} else {
		if result.try >= os.oscfg.MaxRetries {
			os.srv.Errorf("block %d upload max retries reached: last error: %v", result.index, result.err)
			return
		}
		os.uploadQueue <- &uploadRequest{
			block: os.uploadingBlocks[result.index],
			try:   result.try + 1,
		}
	}
}

// flushCurrentBlock flushes any pending messages in the current block
func (os *objectStore) flushCurrentBlock() {
	os.mu.RLock()
	currentBlock := os.currentBlock
	os.mu.RUnlock()

	if currentBlock != nil && currentBlock.meta.count > 0 {
		os.sealAndUploadCurrentBlock()
	}
}

// LoadNextMsg loads the next message after the given sequence
func (os *objectStore) LoadNextMsg(filter string, wc bool, start uint64, sm *StoreMsg) (*StoreMsg, uint64, error) {
	os.mu.RLock()
	defer os.mu.RUnlock()

	if os.closed {
		return nil, 0, ErrStoreClosed
	}

	if os.state.Msgs == 0 || start > os.state.LastSeq {
		return nil, os.state.LastSeq, ErrStoreEOF
	}

	if filter == "" {
		filter = fwcs
	}

	for seq := start; seq <= os.state.LastSeq; seq++ {
		sm, err := os.LoadMsg(seq, sm)
		if err != nil {
			if err == ErrStoreMsgNotFound {
				// HACK:
				// object store does not support deleting messages, so if a seq <= LastSeq is
				// not found, it just hasn't been uploaded yet.
				// don't advance the consumer's sequence past this sequence.
				// return EOF with the previous seq so that the consumer will retry later
				// waiting for it to be uploaded.
				return nil, seq - 1, ErrStoreEOF
			}
			return nil, 0, err
		}

		if SubjectMatchesFilter(sm.subj, filter) {
			return sm, sm.seq, nil
		}
	}

	return nil, os.state.LastSeq, ErrStoreEOF
}

// LoadPrevMsg loads the previous message before the given sequence
func (os *objectStore) LoadPrevMsg(start uint64, smp *StoreMsg) (sm *StoreMsg, err error) {
	if os.isClosed() {
		return nil, ErrStoreClosed
	}

	os.mu.RLock()
	firstSeq := os.state.FirstSeq
	os.mu.RUnlock()

	// Search backwards for the previous available message
	for prevSeq := start - 1; prevSeq >= firstSeq && prevSeq > 0; prevSeq-- {
		msg, err := os.LoadMsg(prevSeq, smp)
		if err != nil {
			if err == ErrStoreMsgNotFound {
				continue // Message might be deleted/skipped
			}
			return nil, err
		}

		return msg, nil
	}

	return nil, ErrStoreMsgNotFound
}

// LoadNextMsgMulti loads the next message after the given sequence using multiple filters
// TODO: no idea if this is implemented correctly
func (os *objectStore) LoadNextMsgMulti(sl *gsl.SimpleSublist, start uint64, smp *StoreMsg) (sm *StoreMsg, skip uint64, err error) {
	if os.isClosed() {
		return nil, 0, ErrStoreClosed
	}

	os.mu.RLock()
	lastSeq := os.state.LastSeq
	os.mu.RUnlock()

	// Search for the next available message
	for nextSeq := start + 1; nextSeq <= lastSeq; nextSeq++ {
		msg, err := os.LoadMsg(nextSeq, smp)
		if err != nil {
			if err == ErrStoreMsgNotFound {
				skip++
				continue // Message might be deleted/skipped
			}
			return nil, skip, err
		}

		if sl != nil && !sl.HasInterest(msg.subj) {
			skip++
			continue
		}

		return msg, skip, nil
	}

	return nil, skip, ErrStoreEOF
}

// LoadMsg loads a message by sequence number
func (os *objectStore) LoadMsg(seq uint64, sm *StoreMsg) (*StoreMsg, error) {
	os.mu.RLock()
	if os.closed {
		return nil, ErrStoreClosed
	}
	// Check if sequence is in valid range
	if seq < os.state.FirstSeq || seq > os.state.LastSeq {
		os.mu.RUnlock()
		return nil, ErrStoreMsgNotFound
	}
	os.mu.RUnlock()

	msg, err := os.downloadAndUnmarshalMsg(seq)
	if err != nil {
		return nil, err
	}

	if sm == nil {
		sm = new(StoreMsg)
	} else {
		sm.clear()
	}

	sm.subj = msg.subj
	sm.seq = msg.seq
	sm.hdr = msg.hdr
	sm.msg = msg.msg
	sm.ts = msg.ts

	return sm, nil
}

// LoadLastMsg loads the last message for a given subject
func (os *objectStore) LoadLastMsg(subject string, sm *StoreMsg) (*StoreMsg, error) {
	if subject == "" || subject == fwcs {
		os.mu.RLock()
		last := os.state.LastSeq
		os.mu.RUnlock()
		return os.LoadMsg(last, sm)
	}

	if os.isClosed() {
		return nil, ErrStoreClosed
	}

	ss, exists := os.fss.Find(stringToBytes(subject))
	if !exists {
		return nil, ErrStoreMsgNotFound
	}

	return os.LoadMsg(ss.Last, sm)
}

// findMsgBlockWithSeq binary searches through the blocks to find the one that contains a given seq
func (os *objectStore) findMsgBlockWithSeq(seq uint64) *blockMeta {
	left, right := 0, len(os.blocks)-1
	for left <= right {
		mid := (left + right) / 2
		block := os.blocks[mid]

		if seq < block.first.seq {
			right = mid - 1
		} else if seq > block.last.seq {
			left = mid + 1
		} else {
			return block
		}
	}

	return nil
}

func (os *objectStore) downloadAndUnmarshalBlock(index uint32) (*memoryBlock, error) {
	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	key := fmt.Sprintf(blockKeyPattern, index)
	data, err := os.client.GetObject(ctx, key)
	if err != nil {
		return nil, err
	}

	return unmarshalBlock(data)
}

func (os *objectStore) downloadAndUnmarshalMsg(seq uint64) (*storedMessage, error) {
	// find which block contains this sequence
	blockMeta := os.findMsgBlockWithSeq(seq)
	if blockMeta == nil {
		return nil, ErrStoreMsgNotFound
	}

	// download the block metadata to get the offsets of the message index
	key := fmt.Sprintf(blockKeyPattern, blockMeta.index)

	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()
	data, err := os.client.GetObjectRange(ctx, key, 0, -blockHeaderSize)
	if err != nil {
		return nil, err
	}
	footer, err := unmarshalBlockFooter(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}

	// download the index
	ctx, cancel = context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()
	data, err = os.client.GetObjectRange(
		ctx,
		key,
		int64(footer.indexOffset),
		int64(footer.indexOffset+footer.indexLen))
	if err != nil {
		return nil, err
	}
	index, err := unmarshalMessageIndex(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	entryIdx, found := slices.BinarySearchFunc(index, seq, func(e messageIndexEntry, t uint64) int {
		return cmp.Compare(e.seq, t)
	})

	if !found {
		return nil, ErrStoreMsgNotFound
	}

	// download the message
	ctx, cancel = context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()
	data, err = os.client.GetObjectRange(
		ctx,
		key,
		int64(index[entryIdx].offset),
		int64(index[entryIdx].offset+index[entryIdx].size))
	if err != nil {
		return nil, err
	}
	return unmarshalMessage(bytes.NewReader(data))
}

// Delete removes the stream and all its data from the remote object storage
func (os *objectStore) Delete(inline bool) error {
	if err := os.Stop(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	return os.client.DeleteObjects(ctx, "")
}

// addBlockToMetadata adds a newly uploaded or restored block to stream-level accounting
// blocks are not visible to consumers until this is called
// lock must be taken
func (os *objectStore) addBlockToMetadata(block *memoryBlock) {
	os.blocks = append(os.blocks, block.meta)

	for idx, msg := range block.messages {
		os.srv.Tracef("  block %d, idx %d, seq %d", block.meta.index, idx, msg.seq)
		if info, ok := os.psim.Find(stringToBytes(msg.subj)); ok {
			info.total++
			if block.meta.index > info.lblk {
				info.lblk = block.meta.index
			}
		} else {
			os.psim.Insert(stringToBytes(msg.subj), psi{total: 1, fblk: block.meta.index, lblk: block.meta.index})
		}

		if ss, ok := os.fss.Find(stringToBytes(msg.subj)); ok && ss != nil {
			ss.Msgs++
			ss.Last = msg.seq
			ss.lastNeedsUpdate = false
		} else {
			os.fss.Insert(stringToBytes(msg.subj), SimpleState{Msgs: 1, First: msg.seq, Last: msg.seq})
		}

		// TODO: update state.NumSubjects
		// TODO: update state.Subjects
	}
}
