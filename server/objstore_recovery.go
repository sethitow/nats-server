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
	"fmt"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
)

// recover attempts to recover stream state from the object storage
// if metadata doesn't exist, stream is new. do nothing and return no error.
func (os *objectStore) recover() error {
	_, err := os.downloadMeta()
	if err != nil {
		if isObjNotFound(err) {
			// new stream - no metadata exists yet
			return nil
		}
		return fmt.Errorf("failed to load stream metadata: %w", err)
	}

	discoveredBlocks, err := os.discoverBlocks()
	if err != nil {
		return fmt.Errorf("failed to discover blocks: %w", err)
	}

	err = os.rebuildStreamState(discoveredBlocks)
	if err != nil {
		return err
	}

	return nil
}

// downloadMeta loads stream metadata from object storage
func (os *objectStore) downloadMeta() (*FileStreamInfo, error) {
	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	data, err := os.client.GetObject(ctx, JetStreamMetaFile)
	if err != nil {
		return nil, err
	}

	var metadata FileStreamInfo
	if err := json.Unmarshal(data, &metadata); err != nil {
		return nil, fmt.Errorf("failed to parse stream metadata JSON: %w", err)
	}

	// silly checks
	if metadata.Name != os.cfg.Name {
		return nil, fmt.Errorf("metadata stream name mismatch: expected %s, got %s",
			os.cfg.Name, metadata.Name)
	}

	return &metadata, nil
}

// discoverBlocks discovers all block files in the object storage
func (os *objectStore) discoverBlocks() ([]blockRef, error) {
	ctx, cancel := context.WithTimeout(context.Background(), os.oscfg.UploadTimeout)
	defer cancel()

	objects, err := os.client.ListObjects(ctx, blockKeyPrefix)
	if err != nil {
		return nil, err
	}

	var blocks []blockRef
	for _, obj := range objects {
		if !strings.HasSuffix(obj.Key, blkSuffix) {
			os.srv.Warnf("Object store: invalid block file extension %s: %v", obj.Key, err)
			continue
		}

		filename := path.Base(obj.Key)
		indexStr := strings.TrimSuffix(filename, blkSuffix)

		blockIndex, err := strconv.ParseUint(indexStr, 10, 64)
		if err != nil {
			os.srv.Warnf("Object store: invalid block filename %s: %v", filename, err)
			continue
		}

		ref := blockRef{
			index: uint32(blockIndex),
			key:   obj.Key,
			etag:  obj.ETag,
		}

		blocks = append(blocks, ref)
	}

	// TODO: is this needed? not sure if store guarantees order when listing
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].index < blocks[j].index
	})

	return blocks, nil
}

// rebuildStreamState reconstructs stream state from recovered blocks
func (os *objectStore) rebuildStreamState(blockRefs []blockRef) error {
	for refIdx, ref := range blockRefs {
		blk, err := os.downloadAndUnmarshalBlock(ref.index)
		if err != nil {
			os.srv.Errorf("  failed to parse object store block %d", ref.key)
			return err
		}

		if int(blk.meta.count) != len(blk.messages) {
			os.srv.Errorf("  mismatched message count: blk %d, expected %d, found %d", blk.meta.index, blk.meta.count, len(blk.messages))
		}

		os.state.Msgs += blk.meta.count
		os.state.Bytes += blk.meta.bytes

		if refIdx == 0 {
			os.state.FirstSeq = blk.meta.first.seq
			os.state.FirstTime = time.Unix(0, blk.meta.first.ts).UTC()
		}

		os.addBlockToMetadata(blk)
		os.currentBlock.meta.index = blk.meta.index + 1

		os.state.LastSeq = blk.meta.last.seq
		os.state.LastTime = time.Unix(0, blk.meta.last.ts).UTC()

		// silly checks
		meta := blk.meta
		if blk.messages[0].seq != meta.first.seq {
			os.srv.Errorf("  first message seq does not match metadata: blk %d, metadata seq %d, msg seq %d", meta.index, meta.first.seq, blk.messages[0].seq)
		}
		if blk.messages[0].ts != meta.first.ts {
			os.srv.Errorf("  first message ts does not match metadata: blk %d, metadata ts %d, msg ts %d", meta.index, meta.first.ts, blk.messages[0].ts)
		}
		if blk.messages[len(blk.messages)-1].seq != meta.last.seq {
			os.srv.Errorf("  last message seq does not match metadata: blk %d, metadata seq %d, msg seq %d", meta.index, meta.last.seq, blk.messages[len(blk.messages)-1].seq)
		}
		if blk.messages[len(blk.messages)-1].ts != meta.last.ts {
			os.srv.Errorf("  last message ts does not match metadata: blk %d, metadata ts %d, msg ts %d", meta.index, meta.last.ts, blk.messages[len(blk.messages)-1].ts)
		}
		var lseq uint64
		var lts int64
		for idx, msg := range blk.messages {
			if !(msg.seq > lseq) {
				os.srv.Errorf("  msg seq is not monotonic: blk %d, idx %d, seq %d", meta.index, idx, msg.seq)
			}
			if !(msg.ts > lts) {
				os.srv.Errorf("  msg ts is not monotonic: blk %d, idx %d, seq %d", meta.index, idx, msg.seq)
			}
		}

	}
	return nil
}
