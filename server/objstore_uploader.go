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
	"errors"
	"fmt"
	"sync"
	"time"
)

// Block uploader manages parallel upload workers
type blockUploader struct {
	timeout     time.Duration
	client      ObjectStorageClient
	uploadQueue chan *uploadRequest
	workers     int
	ctx         context.Context
	cancel      context.CancelFunc
	wg          sync.WaitGroup
	resultCh    chan uploadResult
}

type uploadRequest struct {
	block *memoryBlock
	try   int
}

type uploadResult struct {
	index uint32
	err   error
	try   int
}

func (bu *blockUploader) start() {
	bu.wg.Add(bu.workers)
	for range bu.workers {
		go bu.processUploadQueue()
	}
}

// stop kills in-progress uploads and blocks until the workers have exited
func (bu *blockUploader) stop() {
	bu.cancel()
	bu.wg.Wait()
}

func (bu *blockUploader) processUploadQueue() {
	defer bu.wg.Done()
	for {
		select {
		case req, ok := <-bu.uploadQueue:
			if !ok {
				return
			}
			bu.handleUploadRequest(req)
		case <-bu.ctx.Done():
			return
		}
	}
}

func (bu *blockUploader) handleUploadRequest(req *uploadRequest) {
	data, err := marshalBlock(req.block)
	if err != nil {
		bu.resultCh <- uploadResult{
			index: req.block.meta.index,
			err:   fmt.Errorf("failed to serialize block: %w", err),
			try:   req.try,
		}
		return
	}
	key := fmt.Sprintf(blockKeyPattern, req.block.meta.index)
	ctx, cancel := context.WithTimeout(bu.ctx, bu.timeout)
	defer cancel()
	etag, err := bu.client.PutObject(ctx, key, data)
	if err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		bu.resultCh <- uploadResult{
			index: req.block.meta.index,
			err:   err,
			try:   req.try,
		}
	}
	req.block.meta.etag = etag
	bu.resultCh <- uploadResult{
		index: req.block.meta.index,
		try:   req.try,
	}
}
