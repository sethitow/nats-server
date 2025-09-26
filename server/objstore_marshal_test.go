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
	"testing"
)

func TestMarshalUnmarshalBlock(t *testing.T) {
	blk := newMemoryBlock(1)
	msg := storedMessage{
		seq:  1,
		subj: "foo.bar",
		hdr:  []byte{},
		msg:  []byte("onefish"),
		ts:   1,
	}
	blk.addMessage(&msg)

	buf, err := marshalBlock(blk)
	if err != nil {
		t.Fatalf("failed to serialize block: %v", err)
	}

	newBlk, err := unmarshalBlock(buf)
	if err != nil {
		t.Fatalf("failed to parse block: %v", err)
	}

	if len(newBlk.messages) != len(blk.messages) {
		t.Fatalf("length of message list does not match: %v", err)
	}
}

func TestMarshalUnmarshalBlockFooter(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	err := marshalBlockFooter(buf, &blockMeta{
		first: msgId{
			seq: 1,
			ts:  2,
		},
		last: msgId{
			seq: 3,
			ts:  4,
		},
		bytes: 5,
		count: 6,
	}, 7, 8, 9, 10)
	if err != nil {
		t.Fatalf("failed to marshal footer: %v", err)
	}

	f, err := unmarshalBlockFooter(bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("failed to unmarshal footer: %v", err)
	}
	if f.first.seq != 1 {
		t.Fatalf("failed to unmarshal footer: %v", err)
	}
	if f.last.seq != 3 {
		t.Fatalf("failed to unmarshal footer: %v", err)
	}
}

func TestSizeOfMsg(t *testing.T) {
	msg := storedMessage{
		seq:  1,
		subj: "foo.bar",
		hdr:  []byte{},
		msg:  []byte("onefish"),
		ts:   1,
	}

	buf := bytes.NewBuffer(nil)
	err := marshalMessage(buf, &msg)
	if err != nil {
		t.Fatalf("failed to serialize block: %v", err)
	}

	if sizeOfMsg(&msg) != uint32(buf.Len()) {
		t.Fatalf("marshalled length is mismatched from sizeof")
	}
}
