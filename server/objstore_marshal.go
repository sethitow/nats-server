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
	"encoding/binary"
	"fmt"
)

const (
	objStoreMagic   = uint32(0x4E415453)
	objStoreVersion = uint32(1) // v1 is an 84 byte footer

	blockHeaderSize = 84
)

// messageIndexEntry represents an entry in the message index
type messageIndexEntry struct {
	seq    uint64
	offset uint32
	size   uint32
}

// blockFooter is a fixed size chunk that's written at the end of the block
// using uint32 for offsets means a max block size of 4gb
type blockFooter struct {
	magic uint32

	first msgId
	last  msgId
	count uint64
	bytes uint64

	indexOffset uint32
	indexLen    uint32

	subjectOffset uint32
	subjectLen    uint32

	index   uint32
	version uint32
	hash    [8]byte
}

// unmarshalBlock parses binary block data into a structured format
func unmarshalBlock(data []byte) (*memoryBlock, error) {
	if len(data) < blockHeaderSize {
		return nil, fmt.Errorf("block data too small: %d bytes", len(data))
	}
	buf := bytes.NewReader(data[len(data)-blockHeaderSize:])
	footer, err := unmarshalBlockFooter(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse block header: %w", err)
	}

	// parse subject index
	if int(footer.subjectOffset+footer.subjectLen+blockHeaderSize) != len(data) {
		return nil, fmt.Errorf("data mismatch")
	}

	// basic validation
	if footer.magic != objStoreMagic {
		return nil, fmt.Errorf("invalid block magic: got 0x%x, expected 0x%x", footer.magic, objStoreMagic)
	}
	if footer.version != objStoreVersion {
		return nil, fmt.Errorf("unsupported block version: got %d, expected %d", footer.version, objStoreVersion)
	}

	// parse message index
	buf = bytes.NewReader(data[footer.indexOffset : footer.indexOffset+footer.indexLen])
	messageIndex, err := unmarshalMessageIndex(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message index: %w", err)
	}

	// parse msgs using the index
	buf = bytes.NewReader(data[0:footer.indexOffset])
	msgs, msgIndex, err := unmarshalMessages(buf, messageIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to parse messages: %w", err)
	}

	buf = bytes.NewReader(data[footer.subjectOffset : footer.subjectOffset+footer.subjectLen])
	fss, err := unmarshalSubjectIndex(buf)
	if err != nil {
		return nil, fmt.Errorf("failed to parse subject index: %w", err)
	}

	return &memoryBlock{
		meta: &blockMeta{
			blockRef: blockRef{
				index: footer.index,
			},
			first: footer.first,
			last:  footer.last,
			bytes: footer.bytes,
			count: footer.count,
		},
		fss:      fss,
		messages: msgs,
		msgIndex: msgIndex,
	}, nil
}

// unmarshalBlockFooter parses the block header
func unmarshalBlockFooter(buf *bytes.Reader) (*blockFooter, error) {
	footer := &blockFooter{}

	// Read magic number (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.magic); err != nil {
		return nil, err
	}

	// Read first message info (16 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.first.seq); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.first.ts); err != nil {
		return nil, err
	}

	// Read last message info (16 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.last.seq); err != nil {
		return nil, err
	}
	if err := binary.Read(buf, binary.LittleEndian, &footer.last.ts); err != nil {
		return nil, err
	}

	// Read message count (8 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.count); err != nil {
		return nil, err
	}

	// Read block size (8 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.bytes); err != nil {
		return nil, err
	}

	// message index offset (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.indexOffset); err != nil {
		return nil, err
	}

	// message index len (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.indexLen); err != nil {
		return nil, err
	}

	// subject index offset (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.subjectOffset); err != nil {
		return nil, err
	}

	// subject index len (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.subjectLen); err != nil {
		return nil, err
	}

	// Read block index (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.index); err != nil {
		return nil, err
	}

	// Read checksum (8 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.hash); err != nil {
		return nil, err
	}
	// TODO: check hash

	// Read version (4 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &footer.version); err != nil {
		return nil, err
	}

	return footer, nil
}

// unmarshalMessageIndex parses the message index section
func unmarshalMessageIndex(buf *bytes.Reader) ([]messageIndexEntry, error) {
	// number of messages (4 bytes)
	var numMessages uint32
	if err := binary.Read(buf, binary.LittleEndian, &numMessages); err != nil {
		return nil, err
	}

	// message index entries
	entries := make([]messageIndexEntry, numMessages)
	for i := uint32(0); i < numMessages; i++ {
		// Sequence number (8 bytes)
		if err := binary.Read(buf, binary.LittleEndian, &entries[i].seq); err != nil {
			return nil, err
		}

		// Offset (4 bytes)
		if err := binary.Read(buf, binary.LittleEndian, &entries[i].offset); err != nil {
			return nil, err
		}

		// Size (4 bytes)
		if err := binary.Read(buf, binary.LittleEndian, &entries[i].size); err != nil {
			return nil, err
		}
	}

	return entries, nil
}

// unmarshalMessages parses all messages using the message index
func unmarshalMessages(buf *bytes.Reader, messageIndex []messageIndexEntry) ([]*storedMessage, map[uint64]*storedMessage, error) {
	messages := make([]*storedMessage, len(messageIndex))
	msgIndex := make(map[uint64]*storedMessage)

	for idx, entry := range messageIndex {
		msg, err := unmarshalMessage(buf)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse message %d at offset %d: %w", entry.seq, entry.offset, err)
		}
		messages[idx] = msg
		msgIndex[entry.seq] = msg
	}

	return messages, msgIndex, nil
}

// unmarshalMessage parses a single message from binary data
// TODO: this is a lot of copying, try to make this zero copy for the msg and hdr
func unmarshalMessage(buf *bytes.Reader) (*storedMessage, error) {
	msg := &storedMessage{}

	// Read sequence number (8 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &msg.seq); err != nil {
		return nil, err
	}

	// Read timestamp (8 bytes)
	if err := binary.Read(buf, binary.LittleEndian, &msg.ts); err != nil {
		return nil, err
	}

	// Read subject length and subject
	var subjLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &subjLen); err != nil {
		return nil, err
	}

	if subjLen > 0 {
		subjBytes := make([]byte, subjLen)
		if _, err := buf.Read(subjBytes); err != nil {
			return nil, err
		}
		msg.subj = string(subjBytes)
	}

	// Read header length and header
	var hdrLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &hdrLen); err != nil {
		return nil, err
	}

	if hdrLen > 0 {
		msg.hdr = make([]byte, hdrLen)
		if _, err := buf.Read(msg.hdr); err != nil {
			return nil, err
		}
	}

	// Read message length and message
	var msgLen uint32
	if err := binary.Read(buf, binary.LittleEndian, &msgLen); err != nil {
		return nil, err
	}

	if msgLen > 0 {
		msg.msg = make([]byte, msgLen)
		if _, err := buf.Read(msg.msg); err != nil {
			return nil, err
		}
	}

	// Read and verify message checksum (8 bytes)
	// TODO: verify hash
	hash := [8]byte{}
	if _, err := buf.Read(hash[:]); err != nil {
		return nil, err
	}

	return msg, nil
}

// unmarshalSubjectIndex parses the subject index section
func unmarshalSubjectIndex(buf *bytes.Reader) (map[string]*SimpleState, error) {
	subjects := make(map[string]*SimpleState)

	// Read number of subjects (4 bytes)
	var numSubjects uint32
	if err := binary.Read(buf, binary.LittleEndian, &numSubjects); err != nil {
		return nil, err
	}

	// Read subject entries
	for i := uint32(0); i < numSubjects; i++ {
		// Read subject length (4 bytes)
		var subjLen uint32
		if err := binary.Read(buf, binary.LittleEndian, &subjLen); err != nil {
			return nil, err
		}

		// Read subject name
		subjBytes := make([]byte, subjLen)
		if _, err := buf.Read(subjBytes); err != nil {
			return nil, err
		}
		subject := string(subjBytes)

		// Read subject info
		info := &SimpleState{}
		if err := binary.Read(buf, binary.LittleEndian, &info.First); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &info.Last); err != nil {
			return nil, err
		}
		if err := binary.Read(buf, binary.LittleEndian, &info.Msgs); err != nil {
			return nil, err
		}

		subjects[subject] = info
	}

	return subjects, nil
}

func marshalBlock(block *memoryBlock) ([]byte, error) {
	if len(block.messages) == 0 {
		return nil, fmt.Errorf("cannot serialize empty block")
	}

	// TODO: fix this preallocation math
	buf := bytes.NewBuffer(make([]byte, 0, block.meta.bytes+1024))

	// write messages and keep track of the offsets to write the index later
	messageOffsets := make([]messageIndexEntry, len(block.messages))
	for i, msg := range block.messages {
		messageOffsets[i].seq = msg.seq
		messageOffsets[i].offset = uint32(buf.Len())
		err := marshalMessage(buf, msg)
		if err != nil {
			return nil, fmt.Errorf("failed to write message %d: %w", i, err)
		}
		messageOffsets[i].size = sizeOfMsg(msg)
	}

	// write message index
	indexOffset := uint32(buf.Len())
	if err := marshalMessageIndex(buf, messageOffsets); err != nil {
		return nil, fmt.Errorf("failed to write message index: %w", err)
	}
	indexLen := uint32(buf.Len()) - indexOffset

	// write subject index
	subjectOffset := uint32(buf.Len())
	if err := marshalSubjectIndex(buf, block.fss); err != nil {
		return nil, fmt.Errorf("failed to write subject index: %w", err)
	}
	subjectLen := uint32(buf.Len()) - subjectOffset

	// write block footer
	if err := marshalBlockFooter(buf, block.meta, indexOffset, indexLen, subjectOffset, subjectLen); err != nil {
		return nil, fmt.Errorf("failed to write block footer: %w", err)
	}

	return buf.Bytes(), nil
}

// marshalBlockHeader writes the block header
func marshalBlockFooter(buf *bytes.Buffer, bm *blockMeta, indexOffset, indexLen, subjOffset, subLen uint32) error {
	// Magic number (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, objStoreMagic); err != nil {
		return err
	}

	// First message seq and timestamp (16 bytes)
	if err := binary.Write(buf, binary.LittleEndian, bm.first.seq); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, bm.first.ts); err != nil {
		return err
	}

	// Last message seq and timestamp (16 bytes)
	if err := binary.Write(buf, binary.LittleEndian, bm.last.seq); err != nil {
		return err
	}
	if err := binary.Write(buf, binary.LittleEndian, bm.last.ts); err != nil {
		return err
	}

	// Message count (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, bm.count); err != nil {
		return err
	}

	// Block size in bytes (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, bm.bytes); err != nil {
		return err
	}

	// Message index offset (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, indexOffset); err != nil {
		return err
	}

	// Message index length (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, indexLen); err != nil {
		return err
	}

	// Subject index offset (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, subjOffset); err != nil {
		return err
	}

	// Subject index length (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, subLen); err != nil {
		return err
	}

	// Block index (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, bm.index); err != nil {
		return err
	}

	// TODO: hash the metadata
	hash := [8]byte{}
	if _, err := buf.Write(hash[:]); err != nil {
		return err
	}

	// Version (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, objStoreVersion); err != nil {
		return err
	}

	return nil
}

// calculates the size of a message as stored in a block
// TODO: test against marshalMessage
func sizeOfMsg(msg *storedMessage) uint32 {
	var size uint32
	size += 8                             // seq
	size += 8                             // ts
	size += 4                             // subj len
	size += uint32(len([]byte(msg.subj))) // subj
	size += 4                             // hdr len
	size += uint32(len(msg.hdr))          // hdr
	size += 4                             // msg len
	size += uint32(len(msg.msg))          // msg
	size += 8                             // hash
	return size
}

// marshalMessage writes a single message to the buffer
func marshalMessage(buf *bytes.Buffer, msg *storedMessage) error {
	// sequence number (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, msg.seq); err != nil {
		return err
	}

	// timestamp (8 bytes)
	if err := binary.Write(buf, binary.LittleEndian, msg.ts); err != nil {
		return err
	}

	// subject length and subject (variable)
	subjBytes := []byte(msg.subj)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(subjBytes))); err != nil {
		return err
	}
	if _, err := buf.Write(subjBytes); err != nil {
		return err
	}

	// header length and header (variable)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(msg.hdr))); err != nil {
		return err
	}
	if len(msg.hdr) > 0 {
		if _, err := buf.Write(msg.hdr); err != nil {
			return err
		}
	}

	// message length and message (variable)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(msg.msg))); err != nil {
		return err
	}
	if len(msg.msg) > 0 {
		if _, err := buf.Write(msg.msg); err != nil {
			return err
		}
	}

	// hash (8 bytes)
	// TODO: hash message
	hash := [8]byte{}
	if _, err := buf.Write(hash[:]); err != nil {
		return err
	}

	return nil
}

// marshalMessageIndex writes the message index
func marshalMessageIndex(buf *bytes.Buffer, offsets []messageIndexEntry) error {
	// Number of messages (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(offsets))); err != nil {
		return err
	}

	// For each message: seq, offset, size
	for _, msg := range offsets {
		// Sequence number (8 bytes)
		if err := binary.Write(buf, binary.LittleEndian, msg.seq); err != nil {
			return err
		}

		// Offset in block (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, msg.offset); err != nil {
			return err
		}

		// Message size (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, msg.size); err != nil {
			return err
		}
	}

	return nil
}

// marshalSubjectIndex writes the subject index
func marshalSubjectIndex(buf *bytes.Buffer, subjects map[string]*SimpleState) error {
	// Number of subjects (4 bytes)
	if err := binary.Write(buf, binary.LittleEndian, uint32(len(subjects))); err != nil {
		return err
	}

	// For each subject: name, first seq, last seq, count
	for subj, info := range subjects {
		subjBytes := []byte(subj)

		// Subject length (4 bytes)
		if err := binary.Write(buf, binary.LittleEndian, uint32(len(subjBytes))); err != nil {
			return err
		}

		// Subject name (variable)
		if _, err := buf.Write(subjBytes); err != nil {
			return err
		}

		// First sequence (8 bytes) - use block's first sequence
		if err := binary.Write(buf, binary.LittleEndian, info.First); err != nil {
			return err
		}

		// Last sequence (8 bytes) - use block's last sequence
		if err := binary.Write(buf, binary.LittleEndian, info.Last); err != nil {
			return err
		}

		// Message count for this subject (8 bytes)
		if err := binary.Write(buf, binary.LittleEndian, info.Msgs); err != nil {
			return err
		}
	}

	return nil
}
