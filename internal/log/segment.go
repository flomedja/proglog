package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/flomedja/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment creates and initializes a new log segment with associated store and index files.
// A segment consists of two files: a store file containing the actual log records, and an
// index file containing offset-to-position mappings for efficient record lookup.
// Parameters:
// - dir: the directory where the segment files will be created
// - baseOffset: the starting offset number for this segment (used in filenames)
// - c: configuration containing segment settings (e.g., MaxIndexBytes, MaxStoreBytes)
// Returns a pointer to the initialized segment and any error encountered during setup.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	// Create a new segment instance with the base offset
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error

	// Create or open the store file for this segment
	// Filename format: <baseOffset>.store (e.g., "0.store", "1000.store")
	// Flags: O_RDWR (read/write), O_CREATE (create if not exists), O_APPEND (append mode)
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// Initialize the store with the opened file
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// Create or open the index file for this segment
	// Filename format: <baseOffset>.index (e.g., "0.index", "1000.index")
	// Flags: O_RDWR (read/write), O_CREATE (create if not exists)
	// Note: No O_APPEND flag since index uses memory mapping for random access
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}

	// Initialize the index with the opened file and configuration
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Determine the next offset to use for new records
	// Try to read the last entry from the index to find where to continue
	if off, _, err := s.index.Read(-1); err != nil {
		// If index is empty or error reading, start from base offset
		s.nextOffset = baseOffset
	} else {
		// If index has entries, next offset is base + last relative offset + 1
		s.nextOffset = baseOffset + uint64(off) + 1
	}

	return s, nil
}

// Append adds a new record to the segment, storing it in both the store and index.
// The function coordinates between the store (which holds the actual data) and the index
// (which maps offsets to positions for efficient lookups).
// Parameters:
// - record: the protobuf record to append to the segment
// Returns:
// - offset: the global offset assigned to this record
// - err: any error encountered during the append operation
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	// Capture the current next offset for this record
	// This will be the global offset assigned to this record
	cur := s.nextOffset

	// Set the record's offset field to the assigned offset
	// This ensures the record knows its own position in the log
	record.Offset = cur

	// Serialize the record to bytes using protocol buffers
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// Append the serialized record to the store file
	// This returns the position where the record was written
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// Add an entry to the index mapping the relative offset to the store position
	// Key insight: index stores relative offsets (offset - baseOffset) for efficiency
	// This allows the index to use smaller numbers and be more compact
	if err = s.index.Write(
		// Convert global offset to relative offset within this segment
		uint32(s.nextOffset-s.baseOffset),
		pos, // Position in the store file where the record is located
	); err != nil {
		return 0, err
	}

	// Increment the next offset for future records
	s.nextOffset++

	// Return the global offset that was assigned to this record
	return cur, nil
}
