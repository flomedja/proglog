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

// Read retrieves a record from the segment by its global offset.
// The function performs a two-step lookup: first using the index to find the record's
// position in the store, then reading the actual data from the store and deserializing it.
// Parameters:
// - off: the global offset of the record to retrieve
// Returns:
// - *api.Record: the deserialized record, or nil if not found
// - error: any error encountered during the read operation
func (s *segment) Read(off uint64) (*api.Record, error) {
	// Convert global offset to relative offset for index lookup
	// The index stores relative offsets (offset - baseOffset) for efficiency
	// Example: if baseOffset=1000 and off=1005, we look up relative offset 5
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	// Use the position returned by the index to read the raw data from the store
	// The index told us "record is at position X in the store file"
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// Create a new Record instance to deserialize into
	record := &api.Record{}

	// Deserialize the raw bytes back into a structured Record using protocol buffers
	// This reverses the marshaling done in the Append function
	err = proto.Unmarshal(p, record)

	// Return the reconstructed record and any unmarshaling error
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove completely deletes the segment and its associated files from disk.
// This function performs cleanup operations to properly close the segment before deletion,
// ensuring that any buffered data is flushed and file handles are released.
// Both the index file (.index) and store file (.store) are permanently deleted.
// This operation is irreversible and should be used with caution.
// Returns any error encountered during the close or file deletion operations.
func (s *segment) Remove() error {
	// First, properly close the segment to ensure all data is flushed to disk
	// and file handles are released before attempting deletion
	if err := s.Close(); err != nil {
		return err
	}

	// Delete the index file from the filesystem
	// This removes the offset-to-position mapping file (e.g., "1000.index")
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}

	// Delete the store file from the filesystem
	// This removes the actual record data file (e.g., "1000.store")
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}

	// All files successfully deleted
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the largest multiple of k that is less than or equal to j.
// This is commonly used for alignment purposes, such as creating segments at
// predictable offset boundaries (e.g., offsets 0, 1000, 2000, etc.).
//
// Examples:
//
//	nearestMultiple(1537, 1000) = 1000
//	nearestMultiple(2000, 1000) = 2000
//	nearestMultiple(999, 1000) = 0
//
// Parameters:
//
//	j: the value to find the nearest multiple for
//	k: the multiple base (must be > 0)
//
// Returns the largest multiple of k ≤ j, or 0 if k is 0.
func nearestMultiple(j, k uint64) uint64 {
	// Handle edge case: avoid division by zero
	if k == 0 {
		return 0
	}

	// For unsigned integers, this is simply integer division * k
	// Integer division automatically floors the result
	return (j / k) * k
}
