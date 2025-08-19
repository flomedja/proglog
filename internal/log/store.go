package log

import (
  "bufio"
  "encoding/binary"
  "os"
  "sync"
)

var (
  enc = binary.BigEndian
)

const (
  lenWidth = 8
)

type store struct {
  *os.File
  mu   sync.Mutex
  buf  *bufio.Writer
  size uint64
}

// newStore creates and initializes a new store instance for log record storage.
// The store uses buffered I/O for efficient writes and tracks the file size for positioning.
// Parameters:
// - f: the file to use as the underlying storage for log records
// Returns a pointer to the initialized store and any error encountered during setup.
func newStore(f *os.File) (*store, error) {
  // Get file information to determine the current size of the file
  // This is needed to know where to append new records
  fi, err := os.Stat(f.Name())
  if err != nil {
    return nil, err
  }

  // Extract the current file size for tracking record positions
  size := uint64(fi.Size())

  // Create and return the store instance with:
  // - File: embedded *os.File for direct file operations
  // - buf: buffered writer for efficient sequential writes
  // - size: current file size for calculating record positions
  return &store{
    File: f,
    buf:  bufio.NewWriter(f),
    size: size,
  }, nil
}

// Append writes a new record to the end of the store file.
// The record is stored with a length prefix (8 bytes) followed by the data.
// This format allows for efficient reading by first reading the length, then the data.
// Parameters:
// - p: the byte slice containing the record data to append
// Returns:
// - n: the total number of bytes written (length prefix + data)
// - pos: the position in the file where this record starts
// - err: any error encountered during the write operation
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
  // Lock to ensure thread-safe writes (prevents concurrent modifications)
  s.mu.Lock()
  defer s.mu.Unlock()

  // Record the current position where this record will be written
  // This position will be returned for indexing purposes
  pos = s.size

  // Write the length of the record first (8-byte big-endian uint64)
  // This length prefix allows readers to know how much data to read
  if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
    return 0, 0, err
  }

  // Write the actual record data to the buffered writer
  w, err := s.buf.Write(p)
  if err != nil {
    return 0, 0, err
  }

  // Calculate total bytes written: data + length prefix
  w += lenWidth

  // Update the store's size to reflect the new record
  s.size += uint64(w)

  // Return total bytes written, starting position, and no error
  return uint64(w), pos, nil
}

// Read retrieves a record from the store at the specified position.
// It reads the length-prefixed record format: first 8 bytes contain the data length,
// followed by the actual record data. This matches the format used by Append.
// Parameters:
// - pos: the file position where the record starts (as returned by Append)
// Returns:
// - []byte: the record data (without the length prefix)
// - error: any error encountered during the read operation
func (s *store) Read(pos uint64) ([]byte, error) {
  // Lock to ensure thread-safe reads (prevents reads during writes)
  s.mu.Lock()
  defer s.mu.Unlock()

  // Flush any buffered writes to ensure we read the most recent data
  // This is crucial because the buffered writer might have pending data
  if err := s.buf.Flush(); err != nil {
    return nil, err
  }

  // Create a buffer to read the length prefix (8 bytes)
  size := make([]byte, lenWidth)

  // Read the length prefix from the file at the specified position
  if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
    return nil, err
  }

  // Decode the length from the 8-byte big-endian format
  length := enc.Uint64(size)

  // Create a buffer to hold the actual record data
  data := make([]byte, length)

  // Read the record data starting after the length prefix
  // Position is: pos + lenWidth (skip the 8-byte length prefix)
  if _, err := s.File.ReadAt(data, int64(pos+lenWidth)); err != nil {
    return nil, err
  }

  // Return the record data (without the length prefix)
  return data, nil
}

// ReadAt provides raw byte-level access to the store file at a specific offset.
// Unlike Read(), this function doesn't interpret the length-prefixed record format.
// It reads raw bytes directly from the file, which is useful for low-level operations
// or when the caller knows the exact structure they want to read.
// This method implements the io.ReaderAt interface.
// Parameters:
// - p: the buffer to read data into
// - off: the byte offset in the file to start reading from
// Returns:
// - int: the number of bytes actually read
// - error: any error encountered during the read operation
func (s *store) ReadAt(p []byte, off int64) (int, error) {
  // Lock to ensure thread-safe reads (prevents reads during writes)
  s.mu.Lock()
  defer s.mu.Unlock()

  // Flush any buffered writes to ensure we read the most recent data
  // This ensures consistency between buffered writes and direct reads
  if err := s.buf.Flush(); err != nil {
    return 0, err
  }

  // Delegate to the underlying file's ReadAt method for raw byte access
  // This bypasses any record format interpretation and reads raw bytes
  return s.File.ReadAt(p, off)
}

func (s *store) Close() error {
  s.mu.Lock()
  defer s.mu.Unlock()
  if err := s.buf.Flush(); err != nil {
    return err
  }
  return s.File.Close()
}
