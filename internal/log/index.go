package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offWidth uint64 = 4
	posWidth uint64 = 8
	entWidth        = offWidth + posWidth
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// newIndex creates and initializes a new index instance from the given file and configuration.
// It sets up memory mapping for efficient index operations and prepares the file for use.
// Parameters:
// - f: the file to use for the index
// - c: the configuration containing segment settings (e.g., MaxIndexBytes)
// Returns a pointer to the initialized index and any error encountered during setup.
func newIndex(f *os.File, c Config) (*index, error) {
	// Create a new index instance with the provided file
	idx := &index{
		file: f,
	}

	// Get file information to determine current size
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	// Store the current file size
	idx.size = uint64(fi.Size())

	// Truncate (or extend) the file to the maximum index size defined in config
	// This pre-allocates space for the memory-mapped operations
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}

	// Create a memory-mapped region of the file for efficient read/write operations.
	// Memory mapping (mmap) maps the file content directly into the process's virtual memory space,
	// allowing the file to be accessed as if it were a regular byte slice in memory.
	// This provides several benefits:
	// 1. Eliminates the need for explicit read/write system calls for each operation
	// 2. The OS handles caching and buffering automatically
	// 3. Multiple processes can share the same mapped region efficiently
	// 4. Changes are automatically synchronized to disk by the OS
	//
	// Parameters explained:
	// - idx.file.Fd(): File descriptor of the index file to be mapped
	// - PROT_READ|PROT_WRITE: Memory protection flags allowing both read and write access
	// - MAP_SHARED: Creates a shared mapping where changes are visible to other processes
	//   and are eventually written back to the underlying file. This ensures persistence
	//   and allows multiple processes to work with the same index file consistently.
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Read returns the associated record's position in the store for the given offset.
// If in is -1, it returns the last entry in the index.
// The function returns:
// - out: the offset stored at the given index position
// - pos: the position of the record in the store file
// - err: io.EOF if the index is empty or the requested entry doesn't exist
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	// Return EOF if the index is empty
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// If in is -1, get the last entry in the index
	if in == -1 {
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in)
	}

	// Calculate the position in the memory-mapped file
	pos = uint64(out) * entWidth

	// Check if the requested entry exists within the index bounds
	if i.size < pos+entWidth {
		return 0, 0, io.EOF
	}

	// Read the offset (4 bytes) from the memory-mapped file
	out = enc.Uint32(i.mmap[pos : pos+entWidth])

	// Read the position (8 bytes) from the memory-mapped file
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])

	return out, pos, nil
}

// Write appends a new index entry to the memory-mapped file.
// It stores the given offset and position as a new entry in the index.
// Parameters:
// - off: the offset (record number) to store (4 bytes)
// - pos: the position of the record in the store file (8 bytes)
// Returns io.EOF if there's not enough space in the memory-mapped file for a new entry.
func (i *index) Write(off uint32, pos uint64) error {
	// Check if there's enough space in the memory-mapped file for a new entry
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}

	// Write the offset (4 bytes) to the current position in the index
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)

	// Write the position (8 bytes) after the offset in the index entry
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)

	// Update the size to reflect the new entry
	i.size += entWidth

	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_ASYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
