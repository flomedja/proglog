package log

import (
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"sync"

	api "github.com/flomedja/proglog/api/v1"
)

type Log struct {
	mu sync.RWMutex

	Dir    string
	Config Config

	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	return l, l.setup()
}

// setup initializes the log by discovering and loading existing segments from the directory.
// This function is called during log creation to restore the log's state from persistent storage.
// It scans the log directory for segment files, determines their base offsets, and recreates
// the segment objects to restore the log's previous state.
// Returns any error encountered during directory scanning or segment initialization.
func (l *Log) setup() error {
	// Read all files in the log directory to discover existing segments
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}

	// Extract base offsets from filenames
	// Segment files are named like: "0.store", "0.index", "1000.store", "1000.index", etc.
	var baseOffsets []uint64
	for _, file := range files {
		// Remove the file extension to get the base offset string
		// e.g., "1000.store" -> "1000", "0.index" -> "0"
		offStr := strings.TrimSuffix(
			file.Name(),
			path.Ext(file.Name()),
		)

		// Parse the filename as a base offset number
		// Ignore parsing errors (non-numeric filenames are skipped)
		off, _ := strconv.ParseUint(offStr, 10, 0)
		baseOffsets = append(baseOffsets, off)
	}

	// Sort base offsets in ascending order to process segments chronologically
	// This ensures segments are loaded in the correct order
	slices.Sort(baseOffsets)

	// Create segment objects for each discovered base offset
	// Note: Each segment has two files (.store and .index), so we see each base offset twice
	for i := 0; i < len(baseOffsets); i++ {
		if err := l.newSegment(baseOffsets[i]); err != nil {
			return err
		}

		// Skip the duplicate entry since each base offset appears twice
		// (once for .store file, once for .index file)
		i++
	}

	// If no existing segments were found, create an initial segment
	// This happens when starting with a fresh/empty log directory
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		err = l.newSegment(off + 1)
	}
	return off, err
}

// Read retrieves a record from the log by its global offset.
// The function performs a two-phase lookup: first finding which segment contains
// the requested offset, then delegating to that segment's Read method.
// This enables efficient access to records across multiple segments.
// Parameters:
// - off: the global offset of the record to retrieve
// Returns:
// - *api.Record: the requested record, or nil if not found
// - error: any error encountered during the read operation or if offset is out of range
func (l *Log) Read(off uint64) (*api.Record, error) {
	// Use a read lock to allow concurrent reads while preventing writes
	// Multiple readers can access the log simultaneously for better performance
	l.mu.RLock()
	defer l.mu.RUnlock()

	// Find the segment that contains the requested offset
	// Each segment covers a range: [baseOffset, nextOffset)
	var s *segment
	for _, segment := range l.segments {
		// Check if the offset falls within this segment's range
		// baseOffset <= off < nextOffset means the record is in this segment
		if segment.baseOffset <= off && off < segment.nextOffset {
			s = segment
			break
		}
	}

	// Validate that we found a segment and the offset is within valid range
	if s == nil || s.nextOffset <= off {
		return nil, fmt.Errorf("offset out of range: %d", off)
	}

	// Delegate to the segment's Read method to retrieve the actual record
	// The segment will handle the index lookup and store retrieval
	return s.Read(off)
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	// Close all active segments
	for _, segment := range l.segments {
		if err := segment.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}

	// Remove the log directory
	return os.RemoveAll(l.Dir)
}

func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	off := l.segments[len(l.segments)-1].nextOffset
	if off == 0 {
		return 0, nil
	}
	return off - 1, nil
}

// Truncate removes all log segments whose highest offset is less than or equal to the specified lowest offset.
// This operation reclaims disk space by deleting old segments that no longer need to be retained.
// It updates the log's segment list to only include segments with records above the truncation point.
// Parameters:
// - lowest: the threshold offset - segments with all records at or below this offset will be removed
// Returns any error encountered during segment removal.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset <= lowest+1 {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

type originReader struct {
	*store
	off int64
}

// Reader returns an io.Reader that streams all records in the log sequentially across all segments.
// This is useful for creating snapshots, backups, or streaming the entire log to another system.
// The returned reader reads from the beginning of the log to the end, segment by segment, in order.
//
// Implementation details:
//   - Acquires a read lock to ensure thread-safe access to the segments slice while building the reader chain.
//   - Constructs a slice of io.Reader, one for each segment, using originReader to wrap each segment's store.
//     The originReader type implements io.Reader by reading from the underlying store starting at offset 0.
//   - Uses io.MultiReader to concatenate all segment readers into a single reader that reads through all segments in order.
//   - The resulting io.Reader can be used to read the entire log as a continuous byte stream, regardless of how many segments exist.
func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()

	readers := make([]io.Reader, len(l.segments))
	for i, segment := range l.segments {
		readers[i] = &originReader{segment.store, 0}
	}
	return io.MultiReader(readers...)
}

func (o *originReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.off)
	o.off += int64(n)
	return n, err
}

func (l *Log) newSegment(off uint64) error {
	s, err := newSegment(l.Dir, off, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
