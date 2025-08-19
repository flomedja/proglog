package log

import (
  "os"
  "path"
  "slices"
  "strconv"
  "strings"
  "sync"
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

func (l *Log) newSegment(off uint64) error {
  panic("unimplemented")
}
