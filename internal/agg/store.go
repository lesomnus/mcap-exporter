package agg

import (
	"encoding/json"
	"os"
)

// Store persists the per-topic last-emitted bucket so a restart does not
// re-emit already-exported buckets. The map is topic -> bucket start (unix
// nanos). A nil *Store is a no-op; callers guard on it.
type Store struct {
	path string
}

// LoadStore opens (or initializes) the checkpoint at path and returns it along
// with the previously persisted state. An empty path disables checkpointing and
// returns a nil store with an empty map.
func LoadStore(path string) (*Store, map[string]int64, error) {
	if path == "" {
		return nil, map[string]int64{}, nil
	}

	s := &Store{path: path}
	b, err := os.ReadFile(path)
	if os.IsNotExist(err) {
		return s, map[string]int64{}, nil
	}
	if err != nil {
		return nil, nil, err
	}

	last := map[string]int64{}
	if len(b) > 0 {
		if err := json.Unmarshal(b, &last); err != nil {
			return nil, nil, err
		}
	}
	return s, last, nil
}

// Save atomically writes the checkpoint (temp file + rename).
func (s *Store) Save(last map[string]int64) error {
	b, err := json.Marshal(last)
	if err != nil {
		return err
	}

	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.path); err != nil {
		return err
	}
	return nil
}
