package archive

import (
	"bytes"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

var inlineValueMarker = []byte{'B', 'I', 'V', '1'}

func encodeInlineValue(value []byte) []byte {
	ref := make([]byte, len(inlineValueMarker)+len(value))
	copy(ref, inlineValueMarker)
	copy(ref[len(inlineValueMarker):], value)
	return ref
}

func decodeInlineValue(ref []byte) ([]byte, bool) {
	if len(ref) == common.HashLength || !bytes.HasPrefix(ref, inlineValueMarker) {
		return nil, false
	}
	return common.CopyBytes(ref[len(inlineValueMarker):]), true
}

func valueDataKey(hash []byte) []byte {
	if len(hash) == 0 {
		return nil
	}
	key := make([]byte, len(hash)+1)
	copy(key, hash)
	key[len(hash)] = 0x02
	return key
}

var flatValuePrefix = []byte{'B', 'F', 'V', '1'}

func flatValueDataKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	dataKey := make([]byte, len(flatValuePrefix)+len(key))
	copy(dataKey, flatValuePrefix)
	copy(dataKey[len(flatValuePrefix):], key)
	return dataKey
}

func (s *Shard) stageValue(value []byte) []byte {
	return s.stageValueWithRef(crypto.Keccak256Hash(value).Bytes(), value)
}

func (s *Shard) stageValueForKey(key []byte, value []byte) []byte {
	s.stageFlatValueForKey(key, value)
	if s.config != nil && s.config.InlineValueThreshold > 0 &&
		len(value) <= s.config.InlineValueThreshold &&
		len(value)+len(inlineValueMarker) <= 255 &&
		len(value)+len(inlineValueMarker) != common.HashLength {
		return encodeInlineValue(value)
	}
	return valueRefForKeyValue(key, value)
}

func (s *Shard) stageFlatValueForKey(key []byte, value []byte) {
	if s.pendingFlatValues == nil {
		s.pendingFlatValues = make(map[string][]byte)
	}
	id := string(key)
	s.pendingFlatValues[id] = common.CopyBytes(value)
	delete(s.pendingFlatValueDeletes, id)
}

func (s *Shard) stageFlatDeleteForKey(key []byte) {
	id := string(key)
	delete(s.pendingFlatValues, id)
	if s.pendingFlatValueDeletes == nil {
		s.pendingFlatValueDeletes = make(map[string]struct{})
	}
	s.pendingFlatValueDeletes[id] = struct{}{}
}

func (s *Shard) stageValueWithRef(valueHash []byte, value []byte) []byte {
	if s.pendingValues == nil {
		s.pendingValues = make(map[string][]byte)
	}
	key := string(valueHash)
	if _, ok := s.pendingValues[key]; !ok {
		s.pendingValues[key] = common.CopyBytes(value)
	}
	delete(s.pendingValueDeletes, key)
	return valueHash
}

func (s *Shard) releaseValue(valueRef []byte) {
	if s.config == nil || !s.config.DeleteOldValues || len(valueRef) == 0 {
		return
	}
	if _, ok := decodeInlineValue(valueRef); ok {
		return
	}
	key := string(valueRef)
	if _, ok := s.pendingValues[key]; ok {
		delete(s.pendingValues, key)
		return
	}
}

func (s *Shard) getValue(valueHash []byte) ([]byte, error) {
	if len(valueHash) == 0 {
		return nil, ErrNodeNotFound
	}
	if value, ok := decodeInlineValue(valueHash); ok {
		return value, nil
	}
	key := string(valueHash)
	if val, ok := s.pendingValues[key]; ok {
		return val, nil
	}
	for i := len(s.stagedValues) - 1; i >= 0; i-- {
		if val, ok := s.stagedValues[i][key]; ok {
			return val, nil
		}
	}
	return s.getStoredValue(valueHash)
}

func (s *Shard) getFlatValue(key []byte) ([]byte, error) {
	if len(key) == 0 {
		return nil, ErrNodeNotFound
	}
	id := string(key)
	if value, ok := s.pendingFlatValues[id]; ok {
		return value, nil
	}
	if _, ok := s.pendingFlatValueDeletes[id]; ok {
		return nil, ErrNodeNotFound
	}
	for i := len(s.stagedFlatValues) - 1; i >= 0; i-- {
		if value, ok := s.stagedFlatValues[i][id]; ok {
			return value, nil
		}
	}
	if s.config != nil && s.config.FlatReader != nil {
		if value, err := s.config.FlatReader.GetFlatValue(key); err == nil && value != nil {
			return value, nil
		}
	}
	value, err := s.db.Get(flatValueDataKey(key))
	if err == nil && value != nil {
		return value, nil
	}
	return nil, ErrNodeNotFound
}

func (s *Shard) hasFlatValue(key []byte) bool {
	if len(key) == 0 {
		return false
	}
	id := string(key)
	if _, ok := s.pendingFlatValues[id]; ok {
		return true
	}
	if _, ok := s.pendingFlatValueDeletes[id]; ok {
		return false
	}
	for i := len(s.stagedFlatValues) - 1; i >= 0; i-- {
		if _, ok := s.stagedFlatValues[i][id]; ok {
			return true
		}
	}
	if s.config != nil && s.config.FlatReader != nil {
		if value, err := s.config.FlatReader.GetFlatValue(key); err == nil && value != nil {
			return true
		}
	}
	value, err := s.db.Get(flatValueDataKey(key))
	return err == nil && value != nil
}

func (s *Shard) commitPendingValues(batch Batcher) error {
	if len(s.pendingValues) == 0 && len(s.pendingValueDeletes) == 0 && len(s.pendingFlatValues) == 0 && len(s.pendingFlatValueDeletes) == 0 {
		return nil
	}
	flatValues := s.pendingFlatValues
	if err := s.commitFlatValueStore(batch, flatValues, s.pendingFlatValueDeletes); err != nil {
		return err
	}
	s.stagedFlatValues = append(s.stagedFlatValues, flatValues)
	if len(s.stagedFlatValues) > 2 {
		s.stagedFlatValues = append([]map[string][]byte(nil), s.stagedFlatValues[len(s.stagedFlatValues)-2:]...)
	}
	values := s.pendingValues
	if err := s.commitValueStore(batch, values, s.pendingValueDeletes); err != nil {
		return err
	}
	s.stagedValues = append(s.stagedValues, values)
	if len(s.stagedValues) > 2 {
		s.stagedValues = append([]map[string][]byte(nil), s.stagedValues[len(s.stagedValues)-2:]...)
	}
	s.pendingValues = make(map[string][]byte)
	s.pendingValueDeletes = make(map[string]struct{})
	s.pendingFlatValues = make(map[string][]byte)
	s.pendingFlatValueDeletes = make(map[string]struct{})
	return nil
}

func (s *Shard) commitFlatValueStore(batch Batcher, values map[string][]byte, deletes map[string]struct{}) error {
	if batch == nil {
		for key := range deletes {
			if err := s.db.Delete(flatValueDataKey([]byte(key))); err != nil {
				return err
			}
		}
		for key, value := range values {
			if err := s.db.Put(flatValueDataKey([]byte(key)), value); err != nil {
				return err
			}
		}
		return nil
	}
	for key := range deletes {
		if err := batch.Delete(flatValueDataKey([]byte(key))); err != nil {
			return err
		}
	}
	for key, value := range values {
		if err := batch.Put(flatValueDataKey([]byte(key)), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) getStoredValue(valueHash []byte) ([]byte, error) {
	dataKey := valueDataKey(valueHash)
	if s.config != nil && s.config.ArchiveDB != nil {
		if value, err := s.config.ArchiveDB.GetBucket(dataKey); err == nil && value != nil {
			return value, nil
		}
	}
	if value, err := s.db.Get(dataKey); err == nil && value != nil {
		return value, nil
	}
	if value, err := s.db.Get(valueHash); err == nil && value != nil {
		return value, nil
	}
	return nil, ErrNodeNotFound
}

func (s *Shard) commitValueStore(batch Batcher, values map[string][]byte, deletes map[string]struct{}) error {
	if s.config != nil && s.config.ArchiveDB != nil {
		if err := s.commitValuesToArchiveStore(values, deletes); err != nil {
			return err
		}
		if batch != nil {
			for h := range deletes {
				if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
					return err
				}
				if err := batch.Delete([]byte(h)); err != nil {
					return err
				}
			}
		}
		return nil
	}
	if batch == nil {
		for h := range deletes {
			if err := s.db.Delete(valueDataKey([]byte(h))); err != nil {
				return err
			}
			if err := s.db.Delete([]byte(h)); err != nil {
				return err
			}
		}
		for h, value := range values {
			if err := s.db.Put(valueDataKey([]byte(h)), value); err != nil {
				return err
			}
		}
		return nil
	}
	for h := range deletes {
		if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
			return err
		}
		if err := batch.Delete([]byte(h)); err != nil {
			return err
		}
	}
	for h, value := range values {
		if err := batch.Put(valueDataKey([]byte(h)), value); err != nil {
			return err
		}
	}
	return nil
}

func (s *Shard) commitValuesToArchiveStore(values map[string][]byte, deletes map[string]struct{}) error {
	if batchStore, ok := s.config.ArchiveDB.(interface{ NewBatch() Batcher }); ok {
		batch := batchStore.NewBatch()
		defer batch.Reset()
		for h := range deletes {
			if err := batch.Delete(valueDataKey([]byte(h))); err != nil {
				return err
			}
		}
		for h, value := range values {
			if err := batch.Put(valueDataKey([]byte(h)), value); err != nil {
				return err
			}
		}
		return batch.Write()
	}
	for h := range deletes {
		if err := s.config.ArchiveDB.DeleteBucket(valueDataKey([]byte(h))); err != nil {
			return err
		}
	}
	for h, value := range values {
		if err := s.config.ArchiveDB.PutBucket(valueDataKey([]byte(h)), value); err != nil {
			return err
		}
	}
	return nil
}
