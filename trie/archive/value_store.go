package archive

import "github.com/ethereum/go-ethereum/common"

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

// stageValueForKey 把真实 value 写入 flat store 的 pending 区，并返回树中保存的 key-bound valueRef。
func (s *Shard) stageValueForKey(key []byte, value []byte) []byte {
	s.stageFlatValueForKey(key, value)
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

// getFlatValue 是真实 value 的唯一读取路径。archive metadata 只证明 membership，
// 执行读取仍从 pending、staged、外部 snapshot reader 或本地 flat-value 表取值。
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

// commitPendingValues 刷新 pending flat value，并只保留很小的 staged 窗口。
// 这样 commit 后、外层数据库视图完全追上前，读路径仍能看到刚提交的数据。
func (s *Shard) commitPendingValues(batch Batcher) error {
	if len(s.pendingFlatValues) == 0 && len(s.pendingFlatValueDeletes) == 0 {
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
