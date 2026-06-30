package archive

import (
	"bytes"
	"errors"
	"fmt"
	"time"
)

type appendTask struct {
	oldHash  []byte
	newItems []ArchivedKV
}

type deleteTask struct {
	oldHash     []byte
	deleteItems []ArchivedKV
}

func archiveDataKey(hash []byte) []byte {
	if len(hash) == 0 {
		return nil
	}
	key := make([]byte, len(hash)+1)
	copy(key, hash)
	key[len(hash)] = 0x01
	return key
}

func (s *Shard) getBucketData(hash []byte) ([]byte, error) {
	if items, ok := s.pendingArchiveItems[string(hash)]; ok {
		return s.serializeArchivedKV(items)
	}
	if data, ok := s.pendingArchives[string(hash)]; ok {
		return data, nil
	}
	if task, ok := s.pendingAppends[string(hash)]; ok {
		if bytes.Equal(task.oldHash, hash) {
			return nil, errors.New("infinite recursion in getBucketData (append)")
		}
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return nil, err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return nil, err
		}
		items = append(items, task.newItems...)
		return s.serializeArchivedKV(items)
	}
	if task, ok := s.pendingDeletes[string(hash)]; ok {
		if bytes.Equal(task.oldHash, hash) {
			return nil, errors.New("infinite recursion in getBucketData (delete)")
		}
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return nil, err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return nil, err
		}
		newItems := make([]ArchivedKV, 0, len(items))
		for _, it := range items {
			found := false
			for _, del := range task.deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		return s.serializeArchivedKV(newItems)
	}
	data, err := s.getStoredBucketData(hash)
	if err == nil {
		s.statsMut.Lock()
		s.stats.ArchiveReadCount++
		s.statsMut.Unlock()
	}
	return data, err
}

func (s *Shard) getStoredBucketData(hash []byte) ([]byte, error) {
	if s.config.ArchiveDB != nil {
		return s.config.ArchiveDB.GetBucket(archiveDataKey(hash))
	}
	return s.db.Get(archiveDataKey(hash))
}

func (s *Shard) HasPendingArchiveWrites() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.pendingArchives) > 0 || len(s.pendingArchiveItems) > 0 || len(s.pendingAppends) > 0 || len(s.pendingDeletes) > 0 || len(s.pendingArchiveDeletes) > 0
}

func (s *Shard) markArchiveDataDelete(hash []byte, oldSize int) {
	if len(hash) == 0 {
		return
	}
	key := string(hash)
	if _, ok := s.pendingArchives[key]; ok {
		delete(s.pendingArchives, key)
		return
	}
	if _, ok := s.pendingArchiveItems[key]; ok {
		delete(s.pendingArchiveItems, key)
		return
	}
	if task, ok := s.pendingAppends[key]; ok {
		delete(s.pendingAppends, key)
		if !bytes.Equal(task.oldHash, hash) {
			s.markArchiveDataDelete(task.oldHash, -1)
		}
		return
	}
	if task, ok := s.pendingDeletes[key]; ok {
		delete(s.pendingDeletes, key)
		if !bytes.Equal(task.oldHash, hash) {
			s.markArchiveDataDelete(task.oldHash, -1)
		}
		return
	}
	if s.pendingArchiveDeletes == nil {
		s.pendingArchiveDeletes = make(map[string]int)
	}
	if prev, ok := s.pendingArchiveDeletes[key]; !ok || (prev < 0 && oldSize >= 0) {
		s.pendingArchiveDeletes[key] = oldSize
	}
}

func (s *Shard) FlushArchives() error {
	flushStart := time.Now()
	s.mu.Lock()
	defer s.mu.Unlock()
	pendingArchives := len(s.pendingArchives)
	pendingArchiveItems := len(s.pendingArchiveItems)
	pendingAppends := len(s.pendingAppends)
	pendingDeletes := len(s.pendingDeletes)
	pendingArchiveDeletes := len(s.pendingArchiveDeletes)
	defer func() {
		recordShardFlushDiagnostics(time.Since(flushStart).Nanoseconds(), pendingArchives+pendingArchiveItems, pendingAppends, pendingDeletes, pendingArchiveDeletes)
	}()
	for h, data := range s.pendingArchives {
		if err := s.putBucketDataWithOldSize([]byte(h), data, 0); err != nil {
			return err
		}
	}
	for h, items := range s.pendingArchiveItems {
		data, err := s.serializeArchivedKV(items)
		if err != nil {
			return err
		}
		if err := s.putBucketDataWithOldSize([]byte(h), data, 0); err != nil {
			return err
		}
	}
	for h, task := range s.pendingAppends {
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}
		items = append(items, task.newItems...)
		if limit := s.config.ResolveArchiveBucketSize(); limit > 0 && len(items) > limit {
			return fmt.Errorf("archive bucket append overflow: items=%d limit=%d", len(items), limit)
		}
		data, err := s.serializeArchivedKV(items)
		if err != nil {
			return err
		}
		if err := s.putBucketDataWithOldSize([]byte(h), data, 0); err != nil {
			return err
		}
		if err := s.deleteBucketDataWithOldSize(task.oldHash, len(oldData)); err != nil {
			return err
		}
	}
	for h, task := range s.pendingDeletes {
		hash := []byte(h)
		oldData, err := s.getBucketData(task.oldHash)
		if err != nil {
			return err
		}
		items, err := s.deserializeArchivedKV(oldData)
		if err != nil {
			return err
		}
		newItems := make([]ArchivedKV, 0, len(items))
		for _, it := range items {
			found := false
			for _, del := range task.deleteItems {
				if it.SuffixBits == del.SuffixBits && bytes.Equal(it.Suffix, del.Suffix) {
					found = true
					break
				}
			}
			if !found {
				newItems = append(newItems, it)
			}
		}
		if len(newItems) > 0 {
			data, err := s.serializeArchivedKV(newItems)
			if err != nil {
				return err
			}
			if err := s.putBucketDataWithOldSize(hash, data, 0); err != nil {
				return err
			}
		} else if err := s.deleteBucketDataWithOldSize(hash, 0); err != nil {
			return err
		}
		if err := s.deleteBucketDataWithOldSize(task.oldHash, len(oldData)); err != nil {
			return err
		}
	}
	for h, oldSize := range s.pendingArchiveDeletes {
		if err := s.deleteBucketDataWithOldSize([]byte(h), oldSize); err != nil {
			return err
		}
	}
	s.pendingArchives = make(map[string][]byte)
	s.pendingArchiveItems = make(map[string][]ArchivedKV)
	s.pendingAppends = make(map[string]appendTask)
	s.pendingDeletes = make(map[string]deleteTask)
	s.pendingArchiveDeletes = make(map[string]int)
	return nil
}

func (s *Shard) putBucketDataWithOldSize(hash []byte, data []byte, oldSize int) error {
	dataKey := archiveDataKey(hash)
	if oldSize < 0 {
		oldSize = 0
		if oldData, err := s.getStoredBucketData(hash); err == nil {
			oldSize = len(oldData)
		}
	}
	if s.config.ArchiveDB != nil {
		if err := s.config.ArchiveDB.PutBucket(dataKey, data); err != nil {
			return err
		}
	} else {
		if err := s.db.Put(dataKey, data); err != nil {
			return err
		}
	}
	s.statsMut.Lock()
	s.stats.ArchiveStorageSize += int64(len(data) - oldSize)
	s.statsMut.Unlock()
	return nil
}

func (s *Shard) deleteBucketDataWithOldSize(hash []byte, oldSize int) error {
	if len(hash) == 0 {
		return nil
	}
	dataKey := archiveDataKey(hash)
	if oldSize < 0 {
		oldSize = 0
		if oldData, err := s.getStoredBucketData(hash); err == nil {
			oldSize = len(oldData)
		}
	}
	if s.config.ArchiveDB != nil {
		if err := s.config.ArchiveDB.DeleteBucket(dataKey); err != nil {
			return err
		}
	} else {
		if err := s.db.Delete(dataKey); err != nil {
			return err
		}
	}
	if oldSize > 0 {
		s.statsMut.Lock()
		s.stats.ArchiveStorageSize -= int64(oldSize)
		s.statsMut.Unlock()
	}
	return nil
}
