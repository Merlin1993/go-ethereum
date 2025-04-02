package kvsrotage

import (
	"github.com/ethereum/go-ethereum/common"
)

// 实际上实现兼容类
type KVSnapshot struct {
	st *KVSlice
}

func NewKVSnapshot(st *KVSlice) *KVSnapshot {
	vm := &KVSnapshot{
		st: st,
	}
	return vm
}

func (vm *KVSnapshot) TryGet(key []byte) ([]byte, error) {
	value := vm.st.getState(key)
	return value, nil
}

func (vm *KVSnapshot) TryGetCommit(key []byte) ([]byte, error) {
	value := vm.st.getCommitedState(key)
	return value, nil
}

func (vm *KVSnapshot) TryUpdate(key, value []byte) error {
	vm.st.WriteState(key, value)
	return nil
}

func (vm *KVSnapshot) TryDelete(key []byte) error {
	return vm.TryUpdate(key, nil)
}

func (vm *KVSnapshot) LedgerHash() common.Hash {
	return vm.st.Hash()
}

func (vm *KVSnapshot) Copy() *KVSnapshot {
	return &KVSnapshot{
		st: vm.st.Copy(),
	}
}

func (vm *KVSnapshot) KnotBlock(rootHash common.Hash) {
	vm.st.knotBlock(rootHash)
}
