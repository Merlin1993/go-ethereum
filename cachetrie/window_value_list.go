package cachetrie

import (
	"container/list"
	"errors"
	"fmt"
)

const DefaultWindows = 32

var (
	ErrBadWindow = errors.New("invalid window id")
	ErrExists    = errors.New("element already exists")
	ErrNotFound  = errors.New("element not found")
	ErrNoopMove  = errors.New("move to same window (no-op)")
)

// ---- 内部结构 ----

type entry struct {
	id  string
	val *ValueNode
}

type idxRec struct {
	win  int
	elem *list.Element // 指向 buckets[win] 中的结点（Value = *entry）
}

type Store struct {
	n       int
	buckets []list.List        // n 条双向链表
	index   map[string]*idxRec // id -> {win, elem}
}

// NewStore 创建一个窗口存储，n<=0 时使用默认 32。
func NewStore(n int) *Store {
	if n <= 0 {
		n = DefaultWindows
	}
	return &Store{
		n:       n,
		buckets: make([]list.List, n),
		index:   make(map[string]*idxRec),
	}
}

// Contains 判断是否存在 id
func (s *Store) Contains(id string) bool {
	_, ok := s.index[id]
	return ok
}

// Insert 在 windowID 中插入 (id, *ValueNode)，若已存在则返回 ErrExists。
// 复杂度：O(1)
func (s *Store) Insert(windowID int, id string, v *ValueNode) error {
	if windowID < 0 || windowID >= s.n {
		return fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	if _, ok := s.index[id]; ok {
		return ErrExists
	}
	en := &entry{id: id, val: v}
	elem := s.buckets[windowID].PushBack(en)
	s.index[id] = &idxRec{win: windowID, elem: elem}
	return nil
}

// Upsert 存在则更新 payload，不存在则插入到 windowID。
// 复杂度：O(1)
func (s *Store) Upsert(windowID int, id string, v *ValueNode) error {
	if windowID < 0 || windowID >= s.n {
		return fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	if rec, ok := s.index[id]; ok {
		rec.elem.Value.(*entry).val = v
		return nil
	}
	en := &entry{id: id, val: v}
	elem := s.buckets[windowID].PushBack(en)
	s.index[id] = &idxRec{win: windowID, elem: elem}
	return nil
}

func (s *Store) EraseWindow(windowID int) (int, error) {
	if windowID < 0 || windowID >= s.n {
		return 0, fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	lst := &s.buckets[windowID]
	deleted := 0

	// 先扫描整个链表，删除 index 中对应的键
	for e := lst.Front(); e != nil; {
		next := e.Next()
		id := e.Value.(*entry).id
		delete(s.index, id)
		deleted++
		e = next
	}

	// 最后把该窗口链表重置为零值（清空）
	*lst = list.List{}
	return deleted, nil
}

// Erase 删除 id，返回是否删到了。
// 复杂度：O(1)
func (s *Store) Erase(id string) bool {
	rec, ok := s.index[id]
	if !ok {
		return false
	}
	s.buckets[rec.win].Remove(rec.elem)
	delete(s.index, id)
	return true
}

// Move 将 id 从当前窗口移动到 toWindow。
// 复杂度：O(1)
func (s *Store) Move(id string, toWindow int) error {
	if toWindow < 0 || toWindow >= s.n {
		return fmt.Errorf("%w: %d", ErrBadWindow, toWindow)
	}
	rec, ok := s.index[id]
	if !ok {
		return ErrNotFound
	}
	if rec.win == toWindow {
		return ErrNoopMove
	}
	// 摘除旧节点（O(1)）
	en := rec.elem.Value.(*entry)
	s.buckets[rec.win].Remove(rec.elem)
	// 插入新窗口（O(1)）
	newElem := s.buckets[toWindow].PushBack(en)
	// 更新索引（O(1)）
	rec.win = toWindow
	rec.elem = newElem
	return nil
}

// Update 替换 id 的 payload（不改窗口）。
// 复杂度：O(1)
func (s *Store) Update(id string, v *ValueNode) error {
	rec, ok := s.index[id]
	if !ok {
		return ErrNotFound
	}
	rec.elem.Value.(*entry).val = v
	return nil
}

// Get 读取 id 的 payload。
// 复杂度：O(1)
func (s *Store) Get(id string) (*ValueNode, bool) {
	rec, ok := s.index[id]
	if !ok {
		return nil, false
	}
	return rec.elem.Value.(*entry).val, true
}

// GetAll 返回 windowID 内的全部 payload 切片（按链表顺序）。
// 取句柄是 O(1)，构建切片是 O(k)。
func (s *Store) GetAll(windowID int) ([]*ValueNode, error) {
	if windowID < 0 || windowID >= s.n {
		return nil, fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	lst := &s.buckets[windowID]
	res := make([]*ValueNode, 0, lst.Len())
	for e := lst.Front(); e != nil; e = e.Next() {
		res = append(res, e.Value.(*entry).val)
	}
	return res, nil
}

// ForEach 对 windowID 内所有元素进行遍历（零分配）。
// 复杂度：拿到迭代起点 O(1)，遍历 O(k)。
func (s *Store) ForEach(windowID int, fn func(id string, v *ValueNode)) error {
	if windowID < 0 || windowID >= s.n {
		return fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	for e := s.buckets[windowID].Front(); e != nil; e = e.Next() {
		en := e.Value.(*entry)
		fn(en.id, en.val)
	}
	return nil
}

// LenWindow 返回某个窗口的元素数量。O(1)
func (s *Store) LenWindow(windowID int) (int, error) {
	if windowID < 0 || windowID >= s.n {
		return 0, fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	return s.buckets[windowID].Len(), nil
}

// LenTotal 返回全量元素数。O(n + sum(k_i))，通常可用 index 长度直接估计。
func (s *Store) LenTotal() int {
	return len(s.index)
}

// DebugIDs 返回窗口里的 id 列表（调试用）。
func (s *Store) DebugIDs(windowID int) ([]string, error) {
	if windowID < 0 || windowID >= s.n {
		return nil, fmt.Errorf("%w: %d", ErrBadWindow, windowID)
	}
	out := make([]string, 0, s.buckets[windowID].Len())
	for e := s.buckets[windowID].Front(); e != nil; e = e.Next() {
		out = append(out, e.Value.(*entry).id)
	}
	return out, nil
}
