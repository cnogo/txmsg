package txmsg

import (
	"container/heap"
	"fmt"
	"github.com/pkg/errors"
	"sync"
)

const (
	closedMsg = "heap is closed"
)

type LessFunc func(interface{}, interface{}) bool

type KeyFunc func(interface{}) (string, error)

type heapItem struct {
	object interface{}
	index int
}

type itemKeyValue struct {
	key string
	obj interface{}
}

type heapData struct {
	items map[string]*heapItem
	queue []string
	keyFunc KeyFunc
	lessFunc LessFunc
}

var (
	_ = heap.Interface(&heapData{})
)

func (h *heapData) Less(i, j int) bool {
	if i > len(h.queue) || j > len(h.queue) {
		return false
	}

	itemi, ok := h.items[h.queue[i]]
	if !ok {
		return false
	}

	itemj, ok := h.items[h.queue[j]]
	if !ok {
		return false
	}

	return h.lessFunc(itemi.object, itemj.object)
}

func (h *heapData) Len() int {
	return len(h.queue)
}

func (h *heapData) Swap(i, j int) {
	h.queue[i], h.queue[j] = h.queue[j], h.queue[i]
	item := h.items[h.queue[i]]
	item.index = i
	item = h.items[h.queue[j]]
	item.index = j
}

func (h *heapData) Push(kv interface{}) {
	keyValue := kv.(*itemKeyValue)
	n := len(h.queue)
	h.items[keyValue.key] = &heapItem{keyValue.obj, n}
	h.queue = append(h.queue, keyValue.key)
}

func (h *heapData) Pop() interface{} {
	key := h.queue[len(h.queue) - 1]
	h.queue = h.queue[0: len(h.queue) - 1]
	item, ok := h.items[key]
	if !ok {
		// this is an error
		return nil
	}

	delete(h.items, key)

	return item.object
}


type Heap struct {
	lock sync.RWMutex
	cond sync.Cond

	data *heapData

	closed bool
}

func (h *Heap) Close() {
	h.lock.Lock()
	defer h.lock.Unlock()

	h.closed = true
	h.cond.Broadcast()
}

func (h *Heap) Add(obj interface{}) error {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return err
	}

	h.lock.Lock()
	defer h.lock.Unlock()
	if h.closed {
		return errors.New(closedMsg)
	}

	if _, exists := h.data.items[key]; exists {
		h.data.items[key].object = obj
		heap.Fix(h.data, h.data.items[key].index)
	} else {
		h.addIfNotPresentLocked(key, obj)
	}

	h.cond.Broadcast()
	return nil
}

func (h *Heap) addIfNotPresentLocked(key string, obj interface{}) {
	if _, exists := h.data.items[key]; exists {
		return
	}

	heap.Push(h.data, &itemKeyValue{key:key, obj: obj})
}

func (h *Heap) BulkAdd(list []interface{}) error {
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.closed {
		return errors.New(closedMsg)
	}

	for _, obj := range list {
		key, err := h.data.keyFunc(obj)
		if err != nil {
			return err
		}

		if _, exists := h.data.items[key]; exists {
			h.data.items[key].object = obj
			heap.Fix(h.data, h.data.items[key].index)
		} else {
			h.addIfNotPresentLocked(key, obj)
		}
	}

	h.cond.Broadcast()
	return nil
}

func (h *Heap) AddIfNotPresent(obj interface{}) error {
	id, err := h.data.keyFunc(obj)
	if err != nil {
		return err
	}
	h.lock.Lock()
	defer h.lock.Unlock()

	if h.closed {
		return errors.New(closedMsg)
	}

	h.addIfNotPresentLocked(id, obj)
	h.cond.Broadcast()
	return nil
}

func (h *Heap) Update(obj interface{}) error {
	return h.Add(obj)
}

func (h *Heap) Delete(obj interface{}) error  {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return err
	}

	h.lock.Lock()
	defer h.lock.Unlock()

	if item, exists := h.data.items[key]; !exists {
		heap.Remove(h.data, item.index)
		return nil
	}

	return errors.New("object not found")
}

func (h *Heap) Pop() (interface{}, error) {
	h.lock.Lock()
	defer h.lock.Unlock()

	if len(h.data.queue) == 0 {
		if h.closed {
			return nil, errors.New(closedMsg)
		}

		h.cond.Wait()
	}

	obj := heap.Pop(h.data)
	if obj == nil {
		return nil, fmt.Errorf("object was removed from heap data")
	}

	return obj, nil
}

func (h *Heap) List() []interface{} {
	h.lock.Lock()
	defer h.lock.RUnlock()

	list := make([]interface{}, 0, len(h.data.items))

	for _, item := range h.data.items {
		list = append(list, item.object)
	}

	return list
}

func (h *Heap) ListKeys() []string {
	h.lock.RLock()
	defer h.lock.RUnlock()

	list := make([]string, 0, len(h.data.items))
	for key := range h.data.items {
		list = append(list, key)
	}

	return list
}

func (h *Heap) Get(obj interface{}) (interface{}, bool, error) {
	key, err := h.data.keyFunc(obj)
	if err != nil {
		return nil, false, err
	}

	return h.GetByKey(key)
}

func (h *Heap) GetByKey(key string) (interface{}, bool, error) {
	h.lock.RLock()
	defer h.lock.RUnlock()
	item, exists := h.data.items[key]
	if !exists {
		return nil, false, nil
	}

	return item.object, true, nil
}

func (h *Heap) IsClosed() bool {
	h.lock.RLock()
	defer h.lock.RUnlock()
	if h.closed {
		return true
	}

	return false
}

func NewHeap(keyFn KeyFunc, lessFn LessFunc) *Heap {
	h := &Heap{
		data: &heapData{
			items: map[string]*heapItem{},
			queue: []string{},
			keyFunc: keyFn,
			lessFunc: lessFn,
		},
	}

	h.cond.L = &h.lock

	return h
}

















