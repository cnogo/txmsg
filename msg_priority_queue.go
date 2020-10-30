package txmsg

import (
	"container/heap"
	"sync"
)

type msgPriorityQueue struct {
	Queue []*Msg
	CompareFunc func(*Msg, *Msg) bool
}

func (p *msgPriorityQueue) Len() int {
	return len(p.Queue)
}

func (p *msgPriorityQueue) Less(i, j int) bool {
	return p.CompareFunc(p.Queue[i], p.Queue[j])
}

func (p *msgPriorityQueue) Swap(i, j int) {
	p.Queue[i], p.Queue[j] = p.Queue[j], p.Queue[i]
}


func (p *msgPriorityQueue) Push(msg interface{}) {
	p.Queue = append(p.Queue, msg.(*Msg))
}

func (p *msgPriorityQueue) Pop() interface{} {
	n := len(p.Queue)
	if n == 0 {
		return nil
	}

	msg := p.Queue[n-1]
	p.Queue = p.Queue[0:n-1]
	return msg
}

type MsgPriorityQueue struct {
	msgQueue *msgPriorityQueue
	mutex sync.Mutex
	cond sync.Cond
}

func NewMsgPriorityQueue(compareFunc func(*Msg, *Msg) bool) *MsgPriorityQueue {
	msgQueue := &MsgPriorityQueue{
		msgQueue: &msgPriorityQueue{
			CompareFunc: compareFunc,
		},
	}

	msgQueue.cond.L = &msgQueue.mutex

	return msgQueue
}

func (p *MsgPriorityQueue) Push(msg *Msg) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	heap.Push(p.msgQueue, msg)
	p.cond.Broadcast()
}

func (p *MsgPriorityQueue) Pop() *Msg {
	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.msgQueue.Len() == 0 {
		p.cond.Wait()
	}

	msg := heap.Pop(p.msgQueue)
	if msg == nil {
		return nil
	}

	return msg.(*Msg)
}

