package pubsub

import "sync"

// SimpleTopic ...
type SimpleTopic struct {
	sync.RWMutex
	elems   map[interface{}]Subscriber
	cleaner func()
	real    Topic
	closed  bool
}

var _ Topic = &SimpleTopic{}

// NewSimpleTopic ...
func NewSimpleTopic(cleaner func(), real ...Topic) *SimpleTopic {
	st := &SimpleTopic{
		elems:   make(map[interface{}]Subscriber),
		cleaner: cleaner,
	}
	if len(real) > 0 {
		st.real = real[0]
	}
	return st
}

// Refresh ...
func (p *SimpleTopic) Refresh(Topic) (bool, error) {
	return true, nil
}

// Subscribe ...
func (p *SimpleTopic) Subscribe(ob Subscriber) (err error) {
	p.Lock()
	if p.closed && p.real != nil && p.cleaner != nil {
		ok, _ := p.real.Refresh(p)
		if ok {
			return p.real.Subscribe(ob)
		}
		return ErrorTopicClosed
	}
	p.elems[ob.ID()] = ob
	p.Unlock()
	return nil
}

// Unsubscribe ...
func (p *SimpleTopic) Unsubscribe(sub Subscriber) error {
	p.Lock()
	defer p.Unlock()
	v, ok := p.elems[sub.ID()]
	if ok && v == sub {
		delete(p.elems, sub.ID())
	}
	if len(p.elems) == 0 && p.cleaner != nil {
		p.cleaner()
		p.closed = true
	}
	return nil
}

// Publish ...
func (p *SimpleTopic) Publish(v interface{}) error {
	p.RLock()
	for _, e := range p.elems {
		e.OnPublish(v)
	}
	p.RUnlock()
	return nil
}

// ConcreteTopic ...
type ConcreteTopic struct {
	sync.Mutex
	attachSubject *SimpleTopic
	notifySubject *SimpleTopic
	cleaner       func()
	closed        bool
	real          Topic
}

var _ Topic = &ConcreteTopic{}

// NewConcreteTopic ...
func NewConcreteTopic(cleaner func(), real ...Topic) *ConcreteTopic {
	ct := &ConcreteTopic{
		attachSubject: NewSimpleTopic(nil),
		notifySubject: NewSimpleTopic(nil),
		cleaner:       cleaner,
	}
	if len(real) > 0 {
		ct.real = real[0]
	}
	return ct
}

// Subscribe ...
func (p *ConcreteTopic) Subscribe(ob Subscriber) (err error) {
	p.Lock()
	if p.closed && p.real != nil && p.cleaner != nil {
		ok, _ := p.real.Refresh(p)
		if ok {
			return p.real.Subscribe(ob)
		}
		return ErrorTopicClosed
	}
	err = p.attachSubject.Subscribe(ob)
	p.Unlock()
	return err
}

// Unsubscribe ...
func (p *ConcreteTopic) Unsubscribe(subs Subscriber) error {
	p.Lock()
	defer p.Unlock()
	err := p.attachSubject.Unsubscribe(subs)
	if err != nil {
		return err
	}
	err = p.notifySubject.Unsubscribe(subs)
	if err != nil {
		return err
	}
	if len(p.attachSubject.elems) == 0 &&
		len(p.notifySubject.elems) == 0 &&
		p.cleaner != nil {
		p.cleaner()
		p.closed = true
	}
	return nil
}

// Refresh ...
func (p *ConcreteTopic) Refresh(Topic) (bool, error) {
	return true, nil
}

// Publish ...
func (p *ConcreteTopic) Publish(v interface{}) error {
	p.Lock()
	p1, p2 := p.notifySubject, p.attachSubject
	p.notifySubject, p.attachSubject = p.attachSubject, p.notifySubject
	p.Unlock()
	p1.Publish(v)
	p2.Publish(v)
	return nil
}
