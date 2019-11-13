package pubsub

import (
	"os"
	"sync"
)

// Subscriber ....
type Subscriber interface {
	OnPublish(interface{}) error
}

// Topic ...
type Topic interface {
	Publish(interface{}) error
	Subscribe(Subscriber) error
	Unsubscribe(Subscriber) error
	Destroy()
}

// MultiToplic ...
type MultiToplic interface {
	Publish(id, value interface{}) error
	Subscribe(id interface{}, suber Subscriber) error
	Unsubscribe(id interface{}, suber Subscriber) error
	Destroy()
}

// TopicPlug ...
type TopicPlug interface {
	Init(Subscriber) error
	Subscribe(Subscriber) error
	Unsubscribe(Subscriber) error
	Destroy() error
}

// NewTopic ...
func NewTopic(plug TopicPlug, suber Subscriber) (Topic, error) {
	return newTopic(plug, suber, nil, nil)
}

// NewMultiTopic ...
func NewMultiTopic(plug TopicPlug, frozen bool) MultiToplic {
	return newMultiTopic(plug, frozen)
}

type topic struct {
	sync.RWMutex
	plug   TopicPlug
	subers map[Subscriber]struct{}
	center *multiTopic
	id     interface{}
}

func newTopic(plug TopicPlug, suber Subscriber,
	center *multiTopic, id interface{}) (tp *topic, e error) {
	tp = &topic{
		plug:   plug,
		subers: make(map[Subscriber]struct{}),
	}
	if plug != nil {
		e = plug.Init(suber)
	}
	return tp, e
}

func (p *topic) Publish(v interface{}) error {
	p.RLock()
	for suber := range p.subers {
		suber.OnPublish(v)
	}
	p.RUnlock()
	return nil
}

func (p *topic) Subscribe(suber Subscriber) error {
	p.Lock()
	defer p.Unlock()
	_, ok := p.subers[suber]
	if ok {
		return os.ErrExist
	}
	if p.plug != nil {
		e := p.plug.Subscribe(suber)
		if e != nil {
			return e
		}
	}
	p.subers[suber] = struct{}{}
	return nil
}

func (p *topic) Unsubscribe(suber Subscriber) error {
	p.Lock()
	p.Unlock()
	_, ok := p.subers[suber]
	if !ok {
		return nil
	}
	if p.plug != nil {
		e := p.plug.Unsubscribe(suber)
		if e != nil {
			return e
		}
	}
	delete(p.subers, suber)
	return nil
}

func (p *topic) Destroy() {
	p.Lock()
	defer p.Unlock()
	if p.plug != nil {
		p.plug.Destroy()
	}
	if p.center != nil {
		p.center.removeTopic(p.id, p)
	}
	p.subers = make(map[Subscriber]struct{})
}

func (p *topic) destroyIfEmpty() error {
	p.Lock()
	defer p.Unlock()
	if len(p.subers) != 0 {
		return nil
	}
	if p.plug != nil {
		return p.plug.Destroy()
	}
	return nil
}

type multiTopic struct {
	sync.RWMutex
	plug   TopicPlug
	topics map[interface{}]*topic
	frozen bool
}

func newMultiTopic(plug TopicPlug, frozen bool) *multiTopic {
	return &multiTopic{
		plug:   plug,
		topics: make(map[interface{}]*topic),
		frozen: frozen,
	}
}

func (p *multiTopic) Publish(id, v interface{}) error {
	p.RLock()
	tp, ok := p.topics[id]
	p.RUnlock()
	if ok {
		return tp.Publish(v)
	}
	return nil
}

func (p *multiTopic) Subscribe(id interface{}, suber Subscriber) (e error) {
	p.Lock()
	defer p.Unlock()
	tp, ok := p.topics[id]
	if !ok {
		tp, e = newTopic(p.plug, suber, p, id)
		if e != nil {
			return
		}
		p.topics[id] = tp
	}
	e = tp.Subscribe(suber)
	return
}

func (p *multiTopic) Unsubscribe(id interface{}, suber Subscriber) (e error) {
	p.Lock()
	defer p.Unlock()
	tp, ok := p.topics[id]
	if !ok {
		return nil
	}
	e = tp.Unsubscribe(suber)
	if e != nil {
		return
	}
	if p.frozen {
		return nil
	}
	return tp.destroyIfEmpty()
}

func (p *multiTopic) Destroy() {
	p.Lock()
	p.Unlock()
	for _, topic := range p.topics {
		topic.Destroy()
	}
	p.topics = make(map[interface{}]*topic)
}

func (p *multiTopic) removeTopic(id interface{}, tp *topic) {
	p.Lock()
	defer p.Unlock()
	old, ok := p.topics[id]
	if !ok || old != tp {
		return
	}
	delete(p.topics, id)
}
