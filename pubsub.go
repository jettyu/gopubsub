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
	Destroy() error
	Len() int
}

// MultiTopic ...
type MultiTopic interface {
	Publish(id, value interface{}) error
	Subscribe(id interface{}, suber Subscriber) error
	Unsubscribe(id interface{}, suber Subscriber) error
	Destroy(id interface{}, topic Topic) error
	DestroyAll() error
	Range(f func(id interface{}, topic Topic) bool)
	Len() int
	SetContext(interface{}) MultiTopic
	GetContext() interface{}
}

// TopicMaker ...
type TopicMaker func(parent MultiTopic,
	id interface{}, first Subscriber) (child Topic, err error)

// NewDefaultTopic ...
func NewDefaultTopic() Topic {
	return newDefaultTopic()
}

// NewSafeTopic : add mutex for topic
// if topic == nil, will use the defautTopic
func NewSafeTopic(topic Topic) Topic {
	return newSafeTopic(topic)
}

// NewMultiTopic :
// if maker == nil, will use the defautTopic
// if frozen == false, the topic will destroy when it has been empty
func NewMultiTopic(maker TopicMaker, frozen bool) MultiTopic {
	return newMultiTopic(maker, frozen)
}

type defaultTopic struct {
	subers map[Subscriber]struct{}
}

func newDefaultTopic() *defaultTopic {
	return &defaultTopic{
		subers: make(map[Subscriber]struct{}),
	}
}

func (p *defaultTopic) Publish(v interface{}) error {
	for suber := range p.subers {
		suber.OnPublish(v)
	}
	return nil
}

func (p *defaultTopic) Subscribe(suber Subscriber) error {
	_, ok := p.subers[suber]
	if ok {
		return os.ErrExist
	}
	p.subers[suber] = struct{}{}
	return nil
}

func (p *defaultTopic) Unsubscribe(suber Subscriber) error {
	_, ok := p.subers[suber]
	if !ok {
		return os.ErrNotExist
	}
	delete(p.subers, suber)
	return nil
}

func (p *defaultTopic) Destroy() error {
	p.subers = make(map[Subscriber]struct{})
	return nil
}

func (p *defaultTopic) Len() int {
	return len(p.subers)
}

type safeTopic struct {
	sync.RWMutex
	self Topic
}

func newSafeTopic(self Topic) (tp *safeTopic) {
	tp = &safeTopic{
		self: self,
	}
	if self == nil {
		tp.self = NewDefaultTopic()
	}
	return
}

func (p *safeTopic) Publish(v interface{}) error {
	p.RLock()
	e := p.self.Publish(v)
	p.RUnlock()
	return e
}

func (p *safeTopic) Subscribe(suber Subscriber) error {
	p.Lock()
	defer p.Unlock()
	e := p.self.Subscribe(suber)
	return e
}

func (p *safeTopic) Unsubscribe(suber Subscriber) error {
	p.Lock()
	p.Unlock()
	e := p.self.Unsubscribe(suber)
	return e
}

func (p *safeTopic) Destroy() error {
	p.Lock()
	defer p.Unlock()
	e := p.self.Destroy()
	return e
}

func (p *safeTopic) Len() int {
	p.RLock()
	n := p.self.Len()
	p.RUnlock()
	return n
}

type multiTopic struct {
	sync.RWMutex
	maker   TopicMaker
	topics  map[interface{}]Topic
	frozen  bool
	context interface{}
}

var defaultTopicMaker = func(parent MultiTopic,
	id interface{}, first Subscriber) (tp Topic, err error) {
	tp = newDefaultTopic()
	return
}

func newMultiTopic(maker TopicMaker, frozen bool) *multiTopic {
	if maker == nil {
		maker = defaultTopicMaker
	}
	return &multiTopic{
		maker:  maker,
		topics: make(map[interface{}]Topic),
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
		tp, e = p.maker(p, id, suber)
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

	if tp.Len() == 0 {
		tp.Destroy()
	}
	delete(p.topics, id)
	return
}

func (p *multiTopic) Destroy(id interface{}, topic Topic) error {
	p.Lock()
	defer p.Unlock()
	tp, ok := p.topics[id]
	if !ok || tp != topic {
		return nil
	}
	delete(p.topics, id)
	return tp.Destroy()
}

func (p *multiTopic) DestroyAll() error {
	p.Lock()
	defer p.Unlock()
	for _, topic := range p.topics {
		topic.Destroy()
	}
	p.topics = make(map[interface{}]Topic)
	return nil
}

func (p *multiTopic) Range(f func(id interface{}, topic Topic) bool) {
	p.RLock()
	defer p.RUnlock()
	for id, topic := range p.topics {
		if !f(id, topic) {
			break
		}
	}
}

func (p *multiTopic) Len() int {
	p.RLock()
	n := len(p.topics)
	p.RUnlock()
	return n
}

func (p *multiTopic) SetContext(c interface{}) MultiTopic {
	p.Len()
	p.context = c
	p.Unlock()
	return p
}

func (p *multiTopic) GetContext() interface{} {
	p.Lock()
	c := p.context
	p.Unlock()
	return c
}
