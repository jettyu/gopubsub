package pubsub

import (
	"os"
	"sync"
	"time"
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
}

// TopicMaker ...
type TopicMaker func(id interface{}, first Subscriber) (topic Topic, err error)

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
// if idelTime < 0, the topic will destroy the topic when which is empty
// if idelTime > 0, the topic will check and destroy the topic witch is empty
func NewMultiTopic(maker TopicMaker, idelTime time.Duration) MultiTopic {
	return newMultiTopic(maker, idelTime)
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

type topicWithTimer struct {
	Topic
	timer *time.Timer
}

type multiTopic struct {
	sync.RWMutex
	maker    TopicMaker
	topics   map[interface{}]*topicWithTimer
	idelTime time.Duration
}

var defaultTopicMaker = func(id interface{}, first Subscriber) (tp Topic, err error) {
	tp = newDefaultTopic()
	return
}

func newMultiTopic(maker TopicMaker, idelTime time.Duration) *multiTopic {
	if maker == nil {
		maker = defaultTopicMaker
	}
	return &multiTopic{
		maker:    maker,
		topics:   make(map[interface{}]*topicWithTimer),
		idelTime: idelTime,
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

func (p *multiTopic) Subscribe(id interface{}, suber Subscriber) (err error) {
	p.Lock()
	defer p.Unlock()
	tp, ok := p.topics[id]
	if !ok {
		itp, e := p.maker(id, suber)
		if e != nil {
			err = e
			return
		}
		tp = &topicWithTimer{
			Topic: itp,
		}
		p.topics[id] = tp
	} else {
		if tp.timer != nil {
			tp.timer.Stop()
			tp.timer = nil
		}
	}
	err = tp.Subscribe(suber)
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
	// do not clear topic
	if p.idelTime < 0 {
		return nil
	}
	if tp.Len() != 0 {
		return nil
	}
	// clear topic at now
	if p.idelTime == 0 {
		tp.Destroy()
		delete(p.topics, id)
	}
	// clear topic after idelTime
	tp.timer = time.AfterFunc(p.idelTime, func() {
		p.Lock()
		defer p.Unlock()
		tp, ok := p.topics[id]
		if !ok || tp.Len() != 0 {
			return
		}
		tp.Destroy()
		delete(p.topics, id)
	})

	return
}

func (p *multiTopic) Destroy(id interface{}, topic Topic) error {
	p.Lock()
	defer p.Unlock()
	e := topic.Destroy()
	if e != nil {
		return e
	}
	tp, ok := p.topics[id]
	if !ok || tp.Topic != topic {
		return nil
	}
	if tp.timer != nil {
		tp.timer.Stop()
	}
	delete(p.topics, id)
	return nil
}

func (p *multiTopic) DestroyAll() error {
	p.Lock()
	defer p.Unlock()
	for _, topic := range p.topics {
		topic.Destroy()
		if topic.timer != nil {
			topic.timer.Stop()
		}
	}
	p.topics = make(map[interface{}]*topicWithTimer)
	return nil
}

func (p *multiTopic) Range(f func(id interface{}, topic Topic) bool) {
	p.RLock()
	defer p.RUnlock()
	for id, topic := range p.topics {
		if !f(id, topic.Topic) {
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
