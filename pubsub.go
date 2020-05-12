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
	IsEmpty() bool
}

// MultiTopic ...
type MultiTopic interface {
	Publish(id, value interface{}) error
	Subscribe(id interface{}, suber Subscriber) error
	Unsubscribe(id interface{}, suber Subscriber) error
	Destroy(id interface{}, topic Topic) error
	DestroyAll() error
	PublishAll(value interface{}) error
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
// if maker == nil, will use the safeTopic
// if idleTime < 0, the topic will destroy the topic when which is empty
// if idleTime > 0, the topic will check and destroy the topic witch is empty
func NewMultiTopic(maker TopicMaker, idleTime time.Duration) MultiTopic {
	return newMultiTopic(maker, idleTime)
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
func (p *defaultTopic) Range(f func(Subscriber) bool) {
	for s := range p.subers {
		if !f(s) {
			break
		}
	}
}

func (p *defaultTopic) IsEmpty() bool {
	return len(p.subers) == 0
}

type syncTopic struct {
	subers sync.Map
}

func NewSyncTopic() Topic {
	return &syncTopic{}
}

func (p *syncTopic) Publish(v interface{}) (err error) {
	var e error
	p.subers.Range(func(key, value interface{}) bool {
		e = key.(Subscriber).OnPublish(v)
		if e != nil {
			err = e
		}
		return true
	})
	return nil
}

func (p *syncTopic) Subscribe(s Subscriber) (err error) {
	_, ok := p.subers.LoadOrStore(s, struct{}{})
	if ok {
		err = os.ErrExist
	}
	return
}

func (p *syncTopic) Unsubscribe(s Subscriber) (err error) {
	p.subers.Delete(s)
	return
}

func (p *syncTopic) Destroy() error {
	p.subers = sync.Map{}
	return nil
}

func (p *syncTopic) IsEmpty() bool {
	ok := true
	p.subers.Range(func(key, value interface{}) bool {
		ok = false
		return false
	})
	return ok
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

func (p *safeTopic) IsEmpty() bool {
	p.RLock()
	ok := p.self.IsEmpty()
	p.RUnlock()
	return ok
}

type topicWithTimer struct {
	Topic
	timer *time.Timer
}

type multiTopic struct {
	sync.RWMutex
	maker    TopicMaker
	topics   map[interface{}]*topicWithTimer
	idleTime time.Duration
}

var syncTopicMaker = func(id interface{}, first Subscriber) (tp Topic, err error) {
	tp = NewSyncTopic()
	return
}

func newMultiTopic(maker TopicMaker, idleTime time.Duration) *multiTopic {
	if maker == nil {
		maker = syncTopicMaker
	}
	return &multiTopic{
		maker:    maker,
		topics:   make(map[interface{}]*topicWithTimer),
		idleTime: idleTime,
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
	if p.idleTime < 0 {
		return nil
	}
	if !tp.IsEmpty() {
		return nil
	}
	// clear topic at now
	if p.idleTime == 0 {
		tp.Destroy()
		delete(p.topics, id)
	}
	// clear topic after idleTime
	tp.timer = time.AfterFunc(p.idleTime, func() {
		p.Lock()
		defer p.Unlock()
		tp, ok := p.topics[id]
		if !ok || !tp.IsEmpty() {
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

func (p *multiTopic) PublishAll(v interface{}) error {
	p.RLock()
	topics := make([]*topicWithTimer, 0, len(p.topics))
	for _, tp := range p.topics {
		topics = append(topics, tp)
	}
	p.RUnlock()
	for _, tp := range topics {
		tp.Publish(v)
	}
	return nil
}

func (p *multiTopic) Range(f func(id interface{}, topic Topic) bool) {
	tmp := make(map[interface{}]Topic)
	p.RLock()
	for id, topic := range p.topics {
		tmp[id] = topic.Topic
	}
	p.RUnlock()
	for id, topic := range tmp {
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
