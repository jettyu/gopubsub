package pubsub

import (
	"sync"
	"sync/atomic"
)

// RealTopic ...
type RealTopic struct {
	*realTopic
}

var _ Topic = (*RealTopic)(nil)

// RealTopicMaker ...
type RealTopicMaker func(id interface{}, cleaner func(), tp *RealTopic) (Topic, error)

// NewSimpleTopicFunc ...
func NewSimpleTopicFunc(cleaner func()) RealTopicMaker {
	return func(id interface{}, cl func(), tp *RealTopic) (Topic, error) {
		ncl := func() {
			if cl != nil {
				cl()
			}
			if cleaner != nil {
				cleaner()
			}
		}
		return NewSimpleTopic(ncl, tp), nil
	}
}

// NewConcreteTopicFunc ...
func NewConcreteTopicFunc(cleaner func()) RealTopicMaker {
	return func(id interface{}, cl func(), tp *RealTopic) (Topic, error) {
		ncl := func() {
			if cl != nil {
				cl()
			}
			if cleaner != nil {
				cleaner()
			}
		}
		return NewConcreteTopic(ncl, tp), nil
	}
}

// NewRealTopic ...
// default use SimpleTopic
func NewRealTopic(maker RealTopicMaker) TopicMaker {
	return func(id interface{}, center TopicCenter) (Topic, error) {
		tp := &RealTopic{
			&realTopic{
				center: center,
				id:     id,
			},
		}
		var cl func()
		if center != nil {
			cl = func() {
				r := tp.realTopic
				atomic.StoreInt32(&r.closed, 1)
				center.Remove(id, tp)
			}
		}
		if maker != nil {
			topic, err := maker(id, cl, tp)
			if err != nil {
				return nil, err
			}
			tp.topic = topic
		} else {
			tp.topic = NewSimpleTopic(cl)
		}

		return tp, nil
	}
}

// Subscribe ...
func (p *RealTopic) Subscribe(s Subscriber) error {
	if p.center != nil && atomic.LoadInt32(&p.closed) == 1 {
		topic, err := p.center.GetAndCreate(p.id)
		if err != nil {
			return err
		}
		p.realTopic = topic.(*RealTopic).realTopic
	}
	return p.realTopic.Subscribe(s)
}

// Refresh ...
func (p *RealTopic) Refresh(tp Topic) (bool, error) {
	if p.center != nil && atomic.LoadInt32(&p.closed) == 1 {
		topic, err := p.center.GetAndCreate(p.id)
		if err != nil {
			return false, err
		}
		p.realTopic = topic.(*RealTopic).realTopic
	}
	if p.realTopic.topic == tp {
		return false, nil
	}
	return true, nil
}

// RealTopicCenter ...
type RealTopicCenter struct {
	topics map[interface{}]Topic
	rwl    sync.RWMutex
	maker  TopicMaker
}

var _ TopicCenter = (*RealTopicCenter)(nil)

// NewRealTopicCenter ...
func NewRealTopicCenter(maker TopicMaker) *RealTopicCenter {
	return &RealTopicCenter{
		topics: make(map[interface{}]Topic),
		maker:  maker,
	}
}

// TopicMaker ...
type TopicMaker func(id interface{},
	center TopicCenter) (Topic, error)

// Get ...
func (p *RealTopicCenter) Get(id interface{}) (Topic, bool) {
	p.rwl.RLock()
	tp, ok := p.topics[id]
	p.rwl.RUnlock()
	return tp, ok
}

// GetAndCreate ...
func (p *RealTopicCenter) GetAndCreate(id interface{}) (topic Topic, err error) {
	p.rwl.RLock()
	tp, ok := p.topics[id]
	p.rwl.RUnlock()
	if ok {
		topic = tp
		return
	}
	if p.maker != nil {
		p.rwl.Lock()
		defer p.rwl.Unlock()
		tp, ok := p.topics[id]
		if ok {
			topic = tp
			return
		}
		topic, err = p.maker(id, p)
		if err != nil {
			return
		}
		p.topics[id] = topic
	}
	return
}

// Remove ...
func (p *RealTopicCenter) Remove(id interface{}, topic Topic) {
	p.rwl.Lock()
	tp, ok := p.topics[id]
	if ok && tp == topic {
		delete(p.topics, id)
	}
	p.rwl.Unlock()
}

// Topic ...
type realTopic struct {
	center TopicCenter
	id     interface{}
	topic  Topic
	closed int32
}

// Subscribe ...
func (p *realTopic) Subscribe(s Subscriber) error {
	return p.topic.Subscribe(s)
}

// Unsubscribe ...
func (p *realTopic) Unsubscribe(s Subscriber) error {
	return p.topic.Unsubscribe(s)
}

// Publish ...
func (p *realTopic) Publish(v interface{}) error {
	return p.topic.Publish(v)
}
