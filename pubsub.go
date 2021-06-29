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

type TopicManager interface {
	Get(interface{}) (Topic, bool)
	GetOrCreate(interface{}) (Topic, error)
	Delete(interface{}, Topic)
	Range(func(interface{}, Topic) bool)
	Len() int
	Clear()
}

type SmartTopic interface {
	Topic
	Get() Topic
}

// NewSynctopic : topic realized by sync.Map
func NewSyncTopic() Topic {
	return &syncTopic{}
}

// TopicMaker ...
type TopicMaker func(id interface{}) (Topic, error)
type SmartTopicMaker func(interface{}, TopicManager) (Topic, error)

// NewTopicManager ...
func NewTopicManager(maker SmartTopicMaker) TopicManager {
	if maker == nil {
		maker = NewSmartTopicMaker(nil, 0)
	}
	return newTopicManager(maker)
}

// NewMultiTopicWithManager :
// if maker == nil, will use the safeTopic
// if idleTime < 0, the topic will destroy the topic when which is empty
// if idleTime > 0, the topic will check and destroy the topic witch is empty

func NewMultiTopic(maker TopicMaker, delayTime time.Duration) MultiTopic {
	return NewMultiTopicWithManager(NewTopicManager(NewSmartTopicMaker(maker, delayTime)))
}

func NewMultiTopicWithManager(manager TopicManager) MultiTopic {
	if manager == nil {
		manager = NewTopicManager(nil)
	}
	return newMultiTopic(manager)
}

func SyncTopicMaker(id interface{}) (Topic, error) {
	return NewSyncTopic(), nil
}

func NewSmartTopic(id interface{}, manager TopicManager,
	maker TopicMaker, delayTime time.Duration) SmartTopic {
	if maker == nil {
		maker = SyncTopicMaker
	}
	return newSmartTopic(id, manager, maker, delayTime)
}

func NewSmartTopicMaker(maker TopicMaker,
	delayTime time.Duration) SmartTopicMaker {
	return func(id interface{}, manager TopicManager) (Topic, error) {
		return NewSmartTopic(id, manager, maker, delayTime), nil
	}
}

type syncTopic struct {
	subers sync.Map
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

type topicManager struct {
	sync.RWMutex
	maker  SmartTopicMaker
	topics sync.Map
}

func newTopicManager(maker SmartTopicMaker) *topicManager {
	return &topicManager{
		maker: maker,
	}
}

func (p *topicManager) Get(id interface{}) (topic Topic, ok bool) {
	v, ok := p.topics.Load(id)
	if !ok {
		return
	}
	topic = v.(Topic)
	return
}

func (p *topicManager) GetOrCreate(id interface{}) (topic Topic, err error) {
	var ok bool
	topic, ok = p.Get(id)
	if ok {
		return
	}
	p.Lock()
	defer p.Unlock()
	topic, ok = p.Get(id)
	if ok {
		return
	}

	topic, err = p.maker(id, p)
	if err != nil {
		return
	}
	p.topics.Store(id, topic)
	return
}

func (p *topicManager) Delete(id interface{}, topic Topic) {
	p.Lock()
	old, ok := p.Get(id)
	if ok && old.(Topic) == topic {
		p.topics.Delete(id)
	}
	p.Unlock()
}

func (p *topicManager) Range(f func(interface{}, Topic) bool) {
	p.topics.Range(func(k, v interface{}) bool {
		return f(k, v.(Topic))
	})
}

func (p *topicManager) Len() int {
	n := 0
	p.topics.Range(func(k, v interface{}) bool {
		n++
		return true
	})
	return n
}

func (p *topicManager) Clear() {
	p.Lock()
	p.topics = sync.Map{}
	p.Unlock()
}

type multiTopic struct {
	manager TopicManager
}

func newMultiTopic(manager TopicManager) *multiTopic {
	return &multiTopic{manager: manager}
}

func (p *multiTopic) Publish(id, value interface{}) error {
	topic, ok := p.manager.Get(id)
	if !ok {
		return nil
	}
	return topic.Publish(value)
}

func (p *multiTopic) Subscribe(id interface{}, suber Subscriber) error {
	topic, err := p.manager.GetOrCreate(id)
	if err != nil {
		return err
	}
	return topic.Subscribe(suber)
}

func (p *multiTopic) Unsubscribe(id interface{}, suber Subscriber) error {
	topic, ok := p.manager.Get(id)
	if !ok {
		return nil
	}
	return topic.Unsubscribe(suber)
}

func (p *multiTopic) Destroy(id interface{}, topic Topic) error {
	err := topic.Destroy()
	p.manager.Delete(id, topic)
	return err
}

func (p *multiTopic) Range(f func(interface{}, Topic) bool) {
	p.manager.Range(f)
}

func (p *multiTopic) DestroyAll() error {
	p.manager.Range(func(id interface{}, topic Topic) bool {
		_ = topic.Destroy()
		return true
	})
	p.manager.Clear()
	return nil
}

func (p *multiTopic) PublishAll(v interface{}) error {
	p.manager.Range(func(id interface{}, topic Topic) bool {
		_ = topic.Publish(v)
		return true
	})
	return nil
}

func (p *multiTopic) Len() int {
	return p.manager.Len()
}

type smartTopic struct {
	sync.RWMutex
	topic     Topic
	id        interface{}
	manager   TopicManager
	maker     TopicMaker
	delayTime time.Duration
	timer     *time.Timer
	destroyed bool
}

func newSmartTopic(
	id interface{},
	manager TopicManager,
	maker TopicMaker,
	delayTime time.Duration) *smartTopic {
	return &smartTopic{
		id:        id,
		manager:   manager,
		maker:     maker,
		delayTime: delayTime,
	}
}

func (p *smartTopic) destroy() {
	if p.destroyed {
		return
	}
	p.destroyed = true
	p.manager.Delete(p.id, p)
	if p.topic != nil {
		_ = p.topic.Destroy()
	}
	if p.timer != nil {
		p.timer.Stop()
	}
}

func (p *smartTopic) expire() {
	if p.delayTime < 0 {
		return
	}
	if p.delayTime == 0 {
		p.destroy()
		return
	}
	if p.timer != nil {
		p.timer.Reset(p.delayTime)
		return
	}
	p.timer = time.AfterFunc(p.delayTime, func() {
		p.Lock()
		defer p.Unlock()
		if !p.topic.IsEmpty() {
			return
		}
		p.destroy()
	})
}

func (p *smartTopic) stopExpire() {
	if p.delayTime <= 0 || p.timer == nil {
		return
	}
	p.timer.Stop()
}

func (p *smartTopic) Subscribe(suber Subscriber) (err error) {
	p.Lock()
	defer p.Unlock()
	if p.destroyed {
		tp, e := p.manager.GetOrCreate(p.id)
		if e != nil {
			err = e
			return
		}
		err = tp.Subscribe(suber)
		return
	}
	if p.topic == nil {
		p.topic, err = p.maker(p.id)
		if err != nil {
			p.destroy()
			return
		}
		err = p.topic.Subscribe(suber)
		if err != nil {
			p.destroy()
			return
		}
		return
	}
	err = p.topic.Subscribe(suber)
	p.stopExpire()
	return
}

func (p *smartTopic) Unsubscribe(suber Subscriber) (err error) {
	p.Lock()
	defer p.Unlock()
	if p.topic == nil || p.destroyed {
		return
	}
	err = p.topic.Unsubscribe(suber)
	if err != nil {
		return
	}
	if !p.topic.IsEmpty() {
		return
	}
	p.expire()
	return
}

func (p *smartTopic) Destroy() (err error) {
	p.Lock()
	defer p.Unlock()
	p.destroy()
	return
}

func (p *smartTopic) IsEmpty() bool {
	p.RLock()
	empty := p.topic == nil || p.topic.IsEmpty()
	p.RUnlock()
	return empty
}

func (p *smartTopic) Publish(v interface{}) (err error) {
	p.RLock()
	topic := p.topic
	destroyed := p.destroyed
	p.RUnlock()
	if topic != nil {
		err = topic.Publish(v)
	}
	if destroyed {
		tp, ok := p.manager.Get(p.id)
		if !ok {
			return
		}
		_ = tp.Publish(v)
	}
	return
}

func (p *smartTopic) Get() Topic {
	p.RLock()
	topic := p.topic
	p.RUnlock()
	return topic
}
