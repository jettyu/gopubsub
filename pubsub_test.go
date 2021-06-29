package pubsub

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testSubscriber struct {
	v int
}

func (p *testSubscriber) OnPublish(v interface{}) error {
	p.v = v.(int)
	return nil
}

func testTopicFun(t *testing.T, topic Topic) {
	var (
		ob1 testSubscriber
		ob2 testSubscriber
	)
	_ = topic.Subscribe(&ob1)
	_ = topic.Subscribe(&ob2)
	_ = topic.Publish(1)
	if !assert.Equal(t, 1, ob1.v) {
		return
	}
	if !assert.Equal(t, 1, ob2.v) {
		return
	}

	_ = topic.Unsubscribe(&ob2)
	_ = topic.Publish(2)
	if !assert.Equal(t, 2, ob1.v) {
		return
	}
	if !assert.Equal(t, 1, ob2.v) {
		return
	}
	_ = topic.Unsubscribe(&ob1)
	if !assert.Equal(t, true, topic.IsEmpty()) {
		return
	}
}

func TestTopic(t *testing.T) {
	testTopicFun(t, NewSyncTopic())
}

type testTopic struct {
	Topic
}

func newTestTopic(id interface{}) (Topic, error) {
	if id.(string) == "createFailed" {
		return nil, errors.New(id.(string))
	}
	return &testTopic{
		Topic: NewSyncTopic(),
	}, nil
}

func (p *testTopic) Subscribe(suber Subscriber) error {
	if suber.(*testSubscriber).v == -1 {
		return errors.New("failed")
	}
	return p.Topic.Subscribe(suber)
}

func TestMultiTopic(t *testing.T) {
	mt := NewMultiTopic(newTestTopic, 0)
	defer func() { _ = mt.DestroyAll() }()
	subers := map[string][]*testSubscriber{
		"a": {
			{
				1,
			},
			{
				1,
			},
		},
		"b": {
			{
				1,
			},
			{
				1,
			},
		},
	}
	for id, v := range subers {
		for _, suber := range v {
			_ = mt.Subscribe(id, suber)
		}
	}
	_ = mt.Publish("a", 100)
	_ = mt.Publish("b", 200)
	for _, v := range subers["a"] {
		if !assert.Equal(t, 100, v.v) {
			return
		}
	}
	for _, v := range subers["b"] {
		if !assert.Equal(t, 200, v.v) {
			return
		}
	}
	_ = mt.PublishAll(300)
	for _, v := range subers["a"] {
		if !assert.Equal(t, 300, v.v) {
			return
		}
	}
	for _, v := range subers["b"] {
		if !assert.Equal(t, 300, v.v) {
			return
		}
	}
	if !assert.Equal(t, 2, mt.Len()) {
		return
	}

	for _, v := range subers["a"] {
		_ = mt.Unsubscribe("a", v)
	}
	if !assert.Equal(t, 1, mt.Len()) {
		return
	}
	mt.Range(func(id interface{}, topic Topic) bool {
		if id.(string) == "a" {
			t.Error(topic)
			return false
		}
		return true
	})
	mt.Range(func(id interface{}, topic Topic) bool {
		if id.(string) != "b" {
			return true
		}
		_, ok := topic.(SmartTopic).Get().(*testTopic)
		if !ok {
			t.Error(topic)
			return false
		}
		_ = mt.Destroy("b", topic)
		return false
	})
	if !assert.Equal(t, 0, mt.Len(), mt) {
		return
	}
	// test create failed
	createFailedSuber := &testSubscriber{}
	err := mt.Subscribe("createFailed", createFailedSuber)
	if err == nil {
		t.Error("test create failed")
		return
	}
	if !assert.Equal(t, 0, mt.Len(), mt) {
		return
	}
	// test first subscribe failed
	firstSubscriber := &testSubscriber{v: -1}
	err = mt.Subscribe("first", firstSubscriber)
	if err == nil {
		t.Error("test first subscribe failed")
		return
	}
	if !assert.Equal(t, 0, mt.Len(), mt) {
		return
	}
	_ = mt.DestroyAll()
}

func TestMultiTopicDelay(t *testing.T) {
	promptTopic := NewMultiTopic(nil, 0)
	s := &testSubscriber{}
	id := 1
	_ = promptTopic.Subscribe(id, s)
	if !assert.Equal(t, 1, promptTopic.Len()) {
		return
	}
	_ = promptTopic.Unsubscribe(id, s)
	if !assert.Equal(t, 0, promptTopic.Len()) {
		return
	}
	delayTopic := NewMultiTopic(nil, time.Millisecond*20)
	_ = delayTopic.Subscribe(id, s)
	if !assert.Equal(t, 1, delayTopic.Len()) {
		return
	}
	_ = delayTopic.Unsubscribe(id, s)
	if !assert.Equal(t, 1, delayTopic.Len()) {
		return
	}
	<-time.After(time.Millisecond * 30)
	if !assert.Equal(t, 0, delayTopic.Len()) {
		return
	}
}
