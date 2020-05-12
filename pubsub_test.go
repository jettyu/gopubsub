package pubsub_test

import (
	"testing"

	pubsub "github.com/jettyu/gopubsub"
	"github.com/stretchr/testify/assert"
)

type testSubscriber struct {
	v int
}

func (p *testSubscriber) OnPublish(v interface{}) error {
	p.v = v.(int)
	return nil
}

func testTopic(t *testing.T, topic pubsub.Topic) {
	var (
		ob1 testSubscriber
		ob2 testSubscriber
	)
	topic.Subscribe(&ob1)
	topic.Subscribe(&ob2)
	topic.Publish(1)
	if !assert.Equal(t, 1, ob1.v) {
		return
	}
	if !assert.Equal(t, 1, ob2.v) {
		return
	}

	topic.Unsubscribe(&ob2)
	topic.Publish(2)
	if !assert.Equal(t, 2, ob1.v) {
		return
	}
	if !assert.Equal(t, 1, ob2.v) {
		return
	}
	topic.Unsubscribe(&ob1)
	if !assert.Equal(t, true, topic.IsEmpty()) {
		return
	}
}

func TestTopic(t *testing.T) {
	testTopic(t, pubsub.NewDefaultTopic())
	testTopic(t, pubsub.NewSyncTopic())
	testTopic(t, pubsub.NewSafeTopic(nil))
}

func TestMultiTopic(t *testing.T) {
	mt := pubsub.NewMultiTopic(nil, 0)
	defer mt.DestroyAll()
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
			mt.Subscribe(id, suber)
		}
	}
	mt.Publish("a", 100)
	mt.Publish("b", 200)
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
	mt.PublishAll(300)
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
		mt.Unsubscribe("a", v)
	}
	if !assert.Equal(t, 1, mt.Len()) {
		return
	}
	mt.Range(func(id interface{}, topic pubsub.Topic) bool {
		if id.(string) == "a" {
			t.Error(topic)
			return false
		}
		return true
	})
	mt.Range(func(id interface{}, topic pubsub.Topic) bool {
		if id.(string) != "b" {
			return true
		}
		mt.Destroy("b", topic)
		return false
	})
	if !assert.Equal(t, 0, mt.Len()) {
		return
	}
}
