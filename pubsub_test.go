package pubsub_test

import (
	"testing"

	pubsub "github.com/jettyu/gopubsub"
)

type testSubscriber struct {
	v int
}

func (p *testSubscriber) OnPublish(v interface{}) error {
	p.v = v.(int)
	return nil
}

func TestPublisher(t *testing.T) {
	topic := pubsub.NewDefaultTopic()
	var (
		ob1 testSubscriber
		ob2 testSubscriber
	)
	_ = topic.Subscribe(&ob1)
	_ = topic.Subscribe(&ob2)
	_ = topic.Publish(1)
	if ob1.v != 1 || ob2.v != 1 {
		t.Fatal(ob1, ob2)
	}
	_ = topic.Unsubscribe(&ob2)
	_ = topic.Publish(2)
	if ob1.v != 2 || ob2.v != 1 {
		t.Fatal(ob1, ob2)
	}
}

func TestMultiTopic(t *testing.T) {
	mt := pubsub.NewMultiTopic(nil, 0)
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
		if v.v != 100 {
			t.Fatal(subers)
		}
	}
	for _, v := range subers["b"] {
		if v.v != 200 {
			t.Fatal(subers)
		}
	}
	_ = mt.PublishAll(300)
	for _, v := range subers["a"] {
		if v.v != 300 {
			t.Fatal(subers)
		}
	}
	for _, v := range subers["b"] {
		if v.v != 300 {
			t.Fatal(subers)
		}
	}
	if mt.Len() != 2 {
		t.Fatal(mt.Len())
	}
	for _, v := range subers["a"] {
		_ = mt.Unsubscribe("a", v)
	}
	if mt.Len() != 1 {
		t.Fatal(mt.Len())
	}
	mt.Range(func(id interface{}, topic pubsub.Topic) bool {
		if id.(string) == "a" {
			t.Fatal(topic)
		}
		return true
	})
}
