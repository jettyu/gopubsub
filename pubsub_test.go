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
	topic, _ := pubsub.NewTopic(nil, nil)
	var (
		ob1 testSubscriber
		ob2 testSubscriber
	)
	topic.Subscribe(&ob1)
	topic.Subscribe(&ob2)
	topic.Publish(1)
	if ob1.v != 1 || ob2.v != 1 {
		t.Fatal(ob1, ob2)
	}
	topic.Unsubscribe(&ob2)
	topic.Publish(2)
	if ob1.v != 2 || ob2.v != 1 {
		t.Fatal(ob1, ob2)
	}
}

func TestMultiTopic(t *testing.T) {
	mt := pubsub.NewMultiTopic(nil, false)
	defer mt.Destroy()
	subers := map[string][]*testSubscriber{
		"a": []*testSubscriber{
			&testSubscriber{
				1,
			},
			&testSubscriber{
				1,
			},
		},
		"b": []*testSubscriber{
			&testSubscriber{
				1,
			},
			&testSubscriber{
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
		if v.v != 100 {
			t.Fatal(subers)
		}
	}
	for _, v := range subers["b"] {
		if v.v != 200 {
			t.Fatal(subers)
		}
	}
	for _, v := range subers["a"] {
		mt.Unsubscribe("a", v)
	}
}
