package pubsub_test

import (
	"testing"

	pubsub "github.com/jettyu/gopubsub"
)

type testObserver struct {
	V int
}

func (p *testObserver) ID() interface{} {
	return p
}

func (p *testObserver) OnPublish(v interface{}) error {
	p.V = v.(int)
	return nil
}

func TestPublisher(t *testing.T) {
	sub := pubsub.NewConcreteTopic(nil)
	var (
		ob1 testObserver
		ob2 testObserver
	)
	sub.Subscribe(&ob1)
	sub.Subscribe(&ob2)
	sub.Publish(1)
	if ob1.V != 1 || ob2.V != 1 {
		t.Fatal(ob1, ob2)
	}
	sub.Unsubscribe(&ob2)
	sub.Publish(2)
	if ob1.V != 2 || ob2.V != 1 {
		t.Fatal(ob1, ob2)
	}
}

func TestCenter(t *testing.T) {
	center := pubsub.NewRealTopicCenter(pubsub.NewRealTopic(nil))
	pb, _ := center.GetAndCreate(1)
	ob1 := &testObserver{}
	ob2 := &testObserver{}
	pb.Subscribe(ob1)
	pb.Subscribe(ob2)
	pb.Publish(1)
	if ob1.V != 1 {
		t.Fatal(ob1)
	}
	pb.Unsubscribe(ob1)
	pb.Publish(2)
	if ob1.V != 1 || ob2.V != 2 {
		t.Fatal(ob1, ob2)
	}
	pb.Unsubscribe(ob2)
}

func TestCenterConcrete(t *testing.T) {
	center := pubsub.NewRealTopicCenter(pubsub.NewRealTopic(pubsub.NewConcreteTopicFunc(nil)))
	pb, _ := center.GetAndCreate(1)
	ob1 := &testObserver{}
	ob2 := &testObserver{}
	pb.Subscribe(ob1)
	pb.Subscribe(ob2)
	pb.Publish(1)
	if ob1.V != 1 {
		t.Fatal(ob1)
	}
	pb.Unsubscribe(ob1)
	pb.Publish(2)
	if ob1.V != 1 || ob2.V != 2 {
		t.Fatal(ob1, ob2)
	}
	pb.Unsubscribe(ob2)
}
