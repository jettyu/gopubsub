package pubsub

import "errors"

var (
	// ErrorTopicClosed ...
	ErrorTopicClosed = errors.New("topic closed")
)

// Subscriber ....
type Subscriber interface {
	ID() interface{}
	OnPublish(interface{}) error
}

// Publisher ...
type Publisher interface {
	Publish(interface{}) error
}

// Topic ...
type Topic interface {
	Publisher
	Subscribe(Subscriber) (err error)
	Unsubscribe(Subscriber) error
	Refresh(Topic) (bool, error)
}

// TopicCenter ...
type TopicCenter interface {
	GetAndCreate(id interface{}) (topic Topic, err error)
	Get(id interface{}) (Topic, bool)
	Remove(id interface{}, topic Topic)
}
