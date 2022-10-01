package rabbitmq

import "time"

// Message captures the fields for a message received from queue.
type Message struct {
	Body interface{}
}

// Acknowledge is an action that occurs after processed this message.
type Acknowledge int

const (
	// Ack default ack this msg after you have successfully processed this message.
	Ack Acknowledge = iota
	// NackDiscard the message will be dropped or delivered to a server configured dead-letter queue.
	NackDiscard
	// NackRequeue deliver this message to a different consumer.
	NackRequeue
	// None for auto ack option.
	None
)

// WorkerHandler defines the worker function with Data message param.
type WorkerHandler func(message Message) (*interface{}, Acknowledge)

// SubscribeHandler defines the subscribe function.
type SubscribeHandler func(message Message)

type Worker struct {
	handler WorkerHandler
	options *WorkerOptions
}

type Subscriber struct {
	handler SubscribeHandler
	options *SubscribeOptions
}

// WorkerOptions defines options of worker.
type WorkerOptions struct {
	AutoAck     bool
	Durable     bool
	QOSPrefetch int
	QOSGlobal   bool
	Concurrency int
}

// SubscribeOptions defines options of subscribe.
type SubscribeOptions struct {
	Durable     bool
	Concurrency int
}

// PublishOptions defines options of publish.
type PublishOptions struct {
	Durable bool
}

// PushOptions defines options of worker.
type PushOptions struct {
	Persistent bool
}

// MessagingOptions defines options of messaging.
type MessagingOptions struct {
	Logger            Logger
	ReconnectInterval time.Duration
}
