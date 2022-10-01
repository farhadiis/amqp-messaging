package rabbitmq

import "time"

var (
	defaultWorkerOptions = &WorkerOptions{
		AutoAck:     true,
		Durable:     true,
		QOSPrefetch: 1,
		QOSGlobal:   false,
		Concurrency: 1,
	}
	defaultPushOptions = &PushOptions{
		Persistent: true,
	}
	defaultSubscribeOptions = &SubscribeOptions{
		Durable:     true,
		Concurrency: 1,
	}
	defaultPublishOptions = &PublishOptions{
		Durable: true,
	}
	defaultMessagingOptions = &MessagingOptions{
		Logger:            &debugLogger{},
		ReconnectInterval: time.Second * 5,
	}
)
