package rabbitmq

import (
	"context"
	"github.com/google/uuid"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type Messaging interface {
	AddWorker(queue string, handler WorkerHandler) error
	AddWorkerWithOptions(queue string, handler WorkerHandler, options *WorkerOptions) error
	SendPush(queue string, data *interface{}) error
	SendPushWithOptions(queue string, data *interface{}, options *PushOptions) error
	RpcCall(queue string, data *interface{}) (error, interface{})
	Publish(queue string, data *interface{}) error
	PublishWithOptions(queue string, data *interface{}, options *PublishOptions) error
	Subscribe(queue string, handler SubscribeHandler) error
	SubscribeWithOptions(queue string, handler SubscribeHandler, options *SubscribeOptions) error
	CancelWorkers() error

	RegisterType(value interface{})
}

type messaging struct {
	logger      Logger
	chManager   ChannelManager
	workers     map[string]Worker
	subscribers map[string][]Subscriber
	replies     map[string]chan interface{}
	replyQueue  string
}

// AddWorker adds a worker function for push queue or rpc call.
func (m *messaging) AddWorker(queue string, handler WorkerHandler) error {
	return m.AddWorkerWithOptions(queue, handler, defaultWorkerOptions)
}

// AddWorkerWithOptions adds a worker function for push queue or rpc call with custom options.
func (m *messaging) AddWorkerWithOptions(queue string, handler WorkerHandler, options *WorkerOptions) error {
	if _, ok := m.workers[queue]; ok {
		m.logger.Warn("the worker '%s' already exists, this handler will ignored.", queue)
		return nil
	}
	m.workers[queue] = Worker{
		handler,
		options,
	}
	err := m.handleWorker(queue, handler, options)
	if err != nil {
		m.logger.Error("handle worker error: %v", err)
		return err
	}
	return nil
}

// SendPush push a message to worker.
func (m *messaging) SendPush(queue string, data *interface{}) error {
	return m.SendPushWithOptions(queue, data, defaultPushOptions)
}

// SendPushWithOptions push a message to worker with custom options.
func (m *messaging) SendPushWithOptions(queue string, data *interface{}, options *PushOptions) error {
	var deliveryMode = amqp.Transient
	if options.Persistent {
		deliveryMode = amqp.Persistent
	}
	err := m.safePublish(queue, "", data, "", deliveryMode, "")
	if err != nil {
		m.logger.Error("can't push message: %v", err)
		return err
	}
	return nil
}

// RpcCall push a message to worker and wait it result.
func (m *messaging) RpcCall(queue string, data *interface{}) (error, interface{}) {
	corrId := uuid.NewString()
	resultChan := make(chan interface{})
	m.replies[corrId] = resultChan

	if m.replyQueue == "" {
		q, err := m.chManager.getChannel().QueueDeclare(
			"",
			false,
			false,
			true,
			false,
			nil,
		)
		if err != nil {
			return err, nil
		}
		message, err := m.chManager.getChannel().Consume(
			q.Name,
			"",
			true,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			return err, nil
		}
		m.replyQueue = q.Name
		go func() {
			for msg := range message {
				if channel, ok := m.replies[msg.CorrelationId]; ok {
					decoded := m.decodeMessage(msg)
					channel <- decoded
				}
			}
		}()
	}

	err := m.safePublish(queue, "", data, m.replyQueue, amqp.Transient, corrId)
	if err != nil {
		m.logger.Error("can't push rpc message: %v", err)
		return err, nil
	}

	result := <-resultChan
	delete(m.replies, corrId)
	return nil, result
}

// Publish push a message to all subscribers in queue.
func (m *messaging) Publish(queue string, data *interface{}) error {
	return m.PublishWithOptions(queue, data, defaultPublishOptions)
}

// PublishWithOptions push a message to all subscribers in queue with custom options.
func (m *messaging) PublishWithOptions(queue string, data *interface{}, options *PublishOptions) error {
	err := m.chManager.getChannel().ExchangeDeclare(
		queue,
		"fanout",
		options.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = m.safePublish("", queue, data, "", amqp.Transient, "")
	if err != nil {
		return err
	}
	return nil
}

// Subscribe define a handler to process message.
func (m *messaging) Subscribe(queue string, handler SubscribeHandler) error {
	return m.SubscribeWithOptions(queue, handler, defaultSubscribeOptions)
}

// SubscribeWithOptions define a handler to process message with custom options.
func (m *messaging) SubscribeWithOptions(queue string, handler SubscribeHandler, options *SubscribeOptions) error {
	if _, ok := m.workers[queue]; ok {
		m.logger.Warn("the '%s' already exists as worker, this handler will ignored.", queue)
		return nil
	}
	m.subscribers[queue] = append(m.subscribers[queue], Subscriber{
		handler,
		options,
	})
	err := m.handleSubscriber(queue, handler, options)
	if err != nil {
		m.logger.Error("handle subscriber error: %v", err)
		return err
	}
	return nil
}

// CancelWorkers remove all worker and subscribes function.
func (m *messaging) CancelWorkers() error {
	m.logger.Info("closing workers...")
	for k := range m.workers {
		delete(m.workers, k)
	}
	m.logger.Info("closing subscribers...")
	for k := range m.subscribers {
		delete(m.subscribers, k)
	}
	m.replyQueue = ""
	for k := range m.replies {
		delete(m.replies, k)
	}
	return m.chManager.close()
}

func (m *messaging) RegisterType(value interface{}) {
	registerType(value)
}

func (m *messaging) handleWorker(queue string, handler WorkerHandler, options *WorkerOptions) error {
	q, err := m.chManager.getChannel().QueueDeclare(
		queue,
		options.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	err = m.chManager.getChannel().Qos(
		options.QOSPrefetch,
		0,
		options.QOSGlobal,
	)
	if err != nil {
		return err
	}

	message, err := m.chManager.getChannel().Consume(
		q.Name,
		"",
		options.AutoAck,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	m.logger.Info("worker for queue '%s' added, waiting for incoming message on %v goroutines.", queue, options.Concurrency)
	for a := 0; a < options.Concurrency; a++ {
		go m.handleWorkerMessage(message, handler, options)
	}
	return nil
}

func (m *messaging) handleWorkerMessage(message <-chan amqp.Delivery, handler WorkerHandler, options *WorkerOptions) {
	for msg := range message {
		decoded := m.decodeMessage(msg)
		resultHandler, ackHandler := handler(Message{decoded})
		if msg.ReplyTo != "" {
			err := m.safePublish(msg.ReplyTo, "", resultHandler, "", amqp.Transient, msg.CorrelationId)
			if err != nil {
				m.logger.Error("can't push reply message: %v", err)
			}
		}
		if !options.AutoAck {
			switch ackHandler {
			case Ack:
				err := msg.Ack(false)
				if err != nil {
					m.logger.Error("can't ack message: %v", err)
				}
			case NackDiscard:
				err := msg.Nack(false, false)
				if err != nil {
					m.logger.Error("can't nack message: %v", err)
				}
			case NackRequeue:
				err := msg.Nack(false, true)
				if err != nil {
					m.logger.Error("can't nack message: %v", err)
				}
			}
		}
	}
	m.logger.Info("worker goroutine closed.")
}

func (m *messaging) handleSubscriber(queue string, handler SubscribeHandler, options *SubscribeOptions) error {
	err := m.chManager.getChannel().ExchangeDeclare(
		queue,
		"fanout",
		options.Durable,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	q, err := m.chManager.getChannel().QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	err = m.chManager.getChannel().QueueBind(
		q.Name,
		"",
		queue,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	message, err := m.chManager.getChannel().Consume(
		q.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	m.logger.Info("subscribe for queue '%s' added, waiting for incoming message on %v goroutines.", queue, options.Concurrency)
	for a := 0; a < options.Concurrency; a++ {
		go func() {
			for msg := range message {
				decoded := m.decodeMessage(msg)
				handler(Message{decoded})
			}
		}()
	}
	return nil
}

func (m *messaging) safePublish(key string, exchange string, data *interface{}, replyTo string, deliveryMode uint8,
	correlationId string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := m.chManager.checkFlow()
	if err != nil {
		return err
	}

	err = m.chManager.checkBlockConnection()
	if err != nil {
		return err
	}

	var body []byte = nil
	if data != nil {
		encoded, err := encode(data)
		if err != nil {
			m.logger.Error("message encode error: %v", err)
			return err
		}
		body = encoded
	}
	return m.chManager.getChannel().PublishWithContext(ctx,
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType:   "application/octet-stream",
			DeliveryMode:  deliveryMode,
			CorrelationId: correlationId,
			ReplyTo:       replyTo,
			Body:          body,
		},
	)
}

func (m *messaging) decodeMessage(msg amqp.Delivery) interface{} {
	var decoded interface{} = nil
	if msg.Body != nil {
		err := decode(msg.Body, &decoded)
		if err != nil {
			m.logger.Error("message decode error: %v", err)
		}
	}
	return decoded
}

func (m *messaging) registerWorkersOnRestart() {
	for err := range m.chManager.onChannelRestart() {
		m.logger.Info("successful restart from: %v", err)
		for k, v := range m.workers {
			err = m.handleWorker(k, v.handler, v.options)
			if err != nil {
				m.logger.Error("error restarting worker goroutines after cancel reset: %v", err)
			}
		}
		for k, v := range m.subscribers {
			for _, s := range v {
				err = m.handleSubscriber(k, s.handler, s.options)
				if err != nil {
					m.logger.Error("error restarting subscriber goroutines after cancel reset: %v", err)
				}
			}
		}
		m.replyQueue = ""
		if err != nil {
			m.logger.Error("error restarting reply queue after cancel reset: %v", err)
		}
	}
}

func NewMessaging(url string) (Messaging, error) {
	return NewMessagingWithOptions(url, defaultMessagingOptions)
}

func NewMessagingWithOptions(url string, options *MessagingOptions) (Messaging, error) {
	chManager, err := newChannelManager(url, options.Logger, options.ReconnectInterval)
	if err != nil {
		return nil, err
	}
	m := messaging{
		options.Logger,
		chManager,
		make(map[string]Worker),
		make(map[string][]Subscriber),
		make(map[string]chan interface{}),
		"",
	}
	go m.registerWorkersOnRestart()
	return &m, nil
}
