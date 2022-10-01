package rabbitmq

import "testing"

const url = "amqp://guest:guest@localhost:5672/"

// TestMessaging_AddWorker_SendPush define a worker and checking send push message in worker.
func TestMessaging_AddWorker_SendPush(t *testing.T) {
	var messaging, err = NewMessaging(url)
	if err != nil {
		t.Fatal(err)
	}
	c := make(chan interface{})
	err = messaging.AddWorker(
		"bar",
		func(message Message) (*interface{}, Acknowledge) {
			c <- message.Body
			return nil, None
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var data interface{} = "foo"
	err = messaging.SendPush("bar", &data)
	if err != nil {
		t.Fatal(err)
	}
	if data != <-c {
		t.Fatalf(`The data received in the worker must be equal to the sent in SendPush`)
	}
}

// TestMessaging_AddWorker_RpcCall define a worker and checking result return from Rpc call.
func TestMessaging_AddWorker_RpcCall(t *testing.T) {
	var messaging, err = NewMessaging(url)
	if err != nil {
		t.Fatal(err)
	}
	err = messaging.AddWorker(
		"foo",
		func(message Message) (*interface{}, Acknowledge) {
			var msg = message.Body.(string)
			var result interface{} = msg + "baz"
			return &result, None
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var data interface{} = "bar"
	err, result := messaging.RpcCall("foo", &data)
	if err != nil {
		t.Fatal(err)
	}
	if result != data.(string)+"baz" {
		t.Fatalf(`The return value of the worker must be equal to the value sent in RpcCall`)
	}
}

// TestMessaging_Publish_Subscribe define a subscriber and checking publish.
func TestMessaging_Publish_Subscribe(t *testing.T) {
	var messaging, err = NewMessaging(url)
	if err != nil {
		t.Fatal(err)
	}
	a, b := make(chan interface{}), make(chan interface{})
	err = messaging.Subscribe(
		"topic1",
		func(message Message) {
			a <- message.Body
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	err = messaging.Subscribe(
		"topic1",
		func(message Message) {
			b <- message.Body
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	var data interface{} = "foo"
	err = messaging.Publish("topic1", &data)
	if err != nil {
		t.Fatal(err)
	}
	if data != <-a || data != <-b {
		t.Fatalf(`The data received in the subscribers must be equal to the sent in Publish`)
	}
}