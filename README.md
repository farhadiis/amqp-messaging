# AMQP-Messaging
Highly efficient library to communicate with other microservices using RabbitMQ(AMQP Protocol) in Go.

### Installing
Install via go get:
```shell
$ go get github.com/farhadiis/amqp-messaging
```

## Creating messaging variable
```go
import rabbitmq "github.com/farhadiis/amqp-messaging"

messaging := rabbitmq.NewMessaging("RABBITMQ-ADDRESS");
//now you can work with messaging APIs
``` 

## Work Queues
When you want to push to a queue and go ahead, You're not waiting for any responses. you can see this example:
```go
err := messaging.AddWorker("mySampleQueue", 
        func(message rabbitmq.Message) (interface{}, rabbitmq.Acknowledge) {
        log.Printf("Received a message: %s\n", message.Body)
        return nil, rabbitmq.None
    },
)
if err != nil {
	log.Fatal(err)
}

var data interface{} = "foo"
err = messaging.SendPush("mySampleQueue", &data)
if err != nil {
	log.Fatal(err)
}
``` 
default QOSPrefetch is set to 1 in options. If you set it more than 1 Rabbitmq will give more than 1 items at once.

## Publish/Subscribe
```go
err := messaging.Subscribe("mySampleTopic", func(message rabbitmq.Message) {
	log.Printf("Received a message: %s\n", message.Body)
})
if err != nil {
	log.Fatal(err)
}

var data interface{} = "foo"
err = messaging.Publish("mySampleTopic", &data)
if err != nil {
	log.Fatal(err)
}
```
## RPC
```go
type User struct {
	Id   string
	Name string
}
	
messaging.RegisterType(User{})
	
err := messaging.AddWorker("findUser",
    func(message rabbitmq.Message) (interface{}, rabbitmq.Acknowledge) {
        var id = message.Body.(string)
        var result interface{} = User{id, "Farhad"}
        return &result, rabbitmq.None
    },
)
if err != nil {
    log.Fatal(err)
}

var id interface{} = "12"
err, result := messaging.RpcCall("findUser", &id)
if err != nil {
	log.Fatal(err)
}
user := result.(User)
log.Printf("user: %v\n", user)
```


## Finish all jobs and stop all rabbitmq workers
You can stop Gracefully shutdown with command below. It will cancel all Workers.
```go
err := messaging.CancelWorkers()
if err != nil {
	log.Fatal(err)
}
```


## Develop
We're open for pull requests. in order to run tests just run `go test -v`

