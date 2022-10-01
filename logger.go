package rabbitmq

import (
	"fmt"
	"log"
)

type Logger interface {
	Fatal(string, ...interface{})
	Error(string, ...interface{})
	Warn(string, ...interface{})
	Info(string, ...interface{})
	Debug(string, ...interface{})
	Trace(string, ...interface{})
}

const prefix = "[amqp-messaging]"

type debugLogger struct{}

func (l *debugLogger) Fatal(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s FATAL: %s", prefix, format), v...)
}

func (l *debugLogger) Error(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s ERROR: %s", prefix, format), v...)
}

func (l *debugLogger) Warn(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s WARN: %s", prefix, format), v...)
}

func (l *debugLogger) Info(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s INFO: %s", prefix, format), v...)
}

func (l *debugLogger) Debug(format string, v ...interface{}) {
	log.Printf(fmt.Sprintf("%s DEBUG: %s", prefix, format), v...)
}

func (l *debugLogger) Trace(format string, v ...interface{}) {}
