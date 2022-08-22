package mq

import (
	"fmt"

	"go.uber.org/zap"
)

type infoLogger struct {
	internal *zap.Logger
}

func (l infoLogger) Printf(format string, v ...interface{}) {
	l.internal.Info(fmt.Sprintf(format, v...))
}

type errorLogger struct {
	internal *zap.Logger
}

func (l errorLogger) Printf(format string, v ...interface{}) {
	l.internal.Error(fmt.Sprintf(format, v...))
}

type Config struct {
	Brokers  []string
	Topic    string
	GroupId  string
	Username string
	Password string
}

type options struct {
	username string
	password string
}

type FuncOptions func(o *options)

func WithAuthenticate(username, password string) FuncOptions {
	return func(o *options) {
		o.username = username
		o.password = password
	}
}
