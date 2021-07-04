package store

import (
	"backend/config"
	"context"
)

const (
	NewTopicChan = "topicChan"
	SkipTopics   = "__consumer_offsets"
)

type Service interface {
	Serve()
	Stop()
	InitializeContext() error
	Topics(socketContext context.Context, startChan <-chan interface{}) <-chan Message
	Messages(socketContext context.Context, filterChan <-chan Filters) <-chan Message
	config(configure *config.Configure)
}

type Store struct {
	configure    *config.Configure `di.inject:"appConfigure"`
	innerService *Service
}

func (self *Store) GetService() Service {
	return *self.innerService
}

func (self *Store) SetService(service Service) *Store {
	service.config(self.configure)
	self.innerService = &service
	return self
}

func (self *Store) config(configure *config.Configure) {
	self.GetService().config(configure)
}

func (self *Store) Serve() {
	self.GetService().Serve()
}

func (self *Store) Stop() {
	self.GetService().Stop()
}

func (self *Store) InitializeContext() error {
	return self.GetService().InitializeContext()
}

func (self *Store) Topics(socketContext context.Context, startChan <-chan interface{}) <-chan Message {
	return self.GetService().Topics(socketContext, startChan)
}

func (self *Store) Messages(socketContext context.Context, filterChan <-chan Filters) <-chan Message {
	return self.GetService().Messages(socketContext, filterChan)
}
