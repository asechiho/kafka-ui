package main

import (
	"context"
	"os"
	"reflect"

	"backend/application"
	"backend/config"
	"backend/provider"
	"backend/store"
	"backend/ws"

	"github.com/goioc/di"
	log "github.com/sirupsen/logrus"
)

func main() {
	log.SetFormatter(&log.JSONFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.TraceLevel)

	app := initContainers()

	if err := app.Run(); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func initContainers() *application.Application {
	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), store.NewTopicChan, make(chan string)))

	_, _ = di.RegisterBeanInstance("appContext", ctx)
	_, _ = di.RegisterBeanInstance("appConfig", new(config.Config).Defaults())
	_, _ = di.RegisterBean("appConfigure", reflect.TypeOf((*config.Configure)(nil)))
	_, _ = di.RegisterBean("wsService", reflect.TypeOf((*ws.WsService)(nil)))
	_, _ = di.RegisterBean("providerService", reflect.TypeOf((*provider.Provider)(nil)))
	_, _ = di.RegisterBean("storeService", reflect.TypeOf((*store.MongoDBService)(nil)))
	_ = di.InitializeContainer()

	if _, err := di.GetInstance("appConfigure").(*config.Configure).LoadConfig(); err != nil {
		log.Error(err.Error())
	}

	return application.New(cancel, "storeService", "providerService", "wsService")
}
