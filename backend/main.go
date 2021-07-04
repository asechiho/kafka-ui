package main

import (
	"context"
	"fmt"
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
	log.SetLevel(log.InfoLevel)

	app := initContainers()

	if err := app.Run(); err != nil {
		log.Error(err.Error())
		os.Exit(1)
	}

	os.Exit(0)
}

func initContainers() *application.Application {
	var err error

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), store.NewTopicChan, make(chan string)))

	_, _ = di.RegisterBeanInstance("appContext", ctx)
	_, _ = di.RegisterBeanInstance("appConfig", new(config.Config).Defaults())
	_, _ = di.RegisterBean("appConfigure", reflect.TypeOf((*config.Configure)(nil)))
	_, _ = di.RegisterBean("wsService", reflect.TypeOf((*ws.WsService)(nil)))
	_, _ = di.RegisterBean("providerService", reflect.TypeOf((*provider.Provider)(nil)))
	_, _ = di.RegisterBean("storeService", reflect.TypeOf((*store.Store)(nil)))

	_ = di.RegisterBeanPostprocessor(reflect.TypeOf((*config.Configure)(nil)), func(instance interface{}) error {
		var configure *config.Configure
		if configure, err = instance.(*config.Configure).LoadConfig(); err != nil {
			return err
		}
		instance = configure
		return nil
	})

	_ = di.RegisterBeanPostprocessor(reflect.TypeOf((*store.Store)(nil)), func(instance interface{}) error {
		var configure = di.GetInstance("appConfigure").(*config.Configure)

		var dbService store.Service
		switch configure.Config.DatabaseType() {
		case config.DatabaseTypeMongo:
			dbService = &store.MongoDBService{}
		case config.DatabaseTypeRethink:
			dbService = &store.RethinkDBService{}
		default:
			return fmt.Errorf("not supported database type: %s", configure.Config.Databasetype)
		}

		instance = instance.(*store.Store).SetService(dbService)
		return nil
	})

	_ = di.InitializeContainer()
	return application.New(cancel, "storeService", "providerService", "wsService")
}
