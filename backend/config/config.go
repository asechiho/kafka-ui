package config

import (
	"context"
	"fmt"
	"github.com/heetch/confita/backend/env"

	"github.com/heetch/confita"
	"github.com/heetch/confita/backend/flags"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	WebSocketPort string `config:"ws-port"`
	KafkaHost     string `config:"kafka-host"`
	KafkaPort     string `config:"kafka-port"`
	KafkaGroup    string `config:"kafka-group-id"`
	Databasetype  string `config:"db-type"`
	DatabaseHost  string `config:"db-host"`
	DatabasePort  string `config:"db-port"`
	DatabaseName  string `config:"db-name"`
	TLSConfig     TLS
}

type TLS struct {
	CertificateAuthority string `config:"kafka-tls-ca"`
	Certificate          string `config:"kafka-tls-cert"`
	PrivateKey           string `config:"kafka-tls-key"`
}

func (config *Config) Defaults() *Config {
	config.WebSocketPort = "9002"
	config.KafkaHost = "127.0.0.1"
	config.KafkaPort = "9092"
	config.KafkaGroup = "kafka-ui-messages-fetch"
	config.DatabaseHost = "127.0.0.1"
	config.DatabasePort = "27017"
	config.DatabaseName = "softswiss"
	return config
}

func (config *Config) DatabaseServer() string {
	return fmt.Sprintf("%s:%s", config.DatabaseHost, config.DatabasePort)
}

func (config *Config) DatabaseType() DatabaseType {
	tp, _ := ParseDatabaseType(config.Databasetype)
	return tp
}

func (config *Config) IsTLS() bool {
	return config.TLSConfig.CertificateAuthority != "" && config.TLSConfig.Certificate != "" && config.TLSConfig.PrivateKey != ""
}

type Configure struct {
	GlobalContext    context.Context `di.inject:"appContext"`
	Config           *Config         `di.inject:"appConfig"`
	serveMessageChan chan interface{}
}

func (configure *Configure) ServeReadChannel() <-chan interface{} {
	return configure.serveMessageChan
}

func (configure *Configure) ServeWriteChannel() chan interface{} {
	return configure.serveMessageChan
}

func (configure *Configure) LoadConfig() (cfg *Configure, err error) {
	configure.serveMessageChan = make(chan interface{})

	if err = confita.NewLoader(env.NewBackend(), flags.NewBackend()).Load(context.Background(), configure.Config); err != nil {
		log.Warnf("Error load config: %s", err.Error())
		return configure, err
	}

	return configure, nil
}
