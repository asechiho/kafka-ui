package store

import (
	"context"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"strings"
	"sync"
	"time"

	"backend/config"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
)

const (
	CollectionName  = "kafka"
	CollectionIndex = "topic"
)

type MService interface {
	Serve()
	Stop()
}

type MongoDBService struct {
	configure      *config.Configure `di.inject:"appConfigure"`
	connectionPool map[uuid.UUID]*mongo.Client
	topics         []string
	newTopicChan   chan string
	mutex          sync.RWMutex
}

func (mongoDB *MongoDBService) InitializeContext() error {
	var (
		index      = CollectionIndex
		connection *mongo.Client
		err        error
		id         uuid.UUID
		indexModel = mongo.IndexModel{
			Keys: bson.M{
				"topic": 1,
			},
			Options: &options.IndexOptions{
				Name: &index,
			},
		}
	)
	mongoDB.mutex = sync.RWMutex{}
	mongoDB.connectionPool = make(map[uuid.UUID]*mongo.Client)
	mongoDB.newTopicChan = make(chan string)

	if id, err = mongoDB.connect(mongoDB.configure.GlobalContext); err != nil {
		return err
	}

	connection = mongoDB.getConnection(id)
	if err = connection.Database(mongoDB.configure.Config.DatabaseName).CreateCollection(mongoDB.configure.GlobalContext, CollectionName); err != nil {
		log.Warnf("Create Collection: %s", err.Error())
	}

	if _, err = mongoDB.getCollection(id).Indexes().CreateOne(mongoDB.configure.GlobalContext, indexModel); err != nil {
		log.Warnf("Create Index: %s", err.Error())
	}

	return nil
}

func (mongoDB *MongoDBService) Topics(socketContext context.Context, startChan <-chan interface{}) <-chan Message {
	var (
		err     error
		msgChan = make(chan Message, 1)
		topic   string
	)

	go func() {
		defer close(msgChan)

		//todo: add retires
		id, _ := mongoDB.connect(socketContext)
		defer mongoDB.close(id)

		for {
			select {
			case <-socketContext.Done():
				log.Info("Close MongoDB connection for read topics. Socket context close")
				return

			case <-mongoDB.configure.GlobalContext.Done():
				log.Info("Close MongoDB connection for read topics. Application context close")
				return

			case topic = <-mongoDB.newTopicChan:
				log.Tracef("Get new topic: %s", topic)
				msgChan <- Message{Topic: topic}

			case <-startChan:
				var topics []interface{}
				if topics, err = mongoDB.getCollection(id).Distinct(mongoDB.configure.GlobalContext, "topic", bson.D{{}}); err != nil {
					log.Warnf("Distinct topic: %s", err.Error())
				}

				for _, v := range topics {
					msgChan <- Message{Topic: v.(string)}
				}
			}
		}
	}()

	return msgChan
}

func (mongoDB *MongoDBService) Messages(socketContext context.Context, filterChan <-chan Filters) <-chan Message {
	msgChan := make(chan Message, 1)

	go func() {
		var (
			filter      Filters
			changesChan = make(chan Message)
		)
		defer close(msgChan)

		//todo: retries
		id, _ := mongoDB.connect(socketContext)
		defer mongoDB.close(id)

		mongoDB.listenChanges(socketContext, id, changesChan)

		for {
			select {
			case <-socketContext.Done():
				log.Info("Close MongoDB connection to push messages. Socket context close")
				return

			case <-mongoDB.configure.GlobalContext.Done():
				log.Info("Close MongoDB connection to read messages. Application context close")
				return

			case filter = <-filterChan:
				mongoDB.getLastMessages(id, msgChan, filter, filter.Size)

			case msg := <-changesChan:
				if msg.Filter(filter) {
					msgChan <- msg
				}
			}
		}
	}()

	return msgChan
}

func (mongoDB *MongoDBService) Serve() {
	var (
		err        error
		retryCount = 10
	)

	for retryCount > 0 {
		if err = mongoDB.InitializeContext(); err != nil {
			retryCount--
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}

	if retryCount == 0 && err != nil {
		log.Fatalf("Db error: %s", err.Error())
	}

	go func() {
		var (
			topics       []interface{}
			err          error
			insertResult *mongo.InsertOneResult
		)

		id, _ := mongoDB.connect(mongoDB.configure.GlobalContext)
		defer mongoDB.close(id)

		// init start topics
		if topics, err = mongoDB.getCollection(id).Distinct(mongoDB.configure.GlobalContext, "topic", bson.D{{}}); err != nil {
			log.Warnf("Distinct topic: %s", err.Error())
		}

		for _, v := range topics {
			if !strings.Contains(v.(string), SkipTopics) {
				mongoDB.topics = append(mongoDB.topics, v.(string))
			}
		}

		for {
			select {
			case <-mongoDB.configure.GlobalContext.Done():
				return

			case msg, ok := <-mongoDB.configure.ServeReadChannel():
				if !ok {
					return
				}

				mongoDB.appendTopic(msg.(Message).Topic)

				if insertResult, err = mongoDB.getCollection(id).InsertOne(mongoDB.configure.GlobalContext, msg.(Message)); err != nil {
					log.Warnf("Insert message error: %s", err.Error())
					continue
				}

				log.Infof("Insert document: %v. Result: %s", msg.(Message), insertResult.InsertedID)
			}
		}
	}()
}

func (mongoDB *MongoDBService) Stop() {
}

func (mongoDB *MongoDBService) getLastMessages(id uuid.UUID, msgChan chan Message, filters Filters, count int) {
	var (
		topicFilter = bson.M{}
		cursor      *mongo.Cursor
		err         error
	)

	if filters.Topic != "" && filters.Topic != "all" {
		topicFilter = bson.M{
			CollectionIndex: filters.Topic,
		}
	}

	cursor, err = mongoDB.getCollection(id).Find(mongoDB.configure.GlobalContext, topicFilter, options.Find().SetSort(bson.D{{"offset", 1}}).SetLimit(int64(count)))
	if err != nil {
		log.Warnf("Get desc error: %s", err.Error())
		return
	}

	var msgs []Message
	if err = cursor.All(mongoDB.configure.GlobalContext, &msgs); err != nil {
		log.Warnf("Get all messages error: %s", err.Error())
		return
	}

	for _, msg := range msgs {
		if msg.Filter(filters) {
			msgChan <- msg
		}
	}
}

func (mongoDB *MongoDBService) listenChanges(socketContext context.Context, id uuid.UUID, msgChan chan Message) {
	go func() {
		var (
			collection *mongo.Collection
			stream     *mongo.ChangeStream
			err        error
		)

		collection = mongoDB.getCollection(id)
		for {
			select {
			case <-socketContext.Done():
				log.Info("Close MongoDB connection to listen changes. Socket context close")
				return

			case <-mongoDB.configure.GlobalContext.Done():
				log.Info("Close MongoDB connection to listen changes. Application context close")
				return

			default:
				if stream == nil {
					if stream, err = collection.Watch(socketContext, mongo.Pipeline{}, options.ChangeStream()); err != nil {
						log.Errorf("MongoDB get changes error: %s", err.Error())
						continue
					}
				}

				for stream.Next(socketContext) {
					changeDoc := struct {
						FullDocument Message `bson:"fullDocument"`
					}{}
					if err = stream.Decode(&changeDoc); err != nil {
						log.Warnf("MongoDB change decode: %s", err.Error())
					}
					msgChan <- changeDoc.FullDocument
				}
			}
		}
	}()
}

func (mongoDB *MongoDBService) connect(ctx context.Context) (uuid.UUID, error) {
	var (
		client *mongo.Client
		err    error
	)

	if client, err = mongo.Connect(ctx, options.Client().SetHosts([]string{mongoDB.configure.Config.DatabaseServer()}).SetReplicaSet("rs0")); err != nil {
		return [16]byte{}, err
	}

	return mongoDB.createConnection(client), nil
}

func (mongoDB *MongoDBService) appendTopic(topic string) {
	for _, v := range mongoDB.topics {
		if v == topic || strings.Contains(v, SkipTopics) {
			return
		}
	}
	mongoDB.topics = append(mongoDB.topics, topic)
	log.Tracef("Send new topic: %s", topic)
	mongoDB.newTopicChan <- topic
}

func (mongoDB *MongoDBService) close(id uuid.UUID) {
	_ = mongoDB.getConnection(id).Disconnect(mongoDB.configure.GlobalContext)
	mongoDB.mutex.Lock()
	delete(mongoDB.connectionPool, id)
	mongoDB.mutex.Unlock()
}

func (mongoDB *MongoDBService) getCollection(id uuid.UUID) *mongo.Collection {
	return mongoDB.getConnection(id).
		Database(mongoDB.configure.Config.DatabaseName).
		Collection(CollectionName)
}

func (mongoDB *MongoDBService) getConnection(id uuid.UUID) *mongo.Client {
	var client *mongo.Client
	mongoDB.mutex.RLock()
	client = mongoDB.connectionPool[id]
	mongoDB.mutex.RUnlock()
	return client
}

func (mongoDB *MongoDBService) createConnection(client *mongo.Client) uuid.UUID {
	var id = uuid.New()
	mongoDB.mutex.Lock()
	mongoDB.connectionPool[id] = client
	mongoDB.mutex.Unlock()
	return id
}
