package store

import (
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

const messageFilterFields = "offset;partition;timestamp;at;size;"

type Message struct {
	Topic     string                 `rethinkdb:"topic"     bson:"topic"`
	Headers   map[string]string      `rethinkdb:"headers"   bson:"headers"`
	Offset    int                    `rethinkdb:"offset"    bson:"offset"`
	Partition int                    `rethinkdb:"partition" bson:"partition"`
	Timestamp int64                  `rethinkdb:"timestamp" bson:"timestamp"`
	At        time.Time              `rethinkdb:"at"        bson:"at"`
	Size      int                    `rethinkdb:"size"      bson:"size"`
	Message   map[string]interface{} `rethinkdb:"message"   bson:"message"`
}

func (message Message) Filter(filters Filters) bool {
	if filters.Topic == "" && len(filters.Filters) == 0 {
		return false
	}

	if filters.Topic != "" && !strings.EqualFold(message.Topic, filters.Topic) {
		return false
	}

	for _, filter := range filters.Filters {
		if filter.FieldName == "" {
			continue
		}

		r := reflect.ValueOf(message)

		if strings.Contains(messageFilterFields, strings.ToLower(filter.FieldName)) {
			val := r.FieldByName(strings.Title(strings.ToLower(filter.FieldName)))
			log.Tracef("Message value: %s for field %s, filter value: %s", val, strings.Title(strings.ToLower(filter.FieldName)), filter.FieldValue)
			if !filter.Compare(val.Interface(), filter.FieldValue) {
				return false
			}
			continue
		}

		if val, ok := message.Headers[filter.FieldName]; ok {
			log.Tracef("Filter: compare header %s, message value: %s, filter value: %s", filter.FieldName, val, filter.FieldValue.(string))
			if !filter.Compare(val, filter.FieldValue) {
				return false
			}
			continue
		}
		return false
	}

	return true
}

type Changes struct {
	OldValue Message `rethinkdb:"old_val"`
	NewValue Message `rethinkdb:"new_val"`
}

func New(msg kafka.Message) Message {
	var (
		offset int64
		err    error
	)

	keyValue := map[string]string{}
	for _, header := range msg.Headers {
		keyValue[header.Key] = string(header.Value)
	}

	if offset, err = strconv.ParseInt(msg.TopicPartition.Offset.String(), 10, 64); err != nil {
		log.Warnf("Offset parse error: %s", err.Error())
		offset = 0
	}

	body := map[string]interface{}{}
	if err = json.Unmarshal(msg.Value, &body); err != nil {
		log.Warnf("Body parse error: %s", err.Error())
	}

	return Message{
		Topic:     *msg.TopicPartition.Topic,
		Headers:   keyValue,
		Offset:    int(offset),
		Partition: int(msg.TopicPartition.Partition),
		Timestamp: msg.Timestamp.Unix(),
		At:        msg.Timestamp,
		Size:      len(msg.Value),
		Message:   body,
	}
}

type Comparator interface {
	Compare(interface{}, interface{}) bool
}

type Filters struct {
	Topic   string
	Filters []Filter
	Size    int
}

func (filters Filters) findOffset() (Filter, bool) {
	for _, filter := range filters.Filters {
		if "offset" == filter.FieldName {
			return filter, true
		}
	}

	return Filter{}, false
}

func (filters Filters) findByName(name string) []Filter {
	var findSlice []Filter

	for _, filter := range filters.Filters {
		if strings.EqualFold(filter.FieldName, name) {
			findSlice = append(findSlice, filter)
		}
	}

	return findSlice
}

type Filter struct {
	FieldName     string
	FieldValue    interface{}
	Comparator    Comparator
	MongoOperator string
}

func (filter Filter) Compare(left, right interface{}) bool {
	return filter.Comparator.Compare(left, right)
}
