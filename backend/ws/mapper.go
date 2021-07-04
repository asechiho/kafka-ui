package ws

import (
	"fmt"
	log "github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"time"

	"backend/store"
)

var messageFilterFields = map[CastType]string{
	CastTypeStr: "topic;at",
	CastTypeInt: "offset;partition;timestamp;size",
}

func ConvertToWsMessage(message store.Message) Messages {
	return Messages{
		Message: Message{
			Topic:       message.Topic,
			Headers:     message.Headers,
			Offset:      strconv.FormatInt(int64(message.Offset), 10),
			Partition:   string(rune(message.Partition)),
			Timestamp:   strconv.FormatInt(message.Timestamp, 10),
			At:          message.At.Format(time.RFC3339),
			PayloadSize: strconv.Itoa(message.Size),
			Payload:     message.Message,
		},
	}
}

func ConvertToWsTopic(message store.Message) Topic {
	return Topic{
		Topic: Message{
			Topic: message.Topic,
		},
	}
}

func ConvertToStoreFilter(request MessageRequest) (result store.Filters) {
	if len(request.Filters) == 0 {
		return store.Filters{
			Size: 20,
		}
	}

	var err error
	if result.Size, err = strconv.Atoi(request.Size); err != nil {
		result.Size = 20
		log.Warnf("Parse size error: %s", err.Error())
	}

	for _, filter := range request.Filters {
		if filter.Param == "topic" {
			result.Topic = filter.Value
			continue
		}

		var castTypeValue = getCastType(filter.Param)
		result.Filters = append(result.Filters, store.Filter{
			FieldName:     filter.Param,
			FieldValue:    castType(filter.Value, castTypeValue),
			Comparator:    New(filter.Operator, castTypeValue),
			MongoOperator: fmt.Sprintf("$%s", filter.Operator.String()),
		})
	}
	return
}

func getCastType(fieldName string) CastType {
	for t, v := range messageFilterFields {
		if strings.Contains(v, strings.ToLower(fieldName)) {
			return t
		}
	}
	return CastTypeStr
}

func castType(val interface{}, castType2 CastType) interface{} {
	switch castType2 {
	case CastTypeInt:
		return castToInt(val)
	case CastTypeStr:
		return val
	default:
		return val

	}
}

func castToInt(right interface{}) int64 {
	var (
		number int64
		err    error
	)

	if number, err = strconv.ParseInt(right.(string), 10, 64); err != nil {
		log.Debugf("Filter value %v parse error: %s", right, err.Error())
	}

	return number
}
