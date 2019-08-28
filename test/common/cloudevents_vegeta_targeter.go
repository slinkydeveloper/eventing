package common

import (
	cloudevents "github.com/cloudevents/sdk-go"
	cehttp "github.com/cloudevents/sdk-go/pkg/cloudevents/transport/http"
	vegeta "github.com/tsenart/vegeta/lib"
	"math/rand"
	"strconv"
	"time"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

type CloudEventsTargeter struct {
	sinkUrl          string
	msgSize          int
	eventType        string
	eventSource      string
	encodingSelector cehttp.EncodingSelector
}

func NewCloudEventsTargeter(sinkUrl string, msgSize int, eventType string, eventSource string, encoding string) CloudEventsTargeter {
	var encodingSelector cehttp.EncodingSelector
	switch encoding {
	case "binary":
		encodingSelector = cehttp.DefaultBinaryEncodingSelectionStrategy
	case "structured":
		encodingSelector = cehttp.DefaultStructuredEncodingSelectionStrategy
	default:
		encodingSelector = cehttp.DefaultStructuredEncodingSelectionStrategy
	}
	return CloudEventsTargeter{
		sinkUrl:          sinkUrl,
		msgSize:          msgSize,
		eventType:        eventType,
		eventSource:      eventSource,
		encodingSelector: encodingSelector,
	}
}

func (cet CloudEventsTargeter) VegetaTargeter() vegeta.Targeter {
	seqStr := uint64(0)

	codec := cehttp.Codec{
		DefaultEncodingSelectionFn: cet.encodingSelector,
	}

	return func(t *vegeta.Target) (err error) {
		t.Method = "PUT"
		t.URL = cet.sinkUrl

		// Generate CloudEvent
		payload := map[string]string{"msg": generateRandString(cet.msgSize)}
		event := cloudevents.NewEvent()
		event.SetID(strconv.FormatUint(seqStr, 10))
		event.SetType(cet.eventType)
		event.SetSource(cet.eventSource)
		event.SetDataContentType("text/plain")
		if err := event.SetData(payload); err != nil {
			return err
		}

		m, err := codec.Encode(event)
		if err != nil {
			return err
		}

		t.Header = m.(cehttp.Message).Header
		t.Body = m.(cehttp.Message).Body

		return err
	}
}

// generateRandString returns a random string with the given length.
func generateRandString(length int) string {
	b := make([]rune, length)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}
