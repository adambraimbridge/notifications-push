package consumer

import (
	"encoding/json"
	"regexp"
	"strings"

	"github.com/Financial-Times/kafka-client-go/kafka"
)

// NotificationQueueMessage is a wrapper for the queue consumer message type
type NotificationQueueMessage struct {
	kafka.FTMessage
}

// HasSynthTransactionID checks if the message is synthetic
func (msg NotificationQueueMessage) HasSynthTransactionID() bool {
	tid := msg.TransactionID()
	return strings.HasPrefix(tid, "SYNTH")
}

// HasCarouselTransactionID checks if the message is generated by the publish carousel
func (msg NotificationQueueMessage) HasCarouselTransactionID() bool {
	return carouselTransactionIDRegExp.MatchString(msg.TransactionID())
}

var carouselTransactionIDRegExp = regexp.MustCompile(`^.+_carousel_[\d]{10}.*$`) //`^(tid_[\S]+)_carousel_[\d]{10}.*$`

// TransactionID returns the message TID
func (msg NotificationQueueMessage) TransactionID() string {
	return msg.Headers["X-Request-Id"]
}

func (msg NotificationQueueMessage) OriginID() string {
	return msg.Headers["Origin-System-Id"]
}

// AsContent converts the message to a CmsPublicationEvent
func (msg NotificationQueueMessage) AsContent() (event ContentMessage, err error) {
	err = json.Unmarshal([]byte(msg.Body), &event)
	return event, err
}

// AsMetadata converts message to ConceptAnnotations Event
func (msg NotificationQueueMessage) AsMetadata() (AnnotationsMessage, error) {
	var event AnnotationsMessage
	err := json.Unmarshal([]byte(msg.Body), &event)
	return event, err
}

type AnnotationsMessage struct {
	ContentURI   string       `json:"contentUri"`
	LastModified string       `json:"lastModified"`
	Payload      *Annotations `json:"payload"`
}

type Annotations struct {
	ContentID   string `json:"uuid"`
	Annotations []struct {
		Thing struct {
			ID        string `json:"id"`
			Predicate string `json:"predicate"`
		} `json:"thing"`
	} `json:"annotations"`
}

// ContentMessage is the data structure that represents a publication event consumed from Kafka
type ContentMessage struct {
	ContentURI        string
	ContentTypeHeader string
	LastModified      string
	Payload           interface{}
}

// Matches is a method that returns True if the ContentURI of a publication event
// matches a whitelist regexp
func (e ContentMessage) Matches(whitelist *regexp.Regexp) bool {
	return whitelist.MatchString(e.ContentURI)
}

// HasEmptyPayload is a method that returns true if the ContentMessage has an empty payload
func (e ContentMessage) HasEmptyPayload() bool {
	switch v := e.Payload.(type) {
	case nil:
		return true
	case string:
		if len(v) == 0 {
			return true
		}
	case map[string]interface{}:
		if len(v) == 0 {
			return true
		}
	}
	return false
}
