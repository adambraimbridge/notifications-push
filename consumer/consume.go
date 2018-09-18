package consumer

import (
	"regexp"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/notifications-push/dispatch"
			)

var exists = struct{}{}

type Set struct {
	m map[string]struct{}
}

func NewSet() *Set {
	s := &Set{}
	s.m = make(map[string]struct{})
	return s
}

func (s *Set) Add(value string) {
	s.m[value] = exists
}

func (s *Set) Contains(value string) bool {
	_, c := s.m[value]
	return c
}

// MessageQueueHandler is a generic interface for implementation of components to hendle messages form the kafka queue.
type MessageQueueHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type simpleMessageQueueHandler struct {
	contentUriWhitelist  *regexp.Regexp
	contentTypeWhitelist *Set
	mapper               NotificationMapper
	dispatcher           dispatch.Dispatcher
}

// NewMessageQueueHandler returns a new message handler
func NewMessageQueueHandler(contentUriWhitelist *regexp.Regexp, contentTypeWhitelist *Set, mapper NotificationMapper, dispatcher dispatch.Dispatcher) MessageQueueHandler {
	return &simpleMessageQueueHandler{
		contentUriWhitelist:  contentUriWhitelist,
		contentTypeWhitelist: contentTypeWhitelist,
		mapper:               mapper,
		dispatcher:           dispatcher,
	}
}

func (qHandler *simpleMessageQueueHandler) HandleMessage(queueMsg kafka.FTMessage) error {
	msg := NotificationQueueMessage{queueMsg}

	pubEvent, err := msg.ToPublicationEvent()
	if err != nil {
		log.WithField("transaction_id", msg.TransactionID()).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
		return err
	}

	if msg.HasCarouselTransactionID() {
		log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Carousel publish event.")
		return nil
	}

	if msg.HasSynthTransactionID() {
		log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
		return nil
	}

	log.WithField("Whitelist", qHandler.contentTypeWhitelist).Info("Content-Type whitelist")
	contentType := msg.Headers["Content-Type"]
	if contentType == "application/json" || contentType == "" {
		if !pubEvent.Matches(qHandler.contentUriWhitelist) {
			log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).WithField("contentType", contentType).Info("Skipping event: contentUri is not in the whitelist.")
			return nil
		}
	} else {
		if !qHandler.contentTypeWhitelist.Contains(contentType) {
			log.WithField("transaction_id", msg.TransactionID()).WithField("contentType", contentType).Info("Skipping event: contentType is not the whitelist.")
			return nil
		}
	}

	notification, err := qHandler.mapper.MapNotification(pubEvent, msg.TransactionID())
	if err != nil {
		log.WithField("transaction_id", msg.TransactionID()).WithField("msg", string(msg.Body)).WithError(err).Warn("Skipping event: Cannot build notification for message.")
		return err
	}

	log.WithField("resource", notification.APIURL).WithField("transaction_id", notification.PublishReference).Info("Valid notification received")
	qHandler.dispatcher.Send(notification)

	return nil
}
