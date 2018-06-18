package consumer

import (
	"regexp"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/notifications-push/dispatch"
)

// MessageQueueHandler is a generic interface for implementation of components to hendle messages form the kafka queue.
type MessageQueueHandler interface {
	HandleMessage(queueMsg kafka.FTMessage) error
}

type simpleMessageQueueHandler struct {
	whiteList  *regexp.Regexp
	mapper     NotificationMapper
	dispatcher dispatch.Dispatcher
}

// NewMessageQueueHandler returns a new message handler
func NewMessageQueueHandler(whitelist *regexp.Regexp, mapper NotificationMapper, dispatcher dispatch.Dispatcher) MessageQueueHandler {
	return &simpleMessageQueueHandler{
		whiteList:  whitelist,
		mapper:     mapper,
		dispatcher: dispatcher,
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

	if !pubEvent.Matches(qHandler.whiteList) {
		log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
		return nil
	}

	if pubEvent.IsDynamicContent() {
		log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: DynamicContent event.")
		return  nil
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
