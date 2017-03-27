package consumer

import (
	"regexp"

	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"github.com/Financial-Times/notifications-push/dispatcher"
)

// MessageQueueHandler is a generic interface for implementation of components to hendle messages form the kafka queue.
type MessageQueueHandler interface {
	HandleMessage(queueMsg []queueConsumer.Message)
}

type simpleMessageQueueHandler struct {
	whiteList  *regexp.Regexp
	mapper     dispatcher.NotificationMapper
	dispatcher dispatcher.Dispatcher
}

// // NewMessageQueueHandler returns a new message handler
// func NewMessageQueueHandler(whitelist *regexp.Regexp, mapper dispatcher.NotificationMapper, dispatcher dispatcher.Dispatcher) MessageQueueHandler {
// 	return &simpleMessageQueueHandler{
// 		whiteList:  whitelist,
// 		mapper:     mapper,
// 		dispatcher: dispatcher,
// 	}
// }

// func (qHandler *simpleMessageQueueHandler) HandleMessage(msgs []queueConsumer.Message) {
// 	var batch []dispatcher.Notification
// 	for _, queueMsg := range msgs {
// 		msg := NotificationQueueMessage{queueMsg}
//
// 		pubEvent, err := msg.ToPublicationEvent()
// 		if err != nil {
// 			log.WithField("transaction_id", msg.TransactionID()).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
// 			continue
// 		}
//
// 		if msg.HasCarouselTransactionID() {
// 			log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Carousel publish event.")
// 			continue
// 		}
//
// 		if msg.HasSynthTransactionID() {
// 			log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
// 			continue
// 		}
//
// 		if !pubEvent.Matches(qHandler.whiteList) {
// 			log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: It is not in the whitelist.")
// 			continue
// 		}
//
// 		notification, err := qHandler.mapper.MapNotification(pubEvent, msg.TransactionID())
// 		if err != nil {
// 			log.WithField("transaction_id", msg.TransactionID()).WithField("msg", string(msg.Body)).WithError(err).Warn("Skipping event: Cannot build notification for message.")
// 			continue
// 		}
//
// 		batch = append(batch, notification)
// 		log.WithField("resource", notification.APIURL).WithField("transaction_id", notification.PublishReference).Info("Valid notification in message batch")
// 	}
// 	if len(batch) > 0 {
// 		qHandler.dispatcher.Send(batch...)
// 	}
// }
