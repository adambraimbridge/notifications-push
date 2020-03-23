package consumer

import (
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/kafka"
)

type MetadataQueueHandler struct {
	originWhitelist []string
	mapper          NotificationMapper
	dispatcher      notificationDispatcher
	log             *logger.UPPLogger
}

func NewMetadataQueueHandler(originWhitelist []string, mapper NotificationMapper, dispatcher notificationDispatcher, log *logger.UPPLogger) *MetadataQueueHandler {
	return &MetadataQueueHandler{
		originWhitelist: originWhitelist,
		mapper:          mapper,
		dispatcher:      dispatcher,
		log:             log,
	}
}

func (h *MetadataQueueHandler) HandleMessage(queueMsg kafka.FTMessage) error {
	msg := NotificationQueueMessage{queueMsg}
	tid := msg.TransactionID()
	entry := h.log.WithTransactionID(tid)

	event, err := msg.ToAnnotationEvent()
	if err != nil {
		entry.WithField("message_body", msg.Body).WithError(err).Warn("Skipping annotation event.")
		return err
	}

	if msg.HasSynthTransactionID() {
		entry.WithValidFlag(false).WithField("contentID", event.ContentID).Info("Skipping annotation event: Synthetic transaction ID.")
		return nil
	}

	if !h.IsAllowedOrigin(msg.OriginID()) {
		entry.WithValidFlag(false).Info("Skipping annotation event: origin system is not the whitelist.")
		return nil
	}

	notification := h.mapper.MapMetadataNotification(event, msg.TransactionID())
	entry.WithField("resource", notification.APIURL).Info("Valid annotation notification received")
	h.dispatcher.Send(notification)

	return nil
}

func (h *MetadataQueueHandler) IsAllowedOrigin(origin string) bool {
	for _, o := range h.originWhitelist {
		if o == origin {
			return true
		}
	}
	return false
}
