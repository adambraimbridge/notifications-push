package consumer

import (
	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/notifications-push/v4/dispatch"
)

type metadataQueueHandler struct {
	originWhitelist []string
	mapper          NotificationMapper
	dispatcher      dispatch.Dispatcher
}

func NewMetadataQueueHandler(originWhitelist []string, mapper NotificationMapper, dispatcher dispatch.Dispatcher) *metadataQueueHandler {
	return &metadataQueueHandler{
		originWhitelist: originWhitelist,
		mapper:          mapper,
		dispatcher:      dispatcher,
	}
}

func (h *metadataQueueHandler) HandleMessage(queueMsg kafka.FTMessage) error {
	msg := NotificationQueueMessage{queueMsg}
	tid := msg.TransactionID()
	logger := log.WithTransactionID(tid)

	event, err := msg.ToAnnotationEvent()
	if err != nil {
		logger.WithField("message_body", msg.Body).WithError(err).Warn("Skipping annotation event.")
		return err
	}

	if msg.HasSynthTransactionID() {
		logger.WithValidFlag(false).WithField("contentID", event.ContentID).Info("Skipping annotation event: Synthetic transaction ID.")
		return nil
	}

	if !h.IsAllowedOrigin(msg.OriginID()) {
		logger.WithValidFlag(false).Info("Skipping annotation event: origin system is not the whitelist.")
		return nil
	}

	notification := h.mapper.MapMetadataNotification(event, msg.TransactionID())
	logger.WithField("resource", notification.APIURL).Info("Valid annotation notification received")
	h.dispatcher.Send(notification)

	return nil
}

func (h *metadataQueueHandler) IsAllowedOrigin(origin string) bool {
	for _, o := range h.originWhitelist {
		if o == origin {
			return true
		}
	}
	return false
}
