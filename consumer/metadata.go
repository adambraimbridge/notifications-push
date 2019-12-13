package consumer

import "github.com/Financial-Times/kafka-client-go/kafka"

type metadataQueueHandler struct{}

func NewMetadataQueueHandler() *metadataQueueHandler {
	return &metadataQueueHandler{}
}

func (h *metadataQueueHandler) HandleMessage(queueMsg kafka.FTMessage) error {
	return nil
}
