package resources

import (
	"encoding/json"
	"net/http"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/notifications-push/v5/dispatch"
)

type subscriptionStats struct {
	NrOfSubscribers int                   `json:"nrOfSubscribers"`
	Subscribers     []dispatch.Subscriber `json:"subscribers"`
}

type clientsProvider interface {
	Subscribers() []dispatch.Subscriber
}

// Stats returns subscriber stats
func Stats(provider clientsProvider, log *logger.UPPLogger) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		subscribers := provider.Subscribers()

		stats := subscriptionStats{
			NrOfSubscribers: len(subscribers),
			Subscribers:     subscribers,
		}

		bytes, err := json.Marshal(stats)
		if err != nil {
			log.WithError(err).Warn("Error in marshalling stats information", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-type", "application/json")
		b, err := w.Write(bytes)
		if b == 0 {
			log.Warn("Response written to HTTP was empty.")
		}

		if err != nil {
			log.Warnf("Error writing stats to HTTP response: %v", err.Error())
		}
	}
}
