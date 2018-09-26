package resources

import (
	"net/http"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/notifications-push/dispatch"
)

// History returns history data
func History(history dispatch.History) func(w http.ResponseWriter, r *http.Request) {
	errMsg := "Serving /__history request"
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "application/json; charset=UTF-8")

		historyJSON, err := dispatch.MarshalNotificationsJSON(history.Notifications())
		if err != nil {
			log.WithField("event", "NotificationsPus").WithField("monitoring_event", "true").WithError(err).Warn(errMsg)
			http.Error(w, "", http.StatusInternalServerError)
			return
		}

		_, err = w.Write(historyJSON)
		if err != nil {
			log.WithField("event", "NotificationsPush").WithField("monitoring_event", "true").WithError(err).Warn(errMsg)
			http.Error(w, "", http.StatusInternalServerError)
		}
	}
}
