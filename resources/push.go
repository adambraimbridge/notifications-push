package resources

import (
	"bufio"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	log "github.com/Financial-Times/go-logger"

	"github.com/Financial-Times/notifications-push/v4/dispatch"
)

const (
	apiKeyHeaderField       = "X-Api-Key"
	apiKeyQueryParam        = "apiKey"
	defaultSubscriptionType = dispatch.ArticleContentType
)

var supportedSubscriptionTypes = []string{
	dispatch.AnnotationsType,
	dispatch.ArticleContentType,
	dispatch.ContentPackageType,
	dispatch.AudioContentType,
	dispatch.AllContentType,
}

//ApiKey is provided either as a request param or as a header.
func getApiKey(r *http.Request) string {
	apiKey := r.Header.Get(apiKeyHeaderField)
	if apiKey != "" {
		return apiKey
	}

	return r.URL.Query().Get(apiKeyQueryParam)
}

// Push handler for push subscribers
func Push(reg dispatch.Registrar, apiGatewayKeyValidationURL string, httpClient *http.Client) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-type", "text/event-stream; charset=UTF-8")
		w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Pragma", "no-cache")
		w.Header().Set("Expires", "0")

		apiKey := getApiKey(r)
		if isValid, errMsg, errStatusCode := isValidApiKey(apiKey, apiGatewayKeyValidationURL, httpClient); !isValid {
			http.Error(w, errMsg, errStatusCode)
			return
		}

		cn, ok := w.(http.CloseNotifier)
		if !ok {
			http.Error(w, "Cannot stream.", http.StatusInternalServerError)
			return
		}

		bw := bufio.NewWriter(w)

		subscriptionParam, err := resolveSubType(r)
		if err != nil {
			log.WithError(err).Error("Invalid content type")
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		monitorParam := r.URL.Query().Get("monitor")
		isMonitor, _ := strconv.ParseBool(monitorParam)

		var s dispatch.Subscriber

		if isMonitor {
			s = dispatch.NewMonitorSubscriber(getClientAddr(r), subscriptionParam)
		} else {
			s = dispatch.NewStandardSubscriber(getClientAddr(r), subscriptionParam)
		}

		reg.Register(s)
		defer reg.Close(s)

		for {
			select {
			case notification := <-s.NotificationChannel():
				_, err := bw.WriteString("data: " + notification + "\n\n")
				if err != nil {
					logNotificationError(s, notification, err)
					return
				}

				err = bw.Flush()
				if err != nil {
					logNotificationError(s, notification, err)
					return
				}

				flusher := w.(http.Flusher)
				flusher.Flush()

				logSuccessForHeartbeatMessage(notification, s)

			case <-cn.CloseNotify():
				log.WithField("subscriberId", s.Id()).
					WithField("subscriber", s.Address()).
					Info("Notification subscriber disconnected remotely")
				return
			}
		}
	}
}

func logSuccessForHeartbeatMessage(notification string, s dispatch.Subscriber) {
	if notification == dispatch.HeartbeatMsg {
		log.WithField("subscriberId", s.Id()).
			WithField("subscriber", s.Address()).
			Info("Heartbeat sent to subscriber successfully")
	}
}

func logNotificationError(s dispatch.Subscriber, notification string, err error) {
	withError := log.WithField("subscriberId", s.Id()).
		WithField("subscriber", s.Address()).
		WithError(err)

	if notification == dispatch.HeartbeatMsg {
		withError.Error("Sending heartbeat to subscriber has failed ")
		return
	}

	withError.Error("Error while sending notification to subscriber")
}

func getClientAddr(r *http.Request) string {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		addr := strings.Split(xForwardedFor, ",")
		return addr[0]
	}
	return r.RemoteAddr
}

func resolveSubType(r *http.Request) (string, error) {
	subType := r.URL.Query().Get("type")
	if subType == "" {
		return defaultSubscriptionType, nil
	}
	for _, t := range supportedSubscriptionTypes {
		if strings.ToLower(subType) == strings.ToLower(t) {
			return subType, nil
		}
	}
	return "", fmt.Errorf("The specified type (%s) is unsupported", subType)
}
