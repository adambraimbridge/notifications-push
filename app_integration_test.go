// +build integration

package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"regexp"
	"testing"
	"time"

	log "github.com/Financial-Times/go-logger"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/kafka"
	"github.com/Financial-Times/notifications-push/v4/consumer"
	"github.com/Financial-Times/notifications-push/v4/dispatch"
	"github.com/Financial-Times/notifications-push/v4/resources"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

var articleMsg = kafka.NewFTMessage(map[string]string{
	"Message-Id":        "e9234cdf-0e45-4d87-8276-cbe018bafa60",
	"Message-Timestamp": "2019-10-02T15:13:26.329Z",
	"Message-Type":      "cms-content-published",
	"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
	"Content-Type":      "application/vnd.ft-upp-article+json",
	"X-Request-Id":      "test-publish-123",
}, `{ "payload": { "title": "Lebanon eases dollar flow for importers as crisis grows", "type": "Article", "standout": { "scoop": false } }, "contentUri": "http://methode-article-mapper.svc.ft.com/content/3cc23068-e501-11e9-9743-db5a370481bc", "lastModified": "2019-10-02T15:13:19.52Z" }`)

var syntheticMsg = kafka.NewFTMessage(map[string]string{
	"Message-Id":        "e9234cdf-0e45-4d87-8276-cbe018bafa60",
	"Message-Timestamp": "2019-10-02T15:13:26.329Z",
	"Message-Type":      "cms-content-published",
	"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
	"Content-Type":      "application/vnd.ft-upp-article+json",
	"X-Request-Id":      "SYNTH-123",
}, `{"payload":{"title":"Synthetic message","type":"Article","standout":{"scoop":false}},"contentUri":"synthetic/3cc23068-e501-11e9-9743-db5a370481bc","lastModified":"2019-10-02T15:13:19.52Z"}`)

var invalidContentTypeMsg = kafka.NewFTMessage(map[string]string{
	"Message-Id":        "e9234cdf-0e45-4d87-8276-cbe018bafa60",
	"Message-Timestamp": "2019-10-02T15:13:26.329Z",
	"Message-Type":      "cms-content-published",
	"Origin-System-Id":  "http://cmdb.ft.com/systems/methode-web-pub",
	"Content-Type":      "application/invalid-type",
	"X-Request-Id":      "test-publish-123",
}, `{"payload":{"title":"Invalid type message","type":"Article","standout":{"scoop":false}},"contentUri":"invalid type/3cc23068-e501-11e9-9743-db5a370481bc","lastModified":"2019-10-02T15:13:19.52Z"}`)

func TestPushNotifications(t *testing.T) {

	l := logger.NewUPPLogger("TEST", "PANIC")
	log.InitLogger("TEST", "PANIC")

	// handlers vars
	var (
		apiGatewayURL = "/api-gateway"
		heartbeat     = time.Second * 1
		resource      = "content"
	)
	// dispatch vars
	var (
		delay       = time.Millisecond * 200
		historySize = 50
	)
	// message consumer vars
	var (
		uriWhitelist             = `^http://(methode|wordpress-article|content)(-collection|-content-placeholder)?-(mapper|unfolder)(-pr|-iw)?(-uk-.*)?\.svc\.ft\.com(:\d{2,5})?/(content|complementarycontent)/[\w-]+.*$`
		typeWhitelist            = []string{"application/vnd.ft-upp-article+json", "application/vnd.ft-upp-content-package+json", "application/vnd.ft-upp-audio+json"}
		expectedNotificationBody = "data: [{\"apiUrl\":\"test-api/content/3cc23068-e501-11e9-9743-db5a370481bc\",\"id\":\"http://www.ft.com/thing/3cc23068-e501-11e9-9743-db5a370481bc\",\"type\":\"http://www.ft.com/thing/ThingChangeType/UPDATE\",\"title\":\"Lebanon eases dollar flow for importers as crisis grows\",\"standout\":{\"scoop\":false}}]\n\n\n"
		sendDelay                = time.Millisecond * 190
	)

	// dispatcher
	d := startDispatcher(delay, historySize)
	defer d.Stop()

	// consumer
	msgQueue := createMsgQueue(t, uriWhitelist, typeWhitelist, resource, "test-api", d)

	// server
	router := mux.NewRouter()
	server := httptest.NewServer(router)
	defer server.Close()

	// handler
	keyValidator := resources.NewKeyValidator(server.URL+apiGatewayURL, http.DefaultClient, l)
	s := resources.NewSubHandler(d, keyValidator, heartbeat, resource, l)
	s.RegisterHandlers(router)

	// key validation
	router.HandleFunc(apiGatewayURL, func(resp http.ResponseWriter, req *http.Request) {
		resp.WriteHeader(http.StatusOK)
	}).Methods("GET")

	// context that controls the live of all subscribers
	ctx, cancel := context.WithCancel(context.Background())

	testClientWithNONotifications(ctx, t, server.URL, heartbeat, "Audio")
	testClientWithNotifications(ctx, t, server.URL, "Article", expectedNotificationBody)
	testClientWithNotifications(ctx, t, server.URL, "All", expectedNotificationBody)

	// message producer
	go func() {
		msgs := []kafka.FTMessage{
			articleMsg,
			syntheticMsg,
			invalidContentTypeMsg,
		}
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(sendDelay):
				for _, msg := range msgs {
					err := msgQueue.HandleMessage(msg)
					if err != nil {
						assert.NoError(t, err)
						return
					}
				}
			}
		}
	}()

	<-time.After(heartbeat * 5)
	// shutdown test
	cancel()

}

// Tests a subscriber that expects only notifications
func testClientWithNotifications(ctx context.Context, t *testing.T, serverURL string, subType string, expectedBody string) {
	ch, err := startSubscriber(ctx, serverURL, subType)
	assert.NoError(t, err)

	go func() {

		body := <-ch
		assert.Equal(t, "data: []\n\n", body, "Client with type '%s' expects to receive heartbeat message when connecting to the service.", subType)

		for {
			select {
			case <-ctx.Done():
				return
			case body = <-ch:
				assert.Equal(t, expectedBody, body, "Client with type '%s' received incorrect body", subType)
			}
		}
	}()
}

// Tests a subscriber that expects only heartbeats
func testClientWithNONotifications(ctx context.Context, t *testing.T, serverURL string, heartbeat time.Duration, subType string) {

	ch, err := startSubscriber(ctx, serverURL, subType)
	assert.NoError(t, err)

	go func() {

		body := <-ch
		assert.Equal(t, "data: []\n\n", body, "Client with type '%s' expects to receive heartbeat message when connecting to the service.", subType)
		start := time.Now()

		for {
			select {
			case <-ctx.Done():
				return
			case body := <-ch:
				delta := time.Since(start)
				assert.InEpsilon(t, heartbeat.Nanoseconds(), delta.Nanoseconds(), 0.05, "No Notification Client with type '%s' expects to receive heatbests on time.")
				assert.Equal(t, "data: []\n\n", body, "No Notification Client with type '%s' expects to receive only heartbeat messages.")
				start = start.Add(heartbeat)
			}
		}
	}()
}

func startSubscriber(ctx context.Context, serverURL string, subType string) (<-chan string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, serverURL+"/content/notifications-push?type="+subType, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Api-Key", "test-key")

	resp, err := http.DefaultClient.Do(req) //nolint:bodyclose
	if err != nil {
		return nil, err
	}

	ch := make(chan string)
	go func() {

		defer close(ch)
		defer resp.Body.Close()

		buf := make([]byte, 4096)
		for {
			select {
			case <-ctx.Done():
				return
			default:
				idx, err := resp.Body.Read(buf)
				if err != nil {
					return
				}
				ch <- string(buf[:idx])
			}
		}
	}()

	return ch, nil
}

func startDispatcher(delay time.Duration, historySize int) dispatch.Dispatcher {
	h := dispatch.NewHistory(historySize)
	d := dispatch.NewDispatcher(delay, h)
	go d.Start()
	return d
}

func createMsgQueue(t *testing.T, uriWhitelist string, typeWhitelist []string, resource string, apiURL string, d dispatch.Dispatcher) consumer.MessageQueueHandler {
	set := consumer.NewSet()
	for _, value := range typeWhitelist {
		set.Add(value)
	}
	reg, err := regexp.Compile(uriWhitelist)
	assert.NoError(t, err)

	mapper := consumer.NotificationMapper{
		Resource:   resource,
		APIBaseURL: apiURL,
	}
	return consumer.NewMessageQueueHandler(reg, set, mapper, d)
}
