package resources

import (
	"bytes"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/notifications-push/v4/mocks"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestHealthcheck(t *testing.T) {

	tests := map[string]struct {
		url               string
		httpClientMock    *mocks.StatusCodeClient
		kafkaConsumerMock *mocks.KafkaConsumer
		expectedStatus    int
		expectedBody      string
	}{
		"Success - both ok": {
			url: "/__health",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 200,
			expectedBody:   `"ok":true}`,
		},
		"Fail because of kafka": {
			url: "/__health",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return errors.New("Sample error")
				},
			},
			expectedStatus: 200,
			expectedBody:   `"ok":false,"severity":1}`,
		},
		"Fail because of ApiGateway does not return 200 OK": {
			url: "/__health",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 403, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 200,
			expectedBody:   `"ok":false,"severity":1}`,
		},
		"gtg endpoint success": {
			url: "/__gtg",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 200,
			expectedBody:   "OK",
		},
		"gtg endpoint kafka failure": {
			url: "/__gtg",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return errors.New("sample error")
				},
			},
			expectedStatus: 503,
			expectedBody:   "Error connecting to kafka queue",
		},
		"gtg endpoint ApiGateway failure": {
			url: "/__gtg",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 503, errors.New("gateway failed")
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 503,
			expectedBody:   "gateway failed",
		},
		"responds on build-info": {
			url: "/__build-info",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 200,
			expectedBody:   `{"version":`,
		},
		"responds on ping": {
			url: "/__ping",
			httpClientMock: &mocks.StatusCodeClient{
				GetStatusCodeF: func(url string) (int, error) {
					return 200, nil
				},
			},
			kafkaConsumerMock: &mocks.KafkaConsumer{
				ConnectivityCheckF: func() error {
					return nil
				},
			},
			expectedStatus: 200,
			expectedBody:   "pong",
		},
	}

	for name, test := range tests {
		fmt.Printf("Running test %s \n", name)

		hc := NewHealthCheck(test.kafkaConsumerMock, "randomAddress", test.httpClientMock)

		req, err := http.NewRequest("GET", test.url, nil)
		if err != nil {
			t.Fatal(err)
		}

		rr := httptest.NewRecorder()
		servicesRouter := mux.NewRouter()
		hc.RegisterHandlers(servicesRouter)

		servicesRouter.ServeHTTP(rr, req)

		buf := new(bytes.Buffer)
		buf.ReadFrom(rr.Body)
		body := buf.String()

		assert.Equal(t, test.expectedStatus, rr.Code, name+" failed")
		assert.Contains(t, body, test.expectedBody, name+" failed")
	}

}
