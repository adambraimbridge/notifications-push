package resources

import (
	"bytes"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/notifications-push/v4/mocks"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/assert"
)

func TestHealthcheck(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		httpClientMock    *mocks.StatusCodeClient
		kafkaConsumerMock *mocks.KafkaConsumer
		expectedStatus    int
		expectedBody      string
	}{
		"Success - both ok": {
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
			expectedStatus: 200,
			expectedBody:   `"ok":false,"severity":1}`,
		},
		"Fail because of ApiGateway does not return 200 OK": {
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
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			hc := NewHealthCheck(test.kafkaConsumerMock, "randomAddress", test.httpClientMock)

			req, err := http.NewRequest("GET", "/__health", nil)
			if err != nil {
				t.Fatal(err)
			}

			rr := httptest.NewRecorder()
			servicesRouter := mux.NewRouter()
			servicesRouter.HandleFunc("/__health", hc.Health()).Methods("GET")

			servicesRouter.ServeHTTP(rr, req)

			buf := new(bytes.Buffer)
			_, _ = buf.ReadFrom(rr.Body)
			body := buf.String()

			assert.Equal(t, test.expectedStatus, rr.Code, name+" failed")
			assert.Contains(t, body, test.expectedBody, name+" failed")
		})
	}
}
