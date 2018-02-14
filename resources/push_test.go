package resources

import (
	"bufio"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"errors"
	"io/ioutil"
	"strings"

	"github.com/Financial-Times/notifications-push/dispatcher"
	"github.com/Financial-Times/notifications-push/test/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

var start func(sub dispatcher.Subscriber)

func TestPushStandardSubscriber(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusOK,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)

	assert.Equal(t, "text/event-stream; charset=UTF-8", w.Header().Get("Content-Type"), "Should be SSE")
	assert.Equal(t, "no-cache, no-store, must-revalidate", w.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", w.Header().Get("Connection"))
	assert.Equal(t, "no-cache", w.Header().Get("Pragma"))
	assert.Equal(t, "0", w.Header().Get("Expires"))

	reader := bufio.NewReader(w.Body)
	body, _ := reader.ReadString(byte(0)) // read to EOF

	assert.Equal(t, "data: hi\n\n", body)

	assert.Equal(t, http.StatusOK, w.Code, "Should be OK")
	d.AssertExpectations(t)
}

func TestPushMonitorSubscriber(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.monitorSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.monitorSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push?monitor=true", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusOK,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)

	assert.Equal(t, "text/event-stream; charset=UTF-8", w.Header().Get("Content-Type"), "Should be SSE")
	assert.Equal(t, "no-cache, no-store, must-revalidate", w.Header().Get("Cache-Control"))
	assert.Equal(t, "keep-alive", w.Header().Get("Connection"))
	assert.Equal(t, "no-cache", w.Header().Get("Pragma"))
	assert.Equal(t, "0", w.Header().Get("Expires"))

	reader := bufio.NewReader(w.Body)
	body, _ := reader.ReadString(byte(0)) // read to EOF

	assert.Equal(t, "data: hi\n\n", body)

	assert.Equal(t, http.StatusOK, w.Code, "Should be OK")
	d.AssertExpectations(t)
}

func TestPushFailed(t *testing.T) {
	d := new(MockDispatcher)
	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusOK,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestPushInvalidType(t *testing.T) {
	d := new(MockDispatcher)
	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push?type=InvalidType", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusOK,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)

	reader := bufio.NewReader(w.Body)
	body, _ := reader.ReadString(byte(0)) // read to EOF

	assert.True(t, strings.Contains(body, "The specified type (InvalidType) is unsupported"))
}

func TestAPIGatewayDown(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusInternalServerError,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)
	assert.Equal(t, http.StatusServiceUnavailable, w.Code)
}

func TestInvalidApiKey(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-wrong-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		responseStatusCode: http.StatusUnauthorized,
	})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestEmptyApiKey(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{})
	Push(d, "http://dummy.ft.com", httpClient)(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestInvalidUrlForValidatingApiKey(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{})
	Push(d, ":invalidurl", httpClient)(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestClientErrorByRequestingValidatingApiKey(t *testing.T) {
	d := new(MockDispatcher)

	d.On("Register", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()
	d.On("Close", mock.AnythingOfType("*dispatcher.standardSubscriber")).Return()

	w := NewStreamResponseRecorder()
	req, err := http.NewRequest("GET", "/content/notifications-push", nil)
	if err != nil {
		t.Fatal(err)
	}

	req.Header.Set("X-Forwarded-For", "some-host, some-other-host-that-isnt-used")
	req.Header.Set(apiKeyHeaderField, "some-api-key")

	start = func(sub dispatcher.Subscriber) {
		sub.NotificationChannel() <- "hi"
		time.Sleep(10 * time.Millisecond)
		w.closer <- true

		assert.True(t, time.Now().After(sub.Since()))
		assert.Equal(t, "some-host", sub.Address())
	}

	httpClient := initializeMockHTTPClient(&mockTransport{
		shouldReturnError: true,
	})

	Push(d, "http://dummy.ft.com", httpClient)(w, req)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

type MockDispatcher struct {
	mocks.MockDispatcher
}

func (m *MockDispatcher) Register(sub dispatcher.Subscriber) {
	m.Called(sub)
	go start(sub)
}

func NewStreamResponseRecorder() *StreamResponseRecorder {
	w := httptest.NewRecorder()
	closer := make(chan bool)
	return &StreamResponseRecorder{w, closer}
}

type StreamResponseRecorder struct {
	*httptest.ResponseRecorder
	closer chan bool
}

func (r *StreamResponseRecorder) CloseNotify() <-chan bool {
	return r.closer
}

type MockWebClient struct{}
type mockTransport struct {
	responseStatusCode int
	responseBody       string
	shouldReturnError  bool
}

func initializeMockHTTPClient(tr *mockTransport) *http.Client {
	client := http.DefaultClient
	client.Transport = tr
	return client
}

func (t *mockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	response := &http.Response{
		Header:     make(http.Header),
		Request:    req,
		StatusCode: t.responseStatusCode,
	}

	response.Header.Set("Content-Type", "application/json")
	response.Body = ioutil.NopCloser(strings.NewReader(t.responseBody))

	if t.shouldReturnError {
		return nil, errors.New("Client error")
	}
	return response, nil
}
