package mocks

import (
	"context"
	"errors"
	"io/ioutil"
	"net/http"
	"strings"

	"github.com/Financial-Times/notifications-push/v4/dispatch"
	"github.com/stretchr/testify/mock"
)

type KeyValidator struct {
	mock.Mock
}

func (m *KeyValidator) Validate(ctx context.Context, key string) error {
	args := m.Called(ctx, key)

	if args.Get(0) != nil {
		return args.Get(0).(error)
	}
	return nil
}

// Dispatcher is a mock of a dispatcher that can be reused for testing
type Dispatcher struct {
	mock.Mock
}

// Start mocks Start
func (m *Dispatcher) Start() {
	m.Called()
}

// Stop mocks Stop
func (m *Dispatcher) Stop() {
	m.Called()
}

// Send mocks Send
func (m *Dispatcher) Send(notification dispatch.Notification) {
	m.Called(notification)
}

// Subscribers mocks Subscribers
func (m *Dispatcher) Subscribers() []dispatch.Subscriber {
	args := m.Called()
	return args.Get(0).([]dispatch.Subscriber)
}

func (m *Dispatcher) Subscribe(address string, subType string, monitoring bool) dispatch.Subscriber {
	args := m.Called(address, subType, monitoring)
	return args.Get(0).(dispatch.Subscriber)
}
func (m *Dispatcher) Unsubscribe(s dispatch.Subscriber) {
	m.Called(s)
}

type transport struct {
	ResponseStatusCode int
	ResponseBody       string
	Error              error
}

func ClientWithResponseCode(responseCode int) *http.Client {
	return &http.Client{
		Transport: &transport{
			ResponseStatusCode: responseCode,
		},
	}
}

func ClientWithError(err error) *http.Client {
	return &http.Client{
		Transport: &transport{
			Error: err,
		},
	}
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	response := &http.Response{
		Header:     make(http.Header),
		Request:    req,
		StatusCode: t.ResponseStatusCode,
	}

	response.Header.Set("Content-Type", "application/json")
	response.Body = ioutil.NopCloser(strings.NewReader(t.ResponseBody))

	if t.Error != nil {
		return nil, t.Error
	}
	return response, nil
}

type StatusCodeClient struct {
	GetStatusCodeF func(url string) (int, error)
}

func (c *StatusCodeClient) GetStatusCode(url string) (int, error) {
	if c.GetStatusCodeF != nil {
		return c.GetStatusCodeF(url)
	}
	return 0, errors.New("not implemented")
}

type KafkaConsumer struct {
	ConnectivityCheckF func() error
}

func (c *KafkaConsumer) ConnectivityCheck() error {
	if c.ConnectivityCheckF != nil {
		return c.ConnectivityCheckF()
	}
	return errors.New("not implemented")
}
