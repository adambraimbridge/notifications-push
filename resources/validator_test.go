package resources

import (
	"net/http"
	"testing"

	"github.com/Financial-Times/notifications-push/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestIsValidApiKeySuccessful(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusOK)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.True(t, isValid)
	assert.Equal(t, "", errMsg)
	assert.Equal(t, 0, errStatusCode)
}

func TestIsValidApiKeyError(t *testing.T) {
	client := mocks.ErroringMockHTTPClient()
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Request to validate api key failed", errMsg)
	assert.Equal(t, http.StatusInternalServerError, errStatusCode)
}

func TestIsValidApiKeyEmptyKey(t *testing.T) {
	client := mocks.ErroringMockHTTPClient()
	isValid, errMsg, errStatusCode := isValidApiKey("", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Empty api key", errMsg)
	assert.Equal(t, http.StatusUnauthorized, errStatusCode)
}

func TestIsValidApiInvalidGatewayURL(t *testing.T) {
	client := mocks.ErroringMockHTTPClient()
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Invalid URL", errMsg)
	assert.Equal(t, http.StatusInternalServerError, errStatusCode)
}

func TestIsValidApiKeyResponseUnauthorized(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusUnauthorized)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Invalid api key", errMsg)
	assert.Equal(t, http.StatusUnauthorized, errStatusCode)
}

func TestIsValidApiKeyResponseTooManyRequests(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusTooManyRequests)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Rate limit exceeded", errMsg)
	assert.Equal(t, http.StatusTooManyRequests, errStatusCode)
}

func TestIsValidApiKeyResponseForbidden(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusForbidden)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Operation forbidden", errMsg)
	assert.Equal(t, http.StatusForbidden, errStatusCode)
}

func TestIsValidApiKeyResponseInternalServerError(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusInternalServerError)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Request to validate api key returned an unexpected response", errMsg)
	assert.Equal(t, http.StatusInternalServerError, errStatusCode)
}

func TestIsValidApiKeyResponseOtherServerError(t *testing.T) {
	client := mocks.MockHTTPClientWithResponseCode(http.StatusGatewayTimeout)
	isValid, errMsg, errStatusCode := isValidApiKey("testKey", "http://api.gateway.url", client)

	assert.False(t, isValid)
	assert.Equal(t, "Request to validate api key returned an unexpected response", errMsg)
	assert.Equal(t, http.StatusGatewayTimeout, errStatusCode)
}
