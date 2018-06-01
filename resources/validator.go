package resources

import (
	"encoding/json"
	log "github.com/Financial-Times/go-logger"
	"io"
	"io/ioutil"
	"net/http"
)

func isValidApiKey(providedApiKey string, apiGatewayKeyValidationURL string, httpClient *http.Client) (bool, string, int) {
	if providedApiKey == "" {
		return false, "Empty api key", http.StatusUnauthorized
	}

	req, err := http.NewRequest("GET", apiGatewayKeyValidationURL, nil)
	if err != nil {
		log.WithField("url", apiGatewayKeyValidationURL).WithError(err).Error("Invalid URL for api key validation")
		return false, "Invalid URL", http.StatusInternalServerError
	}

	req.Header.Set(apiKeyHeaderField, providedApiKey)

	//if the api key has more than five characters we want to log the last five
	keySuffix := ""
	if len(providedApiKey) > 5 {
		keySuffix = providedApiKey[len(providedApiKey)-5:]
	}
	log.WithField("url", req.URL.String()).WithField("apiKeyLastChars", keySuffix).Info("Calling the API Gateway to validate api key")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.WithField("url", req.URL.String()).WithField("apiKeyLastChars", keySuffix).WithError(err).Error("Cannot send request to the API Gateway")
		return false, "Request to validate api key failed", http.StatusInternalServerError
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	respStatusCode := resp.StatusCode
	if respStatusCode == http.StatusOK {
		return true, "", 0
	}

	responseBody := getResponseBody(resp, keySuffix)

	if respStatusCode == http.StatusUnauthorized {
		log.WithField("apiKeyLastChars", keySuffix).Errorf("Invalid api key: %v", responseBody)
		return false, "Invalid api key", http.StatusUnauthorized
	}

	if respStatusCode == http.StatusTooManyRequests {
		log.WithField("apiKeyLastChars", keySuffix).Errorf("API key rate limit exceeded: %v", responseBody)
		return false, "Rate limit exceeded", http.StatusTooManyRequests
	}

	if respStatusCode == http.StatusForbidden {
		log.WithField("apiKeyLastChars", keySuffix).Errorf("Operation forbidden: %v", responseBody)
		return false, "Operation forbidden", http.StatusForbidden
	}

	log.WithField("url", req.URL.String()).WithField("apiKeyLastChars", keySuffix).Errorf("Received unexpected status code from the API Gateway: %d, error message: %v", respStatusCode, responseBody)
	return false, "Request to validate api key returned an unexpected response", respStatusCode
}

func getResponseBody(resp *http.Response, keySuffix string) string {
	type ApiMessage struct {
		Error string `json:"error"`
	}
	msg := ApiMessage{}
	responseBodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.WithField("apiKeyLastChars", keySuffix).Warnf("Getting API Gateway response body failed: %v", err)
		return ""
	}

	err = json.Unmarshal(responseBodyBytes, &msg)
	if err != nil {
		log.WithField("apiKeyLastChars", keySuffix).Warnf("Decoding API Gateway response body as json failed: %v", err)
		return string(responseBodyBytes[:])
	}
	return msg.Error
}
