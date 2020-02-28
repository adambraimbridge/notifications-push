package resources

import (
	"bytes"
	"fmt"
	"net/http"
	"time"
)

type HTTPClient struct {
}

// GetStatusCode - sends GET request and returns status code
func (hc *HTTPClient) GetStatusCode(url string) (int, error) {

	r, err := http.NewRequest("GET", url, bytes.NewReader([]byte("")))
	if err != nil {
		return 0, fmt.Errorf("error creating request: %w", err)
	}
	client := &http.Client{Timeout: time.Second * 15}
	res, err := client.Do(r)
	if err != nil {
		return 0, fmt.Errorf("error making http request:%w", err)
	}
	defer res.Body.Close()

	return res.StatusCode, nil
}
