package resources

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/Financial-Times/notifications-push/v4/dispatch"
)

func TestHistory(t *testing.T) {
	history := dispatch.NewHistory(1)

	req, err := http.NewRequest("GET", "/__history", nil)
	if err != nil {
		t.Fatal(err)
	}

	w := httptest.NewRecorder()
	History(history)(w, req)

	assert.Equal(t, "application/json; charset=UTF-8", w.Header().Get("Content-Type"), "Should be json")
	assert.Equal(t, 200, w.Code, "Should be OK")
}
