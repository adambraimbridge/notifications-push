package resources

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/Financial-Times/notifications-push/dispatch"
	"github.com/Financial-Times/notifications-push/test/mocks"
	"github.com/stretchr/testify/assert"
)

func TestStats(t *testing.T) {
	d := new(mocks.MockDispatcher)
	d.On("Subscribers").Return([]dispatch.Subscriber{})

	w := httptest.NewRecorder()
	req, err := http.NewRequest("GET", "/stats", nil)
	if err != nil {
		t.Fatal(err)
	}

	Stats(d)(w, req)

	assert.Equal(t, "application/json", w.Header().Get("Content-Type"), "Should be json")
	assert.Equal(t, `{"nrOfSubscribers":0,"subscribers":[]}`, w.Body.String(), "Should be empty array")
	assert.Equal(t, 200, w.Code, "Should be OK")

	d.AssertExpectations(t)
}
