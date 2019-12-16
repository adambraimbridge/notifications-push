package consumer

import (
	"errors"
	"regexp"
	"time"

	"github.com/Financial-Times/notifications-push/v4/dispatch"
)

const (
	ContentUpdateType    = "http://www.ft.com/thing/ThingChangeType/UPDATE"
	ContentDeleteType    = "http://www.ft.com/thing/ThingChangeType/DELETE"
	AnnotationUpdateType = "http://www.ft.com/thing/ThingChangeType/ANNOTATIONS_UPDATE"
)

// NotificationMapper maps CmsPublicationEvents to Notifications
type NotificationMapper struct {
	APIBaseURL string
	Resource   string
}

// UUIDRegexp enables to check if a string matches a UUID
var UUIDRegexp = regexp.MustCompile("[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}")

// MapNotification maps the given event to a new notification.
func (n NotificationMapper) MapNotification(event PublicationEvent, transactionID string) (dispatch.Notification, error) {
	UUID := UUIDRegexp.FindString(event.ContentURI)
	if UUID == "" {
		return dispatch.Notification{}, errors.New("ContentURI does not contain a UUID")
	}

	var eventType string
	var scoop bool
	var title = ""
	var contentType = ""

	if event.HasEmptyPayload() {
		eventType = ContentDeleteType
		contentType = resolveTypeFromMessageHeader(event.ContentTypeHeader)
	} else {
		eventType = ContentUpdateType
		notificationPayloadMap, ok := event.Payload.(map[string]interface{})
		if ok {
			title = getValueFromPayload("title", notificationPayloadMap)
			contentType = getValueFromPayload("type", notificationPayloadMap)
			scoop = getScoopFromPayload(notificationPayloadMap)
		}
	}

	return dispatch.Notification{
		Type:             eventType,
		ID:               "http://www.ft.com/thing/" + UUID,
		APIURL:           n.APIBaseURL + "/" + n.Resource + "/" + UUID,
		PublishReference: transactionID,
		LastModified:     event.LastModified,
		Title:            title,
		Standout:         &dispatch.Standout{Scoop: scoop},
		ContentType:      contentType,
	}, nil
}

func (n NotificationMapper) MapMetadataNotification(event ConceptAnnotationsEvent, transactionID string) dispatch.Notification {
	return dispatch.Notification{
		Type:             AnnotationUpdateType,
		ID:               "http://www.ft.com/thing/" + event.ContentID,
		APIURL:           n.APIBaseURL + "/" + n.Resource + "/" + event.ContentID,
		PublishReference: transactionID,
		ContentType:      AnnotationContentType,
		LastModified:     time.Now().Format(time.RFC3339),
	}
}

func resolveTypeFromMessageHeader(contentTypeHeader string) string {
	switch contentTypeHeader {
	case "application/vnd.ft-upp-article+json":
		return "Article"
	case "application/vnd.ft-upp-content-package+json":
		return "ContentPackage"
	case "application/vnd.ft-upp-audio+json":
		return "Audio"
	default:
		return ""
	}
}

func getScoopFromPayload(notificationPayloadMap map[string]interface{}) bool {
	var standout = notificationPayloadMap["standout"]
	if standout != nil {
		standoutMap, ok := standout.(map[string]interface{})
		if ok && standoutMap["scoop"] != nil {
			return standoutMap["scoop"].(bool)
		}
	}

	return false
}

func getValueFromPayload(key string, payload map[string]interface{}) string {
	if payload[key] != nil {
		return payload[key].(string)
	}

	return ""
}
