package dispatch

import (
	"reflect"
	"strings"
	"sync"
	"time"

	log "github.com/Financial-Times/go-logger"
)

const (
	RFC3339Millis = "2006-01-02T15:04:05.000Z07:00"
)

// NewDispatcher creates and returns a new Dispatcher
// Delay argument configures minimum delay between send notifications
// History is a system that collects a list of all notifications send by Dispatcher
func NewDispatcher(delay time.Duration, history History) *Dispatcher {
	return &Dispatcher{
		delay:       delay,
		inbound:     make(chan Notification),
		subscribers: map[NotificationConsumer]struct{}{},
		lock:        &sync.RWMutex{},
		history:     history,
		stopChan:    make(chan bool),
	}
}

type Dispatcher struct {
	delay       time.Duration
	inbound     chan Notification
	subscribers map[NotificationConsumer]struct{}
	lock        *sync.RWMutex
	history     History
	stopChan    chan bool
}

func (d *Dispatcher) Start() {

	for {
		select {
		case notification := <-d.inbound:
			d.forwardToSubscribers(notification)
			d.history.Push(notification)
		case <-d.stopChan:
			return
		}
	}
}

func (d *Dispatcher) Stop() {
	d.stopChan <- true
}

func (d *Dispatcher) Send(n Notification) {
	log.WithTransactionID(n.PublishReference).Infof("Received notification. Waiting configured delay (%v).", d.delay)
	go func() {
		time.Sleep(d.delay)
		n.NotificationDate = time.Now().Format(RFC3339Millis)
		d.inbound <- n
	}()
}

func (d *Dispatcher) Subscribers() []Subscriber {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var subs []Subscriber
	for sub := range d.subscribers {
		subs = append(subs, sub)
	}
	return subs
}

func (d *Dispatcher) Subscribe(address string, subType string, monitoring bool) Subscriber {
	var s NotificationConsumer
	if monitoring {
		s = NewMonitorSubscriber(address, subType)
	} else {
		s = NewStandardSubscriber(address, subType)
	}
	d.addSubscriber(s)
	return s
}

func (d *Dispatcher) Unsubscribe(subscriber Subscriber) {
	d.lock.Lock()
	defer d.lock.Unlock()

	s := subscriber.(NotificationConsumer)

	delete(d.subscribers, s)
	log.WithField("subscriberId", subscriber.ID()).
		WithField("subscriber", subscriber.Address()).
		WithField("subscriberType", reflect.TypeOf(subscriber).Elem().Name()).
		Info("Unregistered subscriber")
}

func (d *Dispatcher) forwardToSubscribers(notification Notification) {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var sent, failed, skipped int
	defer func() {
		if len(d.subscribers) == 0 || sent > 0 || len(d.subscribers) == skipped {
			log.WithMonitoringEvent("NotificationsPush", notification.PublishReference, notification.SubscriptionType).
				WithFields(map[string]interface{}{"resource": notification.APIURL, "sent": sent, "failed": failed, "skipped": skipped}).
				Info("Processed subscribers.")
		} else {
			log.WithFields(map[string]interface{}{"transaction_id": notification.PublishReference, "resource": notification.APIURL, "sent": sent, "failed": failed, "skipped": skipped}).
				Error("Processed subscribers. Failed to send notifications")
		}
	}()

	for sub := range d.subscribers {
		entry := log.WithField("transaction_id", notification.PublishReference).
			WithField("resource", notification.APIURL).
			WithField("subscriberId", sub.ID()).
			WithField("subscriberAddress", sub.Address()).
			WithField("subscriberSince", sub.Since().Format(time.RFC3339))

		if !matchesSubType(notification, sub) {
			skipped++
			entry.Info("Skipping subscriber.")
			continue
		}

		err := sub.Send(notification)
		if err != nil {
			failed++
			entry.WithError(err).Warn("Failed forwarding to subscriber.")
		} else {
			sent++
			entry.Info("Forwarding to subscriber.")
		}
	}
}

func (d *Dispatcher) addSubscriber(s NotificationConsumer) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.subscribers[s] = struct{}{}

	log.WithField("subscriberId", s.ID()).
		WithField("subscriber", s.Address()).
		WithField("subscriberType", reflect.TypeOf(s).Elem().Name()).
		WithField("acceptedContentType", s.SubType()).
		Info("Registered new subscriber")

}

func matchesSubType(n Notification, s Subscriber) bool {

	subType := strings.ToLower(s.SubType())
	notifType := strings.ToLower(n.SubscriptionType)

	all := strings.ToLower(AllContentType)
	ann := strings.ToLower(AnnotationsType)

	if subType == all && notifType != ann {
		return true
	}

	if n.Type == ContentDeleteType &&
		notifType == "" &&
		subType != ann {
		return true
	}

	return subType == notifType
}
