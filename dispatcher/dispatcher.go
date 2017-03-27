package dispatcher

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/wvanbergen/kafka/consumergroup"

	kafka "github.com/Shopify/sarama"
)

const (
	heartbeatMsg  = "[]"
	rfc3339Millis = "2006-01-02T15:04:05.000Z07:00"
)

// Dispatcher forwards a new notification onto subscribers.
type Dispatcher interface {
	Start()
	Stop()
	Send(notification ...Notification)
	Subscribers() []Subscriber
	Registrar
}

// Registrar (aka Registrator :smirk:) is the interface for a component that
// manages subscriber registration
type Registrar interface {
	Register(subscriber Subscriber)
	Close(subscriber Subscriber)
}

// NewDispatcher creates and returns a new dispatcher
func NewDispatcher(delay time.Duration, heartbeatPeriod time.Duration, history History, consumer *consumergroup.ConsumerGroup, whiteListRegEx *regexp.Regexp) Dispatcher {
	return &dispatcher{
		delay:           delay,
		heartbeatPeriod: heartbeatPeriod,
		inbound:         make(chan Notification),
		subscribers:     map[Subscriber]struct{}{},
		lock:            &sync.RWMutex{},
		history:         history,
		stopChan:        make(chan bool),
		consumer:        consumer,
		whiteListRegEx:  whiteListRegEx,
	}
}

type dispatcher struct {
	delay           time.Duration
	heartbeatPeriod time.Duration
	inbound         chan Notification
	subscribers     map[Subscriber]struct{}
	lock            *sync.RWMutex
	history         History
	stopChan        chan bool
	consumer        *consumergroup.ConsumerGroup
	whiteListRegEx  *regexp.Regexp
}

func (d *dispatcher) Start() {
	log.Info("Dispatcher started")
	heartbeat := time.NewTimer(d.heartbeatPeriod)

	for {
		select {
		case msg := <-d.consumer.Messages():
			notification, err := d.toNotification(msg)
			if err == nil {
				d.forwardToSubscribers(notification)
			}
		case <-heartbeat.C:
			d.heartbeat()
		case <-d.stopChan:
			heartbeat.Stop()
			return
		}

		heartbeat.Reset(d.heartbeatPeriod)
	}
}

func (d *dispatcher) toNotification(msg *kafka.ConsumerMessage) (Notification, error) {
	fmt.Println(string(msg.Value))
	var pubEvent PublicationEvent
	err := json.Unmarshal(msg.Value, pubEvent)
	if err != nil {
		log.WithError(err).Error("Impossible to transform consumed message to pub event")
		return Notification{}, err
	}

	// if err != nil {
	// 	//log.WithField("transaction_id", msg.TransactionID()).WithField("msg", msg.Body).WithError(err).Warn("Skipping event.")
	// 	return Notification{}, err
	// }
	//
	// if pubEvent.HasCarouselTransactionID() {
	// 	log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Carousel publish event.")
	// 	return Notification{}, errors.New("Carousel publish event")
	// }
	//
	// if msg.HasSynthTransactionID() {
	// 	log.WithField("transaction_id", msg.TransactionID()).WithField("contentUri", pubEvent.ContentURI).Info("Skipping event: Synthetic transaction ID.")
	// 	return Notification{}, errors.New("Synthetic transaction ID.")
	// }

	if !d.whiteListRegEx.MatchString(pubEvent.ContentURI) {
		return Notification{}, errors.New("Not in whitelist")
	}

	return Notification{}, nil
}

func (d *dispatcher) forwardToSubscribers(notification Notification) {
	d.lock.RLock()
	defer d.lock.RUnlock()
	for sub := range d.subscribers {
		err := sub.send(notification)
		entry := log.WithField("transaction_id", notification.PublishReference).
			WithField("resource", notification.APIURL).
			WithField("subscriberAddress", sub.Address()).
			WithField("subscriberSince", sub.Since().Format(time.RFC3339))
		if err != nil {
			entry.WithError(err).Warn("Failed forwarding to subscriber.")
		} else {
			entry.Info("Forwarding to subscriber.")
		}

	}
	d.history.Push(notification)
}

func (d *dispatcher) heartbeat() {
	d.lock.RLock()
	defer d.lock.RUnlock()
	for sub := range d.subscribers {
		sub.writeOnMsgChannel(heartbeatMsg)
	}
}

func (d *dispatcher) Stop() {
	d.stopChan <- true
}

func (d *dispatcher) Send(notifications ...Notification) {
	log.WithField("batchSize", len(notifications)).Infof("Received notifications batch. Waiting configured delay (%v).", d.delay)
	go func() {
		d.delayForCache()
		for _, n := range notifications {
			n.NotificationDate = time.Now().Format(rfc3339Millis)
			d.inbound <- n
		}
	}()
}

func (d *dispatcher) delayForCache() {
	time.Sleep(d.delay)
}

func (d *dispatcher) Register(subscriber Subscriber) {
	d.lock.Lock()
	defer d.lock.Unlock()

	d.subscribers[subscriber] = struct{}{}
	log.WithField("subscriber", subscriber.Address()).WithField("subscriberType", reflect.TypeOf(subscriber).Elem().Name()).Info("Registered new subscriber")

	subscriber.writeOnMsgChannel(heartbeatMsg)
}

func (d *dispatcher) Subscribers() []Subscriber {
	d.lock.RLock()
	defer d.lock.RUnlock()

	var subs []Subscriber
	for sub := range d.subscribers {
		subs = append(subs, sub)
	}
	return subs
}

func (d *dispatcher) Close(subscriber Subscriber) {
	d.lock.Lock()
	defer d.lock.Unlock()

	delete(d.subscribers, subscriber)
	log.WithField("subscriber", subscriber.Address()).WithField("subscriberType", reflect.TypeOf(subscriber).Elem().Name()).Info("Unregistered subscriber")
}

type PublicationEvent struct {
	ContentURI   string
	UUID         string
	Payload      interface{}
	LastModified string
}
