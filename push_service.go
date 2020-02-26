package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/kafka-client-go/kafka"
	cons "github.com/Financial-Times/notifications-push/v4/consumer"
)

type pushService struct {
	notifications notificationSystem
	consumer      kafka.Consumer
}

type notificationSystem interface {
	Start()
	Stop()
}

func newPushService(n notificationSystem, consumer kafka.Consumer) *pushService {
	return &pushService{
		notifications: n,
		consumer:      consumer,
	}
}

func (p *pushService) start(queueHandler cons.MessageQueueHandler) {
	go p.notifications.Start()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		log.Println("Started consuming.")
		p.consumer.StartListening(queueHandler.HandleMessage)
		log.Println("Finished consuming.")
		wg.Done()
	}()

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Termination signal received. Quitting message consumer and notification dispatcher function.")
	p.consumer.Shutdown()
	p.notifications.Stop()
	wg.Wait()
}
