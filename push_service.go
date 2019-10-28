package main

import (
	"os"
	"os/signal"
	"sync"
	"syscall"

	log "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/kafka-client-go/kafka"

	cons "github.com/Financial-Times/notifications-push/v4/consumer"
	"github.com/Financial-Times/notifications-push/v4/dispatch"
)

type pushService struct {
	dispatcher dispatch.Dispatcher
	consumer   kafka.Consumer
}

func newPushService(d dispatch.Dispatcher, consumer kafka.Consumer) *pushService {
	return &pushService{
		dispatcher: d,
		consumer:   consumer,
	}
}

func (p *pushService) start(queueHandler cons.MessageQueueHandler) {
	go p.dispatcher.Start()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		log.Println("Started consuming.")
		p.consumer.StartListening(queueHandler.HandleMessage)
		log.Println("Finished consuming.")
		wg.Done()
	}()

	ch := make(chan os.Signal)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	log.Info("Termination signal received. Quitting message consumer and notification dispatcher function.")
	p.consumer.Shutdown()
	p.dispatcher.Stop()
	wg.Wait()
}
