package main

import (
	"context"
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net/http"
	"regexp"
	"time"

	logV1 "github.com/Financial-Times/go-logger"
	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/kafka"
	queueConsumer "github.com/Financial-Times/notifications-push/v4/consumer"
	"github.com/Financial-Times/notifications-push/v4/dispatch"
	"github.com/Financial-Times/notifications-push/v4/resources"
	"github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

type notificationSystem interface {
	Start()
	Stop()
}

func startService(srv *http.Server, n notificationSystem, consumer kafka.Consumer, msgHandler queueConsumer.MessageQueueHandler, log *logger.UPPLogger) func(time.Duration) {

	go n.Start()

	consumer.StartListening(msgHandler.HandleMessage)

	go func() {
		err := srv.ListenAndServe()
		if err != http.ErrServerClosed {
			log.WithError(err).Error("http server")
		}
	}()

	return func(timeout time.Duration) {
		log.Info("Termination started. Quitting message consumer and notification dispatcher function.")
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		_ = srv.Shutdown(ctx)
		consumer.Shutdown()
		n.Stop()
	}
}
func initLogger(serviceName string, logLevel string) *logger.UPPLogger {
	// while transitioning to logger V2 we need to initialize the global V1 logger too
	logV1.InitLogger(serviceName, logLevel)

	return logger.NewUPPLogger(serviceName, logLevel)
}
func initRouter(r *mux.Router,
	s *resources.SubHandler,
	resource string,
	d *dispatch.Dispatcher,
	h dispatch.History,
	hc *resources.HealthCheck,
	log *logger.UPPLogger) {

	r.HandleFunc("/"+resource+"/notifications-push", s.HandleSubscription).Methods("GET")

	r.HandleFunc("/__health", hc.Health())
	r.HandleFunc(httphandlers.GTGPath, httphandlers.NewGoodToGoHandler(hc.GTG))
	r.HandleFunc(httphandlers.BuildInfoPath, httphandlers.BuildInfoHandler)
	r.HandleFunc(httphandlers.PingPath, httphandlers.PingHandler)

	r.HandleFunc("/__stats", resources.Stats(d, log)).Methods("GET")
	r.HandleFunc("/__history", resources.History(h, log)).Methods("GET")

}

func createSupervisedConsumer(log *logger.UPPLogger, address string, groupID string, topics []string) (kafka.Consumer, error) {
	errCh := make(chan error, 2)
	defer close(errCh)
	var fatalErrs = []error{kazoo.ErrPartitionNotClaimed, zk.ErrNoServer}
	fatalErrHandler := func(err error, serviceName string) {
		log.WithError(err).Fatalf("Exiting %s due to fatal error", serviceName)
	}

	supervisor := newServiceSupervisor(serviceName, errCh, fatalErrs, fatalErrHandler)
	go supervisor.Supervise()

	consumerConfig := kafka.DefaultConsumerConfig()
	consumerConfig.Zookeeper.Logger = stdlog.New(ioutil.Discard, "", 0)
	return kafka.NewConsumer(kafka.Config{
		ZookeeperConnectionString: address,
		ConsumerGroup:             groupID,
		Topics:                    topics,
		ConsumerGroupConfig:       consumerConfig,
		Err:                       errCh,
	})
}

func createDispatcher(cacheDelay int, historySize int) (*dispatch.Dispatcher, dispatch.History) {
	history := dispatch.NewHistory(historySize)
	dispatcher := dispatch.NewDispatcher(time.Duration(cacheDelay)*time.Second, history)
	return dispatcher, history
}

type msgHandlerCfg struct {
	Resource        string
	BaseURL         string
	ContentURI      string
	ContentTypes    []string
	MetadataHeaders []string
}

func createMessageHandler(config msgHandlerCfg, dispatcher *dispatch.Dispatcher, log *logger.UPPLogger) (*queueConsumer.MessageQueueRouter, error) {
	mapper := queueConsumer.NotificationMapper{
		Resource:   config.Resource,
		APIBaseURL: config.BaseURL,
		Property:   &conceptTimeReader{},
	}
	whitelistR, err := regexp.Compile(config.ContentURI)
	if err != nil {
		return nil, fmt.Errorf("content whitelist regex MUST compile: %w", err)
	}
	ctWhitelist := queueConsumer.NewSet()
	for _, value := range config.ContentTypes {
		ctWhitelist.Add(value)
	}
	contentHandler := queueConsumer.NewContentQueueHandler(whitelistR, ctWhitelist, mapper, dispatcher, log)
	metadataHandler := queueConsumer.NewMetadataQueueHandler(config.MetadataHeaders, mapper, dispatcher, log)
	handler := queueConsumer.NewMessageQueueHandler(contentHandler, metadataHandler)
	return handler, nil
}

type conceptTimeReader struct{}

func (c *conceptTimeReader) LastModified(event queueConsumer.ConceptAnnotationsEvent) string {
	// Currently PostConceptAnnotations event is missing LastModified property for annotations.
	// So we use current time as a substitute.
	return time.Now().Format(time.RFC3339)
}
