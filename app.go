package main

import (
	"fmt"
	"io/ioutil"
	stdlog "log"
	"net"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"time"

	log "github.com/Financial-Times/go-logger"
	logger "github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/kafka-client-go/kafka"
	queueConsumer "github.com/Financial-Times/notifications-push/v4/consumer"
	"github.com/Financial-Times/notifications-push/v4/dispatch"
	"github.com/Financial-Times/notifications-push/v4/resources"
	"github.com/Financial-Times/service-status-go/httphandlers"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
	"github.com/samuel/go-zookeeper/zk"
	kazoo "github.com/wvanbergen/kazoo-go"
)

const (
	heartbeatPeriod = 30 * time.Second
	serviceName     = "notifications-push"
	appDescription  = "Proactively notifies subscribers about new publishes/modifications."
)

func main() {
	app := cli.App(serviceName, appDescription)
	resource := app.String(cli.StringOpt{
		Name:   "notifications_resource",
		Value:  "",
		Desc:   "The resource of which notifications are produced (e.g., content or lists)",
		EnvVar: "NOTIFICATIONS_RESOURCE",
	})

	consumerAddrs := app.String(cli.StringOpt{
		Name:   "consumer_addr",
		Value:  "",
		Desc:   "Comma separated kafka hosts for message consuming.",
		EnvVar: "KAFKA_ADDRS",
	})

	consumerGroupID := app.String(cli.StringOpt{
		Name:   "consumer_group_id",
		Value:  "",
		Desc:   "Kafka qroup id used for message consuming.",
		EnvVar: "GROUP_ID",
	})
	apiBaseURL := app.String(cli.StringOpt{
		Name:   "api_base_url",
		Value:  "http://api.ft.com",
		Desc:   "The API base URL where resources are accessible",
		EnvVar: "API_BASE_URL",
	})
	apiKeyValidationEndpoint := app.String(cli.StringOpt{
		Name:   "api_key_validation_endpoint",
		Value:  "t800/a",
		Desc:   "The API Gateway ApiKey validation endpoint",
		EnvVar: "API_KEY_VALIDATION_ENDPOINT",
	})
	apiGatewayHealthcheckEndpoint := app.String(cli.StringOpt{
		Name:   "api_healthcheck_endpoint",
		Value:  "/t800-healthcheck",
		Desc:   "The API Gateway healthcheck endpoint",
		EnvVar: "API_HEALTHCHECK_ENDPOINT",
	})
	contentTopic := app.String(cli.StringOpt{
		Name:   "topic",
		Value:  "",
		Desc:   "Kafka topic to read from.",
		EnvVar: "TOPIC",
	})
	metadataTopic := app.String(cli.StringOpt{
		Name:   "metadata_topic",
		Value:  "",
		Desc:   "Kafka topic for annotation changes.",
		EnvVar: "METADATA_TOPIC",
	})
	port := app.Int(cli.IntOpt{
		Name:   "port",
		Value:  8080,
		Desc:   "application port",
		EnvVar: "PORT",
	})
	historySize := app.Int(cli.IntOpt{
		Name:   "notification_history_size",
		Value:  200,
		Desc:   "the number of recent notifications to be saved and returned on the /__history endpoint",
		EnvVar: "NOTIFICATION_HISTORY_SIZE",
	})
	delay := app.Int(cli.IntOpt{
		Name:   "notifications_delay",
		Value:  30,
		Desc:   "The time to delay each notification before forwarding to any subscribers (in seconds).",
		EnvVar: "NOTIFICATIONS_DELAY",
	})
	contentURIWhitelist := app.String(cli.StringOpt{
		Name:   "content_uri_whitelist",
		Desc:   `The contentURI whitelist for incoming notifications - i.e. ^http://.*-transformer-(pr|iw)-uk-.*\.svc\.ft\.com(:\d{2,5})?/content/[\w-]+.*$`,
		EnvVar: "CONTENT_URI_WHITELIST",
	})
	contentTypeWhitelist := app.Strings(cli.StringsOpt{
		Name:   "content_type_whitelist",
		Value:  []string{},
		Desc:   `Comma-separated list of whitelisted ContentTypes for incoming notifications - i.e. application/vnd.ft-upp-article+json,application/vnd.ft-upp-audio+json`,
		EnvVar: "CONTENT_TYPE_WHITELIST",
	})
	whitelistedMetadataOriginSystemHeaders := app.Strings(cli.StringsOpt{
		Name:   "whitelistedMetadataOriginSystemHeaders",
		Value:  []string{"http://cmdb.ft.com/systems/pac", "http://cmdb.ft.com/systems/methode-web-pub", "http://cmdb.ft.com/systems/next-video-editor"},
		Desc:   "Origin-System-Ids that are supported to be processed from the PostPublicationEvents queue.",
		EnvVar: "WHITELISTED_METADATA_ORIGIN_SYSTEM_HEADERS",
	})

	logLevel := app.String(cli.StringOpt{
		Name:   "logLevel",
		Value:  "INFO",
		Desc:   "Logging level (DEBUG, INFO, WARN, ERROR)",
		EnvVar: "LOG_LEVEL",
	})

	app.Action = func() {

		log.InitLogger(serviceName, *logLevel)

		logV2 := logger.NewUPPLogger(serviceName, *logLevel)
		logV2.WithFields(map[string]interface{}{
			"CONTENT_TOPIC":  *contentTopic,
			"METADATA_TOPIC": *metadataTopic,
			"GROUP_ID":       *consumerGroupID,
			"KAFKA_ADDRS":    *consumerAddrs,
		}).Infof("[Startup] notifications-push is starting ")

		errCh := make(chan error, 2)
		defer close(errCh)
		var fatalErrs = []error{kazoo.ErrPartitionNotClaimed, zk.ErrNoServer}
		fatalErrHandler := func(err error, serviceName string) {
			logV2.WithError(err).Fatalf("Exiting %s due to fatal error", serviceName)
		}

		supervisor := newServiceSupervisor(serviceName, errCh, fatalErrs, fatalErrHandler)
		go supervisor.Supervise()

		consumerConfig := kafka.DefaultConsumerConfig()
		consumerConfig.Zookeeper.Logger = stdlog.New(ioutil.Discard, "", 0)
		messageConsumer, err := kafka.NewConsumer(kafka.Config{
			ZookeeperConnectionString: *consumerAddrs,
			ConsumerGroup:             *consumerGroupID,
			Topics: []string{
				*contentTopic,
				*metadataTopic,
			},
			ConsumerGroupConfig: consumerConfig,
			Err:                 errCh,
		})
		if err != nil {
			logV2.WithError(err).Fatal("Cannot create Kafka client")
		}

		httpClient := &http.Client{
			Transport: &http.Transport{
				Proxy: http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					Timeout:   30 * time.Second,
					KeepAlive: 30 * time.Second,
				}).DialContext,
				MaxIdleConnsPerHost:   20,
				TLSHandshakeTimeout:   3 * time.Second,
				ExpectContinueTimeout: 1 * time.Second,
			},
		}

		history := dispatch.NewHistory(*historySize)
		dispatcher := dispatch.NewDispatcher(time.Duration(*delay)*time.Second, history)

		mapper := queueConsumer.NotificationMapper{
			Resource:   *resource,
			APIBaseURL: *apiBaseURL,
			Property:   &conceptTimeReader{},
		}

		whitelistR, err := regexp.Compile(*contentURIWhitelist)
		if err != nil {
			logV2.WithError(err).Fatal("Whitelist regex MUST compile!")
		}

		apiGatewayHealthcheckURL, err := url.Parse(*apiBaseURL)
		if err != nil {
			logV2.WithError(err).Fatal("cannot parse api_base_url")
		}

		r, err := url.Parse(*apiGatewayHealthcheckEndpoint)
		if err != nil {
			logV2.WithError(err).Fatal("cannot parse api_healthcheck_endpoint")
		}

		apiGatewayHealthcheckURL = apiGatewayHealthcheckURL.ResolveReference(r)

		hc := resources.NewHealthCheck(messageConsumer, apiGatewayHealthcheckURL.String(), &resources.HTTPClient{})
		apiGatewayKeyValidationURL := fmt.Sprintf("%s/%s", *apiBaseURL, *apiKeyValidationEndpoint)

		keyValidator := resources.NewKeyValidator(apiGatewayKeyValidationURL, httpClient, logV2)
		subHandler := resources.NewSubHandler(dispatcher, keyValidator, heartbeatPeriod, logV2)

		router := mux.NewRouter()
		initRouter(router, subHandler, *resource, dispatcher, history, hc)
		go startServer(":"+strconv.Itoa(*port), router)

		ctWhitelist := queueConsumer.NewSet()
		for _, value := range *contentTypeWhitelist {
			ctWhitelist.Add(value)
		}
		contentHandler := queueConsumer.NewContentQueueHandler(whitelistR, ctWhitelist, mapper, dispatcher)
		metadataHandler := queueConsumer.NewMetadataQueueHandler(*whitelistedMetadataOriginSystemHeaders, mapper, dispatcher)
		queueHandler := queueConsumer.NewMessageQueueHandler(contentHandler, metadataHandler)
		pushService := newPushService(dispatcher, messageConsumer)
		pushService.start(queueHandler)
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}

func initRouter(r *mux.Router,
	s *resources.SubHandler,
	resource string,
	d *dispatch.Dispatcher,
	h dispatch.History,
	hc *resources.HealthCheck) {

	r.HandleFunc("/"+resource+"/notifications-push", s.HandleSubscription).Methods("GET")

	r.HandleFunc("/__health", hc.Health())
	r.HandleFunc(httphandlers.GTGPath, httphandlers.NewGoodToGoHandler(hc.GTG))
	r.HandleFunc(httphandlers.BuildInfoPath, httphandlers.BuildInfoHandler)
	r.HandleFunc(httphandlers.PingPath, httphandlers.PingHandler)

	r.HandleFunc("/__stats", resources.Stats(d)).Methods("GET")
	r.HandleFunc("/__history", resources.History(h)).Methods("GET")

}

func startServer(addr string, r *mux.Router) {

	err := http.ListenAndServe(addr, r)
	if err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

type conceptTimeReader struct{}

func (c *conceptTimeReader) LastModified(event queueConsumer.ConceptAnnotationsEvent) string {
	// Currently PostConceptAnnotations event is missing LastModified property for annotations.
	// So we use current time as a substitute.
	return time.Now().Format(time.RFC3339)
}
