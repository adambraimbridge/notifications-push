package main

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/Financial-Times/go-logger/v2"
	"github.com/Financial-Times/notifications-push/v5/resources"
	"github.com/gorilla/mux"
	cli "github.com/jawher/mow.cli"
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

	log := logger.NewUPPLogger(serviceName, *logLevel)

	app.Action = func() {

		log.WithFields(map[string]interface{}{
			"CONTENT_TOPIC":  *contentTopic,
			"METADATA_TOPIC": *metadataTopic,
			"GROUP_ID":       *consumerGroupID,
			"KAFKA_ADDRS":    *consumerAddrs,
		}).Infof("[Startup] notifications-push is starting ")

		kafkaConsumer, err := createSupervisedConsumer(log,
			*consumerAddrs,
			*consumerGroupID,
			[]string{
				*contentTopic,
				*metadataTopic,
			})
		if err != nil {
			log.WithError(err).Fatal("could not start kafka consumer")
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

		router := mux.NewRouter()
		srv := &http.Server{
			Addr:    ":" + strconv.Itoa(*port),
			Handler: router,
		}

		baseURL, err := url.Parse(*apiBaseURL)
		if err != nil {
			log.WithError(err).Fatal("cannot parse api_base_url")
		}

		healthCheckEndpoint, err := url.Parse(*apiGatewayHealthcheckEndpoint)
		if err != nil {
			log.WithError(err).Fatal("cannot parse api_healthcheck_endpoint")
		}

		healthCheckEndpoint = baseURL.ResolveReference(healthCheckEndpoint)
		hc := resources.NewHealthCheck(kafkaConsumer, healthCheckEndpoint.String(), requestStatusCode)

		dispatcher, history := createDispatcher(*delay, *historySize, log)

		msgConfig := msgHandlerCfg{
			Resource:        *resource,
			BaseURL:         *apiBaseURL,
			ContentURI:      *contentURIWhitelist,
			ContentTypes:    *contentTypeWhitelist,
			MetadataHeaders: *whitelistedMetadataOriginSystemHeaders,
		}

		queueHandler, err := createMessageHandler(msgConfig, dispatcher, log)
		if err != nil {
			log.WithError(err).Fatal("could not start notification consumer")
		}

		keyValidateURL, err := url.Parse(*apiKeyValidationEndpoint)
		if err != nil {
			log.WithError(err).Fatal("cannot parse api_key_validation_endpoint")
		}
		keyValidateURL = baseURL.ResolveReference(keyValidateURL)
		keyValidator := resources.NewKeyValidator(keyValidateURL.String(), httpClient, log)
		subHandler := resources.NewSubHandler(dispatcher, keyValidator, srv, heartbeatPeriod, log)
		if err != nil {
			log.WithError(err).Fatal("Could not create request handler")
		}

		initRouter(router, subHandler, *resource, dispatcher, history, hc, log)

		shutdown := startService(srv, dispatcher, kafkaConsumer, queueHandler, log)

		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
		<-ch

		shutdown(time.Second * 30)
	}

	if err := app.Run(os.Args); err != nil {
		log.Fatal(err)
	}
}
