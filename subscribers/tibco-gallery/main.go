// ----------------------------------------------------------------------------
// Dump1090 MQTT subscriber - TIBCO
// Subscribe from MQTT and send to TIBCO Gallery
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------

package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

//Configuration Data
type Configuration struct {
	MQTTServerURL string
	MQTTClientID  string
	MQTTTopic     string
	MQTTQos       int
	MQTTUsername  string
	MQTTPassword  string
	Region        string
	Source        string
	TIBURL        string
	TIBUser       string
	TIBPass       string
	LogLevel      string
}

// Struct to create JSON request to TIBCO Gallery
type streamingMessage struct {
	//Format of the message for Spotfire Streaming
	//{"ICAO":"string","FlightId":"string","Altitude":"int","Latitude":"double","Longitude":"double","Heading":"double","Speed":"int","LastReceiveTime":"timestamp","StartReceiveTime":"timestamp","Region":"string","SourceID":"string"},"key":["CQSInternalID"]}}
	ICAO             string
	FlightId         string
	Altitude         int64
	Latitude         float64
	Longitude        float64
	Heading          float64
	Speed            int64
	LastReceiveTime  int64
	StartReceiveTime int64
	Region           string
	SourceID         string
}

// Global variables
var configuration Configuration

// Callback function for each message received
func onMessageReceived(client MQTT.Client, message MQTT.Message) {

	byteData := message.Payload()

	r, _ := gzip.NewReader(bytes.NewReader(byteData))
	result, _ := ioutil.ReadAll(r)

	data := string(result)

	dataArray := strings.Split(data, "\n")

	streamingMessageArray := make([]streamingMessage, 0)

	for _, radarLine := range dataArray {
		lineSplit := strings.Split(radarLine, ",")

		if lineSplit[0] == "1" {
			timestamp, _ := strconv.ParseInt(lineSplit[1], 10, 64)
			singleMessage := createStreamingMessage(lineSplit[2], lineSplit[3], 0, 0.0, 0.0, 0.0, 0, timestamp)
			streamingMessageArray = append(streamingMessageArray, singleMessage)

		} else if lineSplit[0] == "2" {
			timestamp, _ := strconv.ParseInt(lineSplit[1], 10, 64)
			altitude, _ := strconv.ParseInt(lineSplit[3], 10, 64)
			latitude, _ := strconv.ParseFloat(lineSplit[4], 64)
			longitude, _ := strconv.ParseFloat(lineSplit[5], 64)
			singleMessage := createStreamingMessage(lineSplit[2], "", altitude, latitude, longitude, 0.0, 0, timestamp)
			streamingMessageArray = append(streamingMessageArray, singleMessage)
		} else if lineSplit[0] == "3" {
			timestamp, _ := strconv.ParseInt(lineSplit[1], 10, 64)
			altitude, _ := strconv.ParseInt(lineSplit[3], 10, 64)
			latitude, _ := strconv.ParseFloat(lineSplit[4], 64)
			longitude, _ := strconv.ParseFloat(lineSplit[5], 64)
			singleMessage := createStreamingMessage(lineSplit[2], "", altitude, latitude, longitude, 0.0, 0, timestamp)
			streamingMessageArray = append(streamingMessageArray, singleMessage)
		} else if lineSplit[0] == "4" {
			timestamp, _ := strconv.ParseInt(lineSplit[1], 10, 64)
			speedF, _ := strconv.ParseFloat(lineSplit[3], 64)
			speed := int64(speedF)
			track, _ := strconv.ParseFloat(lineSplit[4], 64)
			singleMessage := createStreamingMessage(lineSplit[2], "", 0, 0.0, 0.0, track, speed, timestamp)
			streamingMessageArray = append(streamingMessageArray, singleMessage)

		} else if lineSplit[0] == "5" {
			//
			//timestamp, _ := strconv.ParseInt(dataArray[1], 10, 64)
			//sendRestData(dataArray[2], dataArray[3], 0, 0.0, 0.0, 0.0, 0, timestamp)
		}

	}

	// Send the data here in a separate thread.
	// Change in the fugure to control the maximum number of threads.

	if (len(streamingMessageArray)) > 0 {
		go sendRestData(streamingMessageArray)
		log.Info("Number of tupples sent: ", len(streamingMessageArray))
	} else {
		log.Info("ZERO Tupples. No data sent.")
	}

}

// Main function
func main() {

	// Setup the logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	//log.SetReportCaller(true)

	if configuration.LogLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else if configuration.LogLevel == "INFO" {
		log.SetLevel(log.InfoLevel)
	} else if configuration.LogLevel == "ERROR" {
		log.SetLevel(log.ErrorLevel)
	} else if configuration.LogLevel == "WARN" {
		log.SetLevel(log.WarnLevel)
	} else {
		log.SetLevel(log.InfoLevel) // Make info default in case of missing config
	}

	log.Info("Starting Dump1090 Subscriber to TIBCO Gallery")
	// Read the configuration file using gonfig package
	configuration = Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		log.Error("Error reading configuration file: " + err.Error())
		log.Error("Exiting now.")
		os.Exit(1)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	server := configuration.MQTTServerURL
	topic := configuration.MQTTTopic
	qos := configuration.MQTTQos
	clientid := configuration.MQTTClientID
	username := configuration.MQTTUsername
	password := configuration.MQTTPassword

	connOpts := MQTT.NewClientOptions().AddBroker(server).SetClientID(clientid).SetCleanSession(true)
	if username != "" {
		connOpts.SetUsername(username)
		if password != "" {
			connOpts.SetPassword(password)
		}
	}
	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.Subscribe(topic, byte(qos), onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		log.Error("Error connecting to MQTT server: ", token.Error())

	} else {
		log.Info("Connected to MQTT Server: ", server)
	}

	<-c
}

func createStreamingMessage(icao string, callsign string, altitude int64, latitude float64, longitude float64, heading float64, speed int64, timestamp int64) streamingMessage {
	var region = configuration.Region
	var source = configuration.Source

	var streamingMessage = streamingMessage{
		ICAO:             icao,
		FlightId:         callsign,
		Altitude:         altitude,
		Latitude:         latitude,
		Longitude:        longitude,
		Heading:          heading,
		Speed:            speed,
		LastReceiveTime:  timestamp,
		StartReceiveTime: timestamp,
		Region:           region,
		SourceID:         source}

	return streamingMessage
}

func sendRestData(streamingMessageArray []streamingMessage) string {

	// Setup the logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})
	//log.SetReportCaller(true)

	if configuration.LogLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else if configuration.LogLevel == "INFO" {
		log.SetLevel(log.InfoLevel)
	} else if configuration.LogLevel == "ERROR" {
		log.SetLevel(log.ErrorLevel)
	} else if configuration.LogLevel == "WARN" {
		log.SetLevel(log.WarnLevel)
	} else {
		log.SetLevel(log.InfoLevel) // Make info default in case of missing config
	}

	var url = configuration.TIBURL
	var user = configuration.TIBUser
	var pass = configuration.TIBPass

	// use MarshalIndent to reformat slice array as JSON
	sbMessage, _ := json.Marshal(streamingMessageArray)

	log.Debug("JSON Payload to send: ", string(sbMessage))

	//fmt.Println("JSON Payload to send: ", string(sbMessage))

	//Init HTTP client here
	client := &http.Client{Timeout: 3 * time.Second}

	// Publish message to TIBCO Streaming
	req, err := http.NewRequest(http.MethodPut, url, bytes.NewBuffer(sbMessage))
	req.SetBasicAuth(user, pass)
	// set the request header Content-Type for json
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("Accept", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Error("Error invoking TIBCO server: ", err.Error())
		return ""
	}
	defer resp.Body.Close()
	bodyBytes, err := ioutil.ReadAll(resp.Body)

	log.Debug("TIBCO server response: ", resp.StatusCode, ":", string(bodyBytes))

	if len(bodyBytes) == 0 {
		log.Error("TIBCO server response with ZERO bytes: ", err.Error())
	}
	return resp.Status

}
