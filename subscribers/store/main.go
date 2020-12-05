// ----------------------------------------------------------------------------
// Dump1090 MQTT subscriber
// Subscribed from MQTT and save in a file on a hourly basis rotation
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"os"
	"os/signal"
	"syscall"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

// Channel variables
var done = make(chan bool)
var tasks = make(chan string)

//Configuration global varaible
var configuration Configuration

type Configuration struct {
	MQTTServerURL string
	MQTTClientID  string
	MQTTTopic     string
	MQTTQos       int
	MQTTUsername  string
	MQTTPassword  string
	FilesPath     string
	LogLevel      string
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {

	byteData := message.Payload()

	r, _ := gzip.NewReader(bytes.NewReader(byteData))
	result, _ := ioutil.ReadAll(r)

	data := string(result)

	tasks <- data

}

func genFileName(startTime time.Time) string {

	hour, _, _ := startTime.Clock()
	year := startTime.Year()
	month := int(startTime.Month())
	day := startTime.Day()

	//Change later put trailing zero - Use the time formating
	monthStr := strconv.Itoa(month)
	if month < 10 {
		monthStr = "0" + monthStr
	}

	//Change later put trailing zero
	dayStr := strconv.Itoa(day)
	if day < 10 {
		dayStr = "0" + dayStr
	}

	//Change later put trailing zero
	hourStr := strconv.Itoa(hour)
	if hour < 10 {
		hourStr = "0" + hourStr
	}

	filename := "fr-" + strconv.Itoa(year) + monthStr + dayStr + "_" + hourStr + "00.csv"
	return filename

}

// Calculate next roll over - return next rollover timestamp
// and initial time of start (used for filename)
func nextRollOver() (int64, time.Time) {

	loc, _ := time.LoadLocation("UTC")
	currentTime := time.Now().In(loc)
	hour, _, _ := currentTime.Clock()
	year := currentTime.Year()
	month := currentTime.Month()
	day := currentTime.Day()

	windowInit := time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
	windowClose := windowInit.Add(time.Hour)

	windowCloseTS := windowClose.UnixNano() / 1000000
	return windowCloseTS, windowInit
}

func consume() {
	nextRoll, startTime := nextRollOver()
	filename := genFileName(startTime)

	fullpath := filepath.Join(configuration.FilesPath, filename)
	file, err := os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	loc, _ := time.LoadLocation("UTC")

	//Debug
	tm := time.Unix(nextRoll/1000, 0).In(loc)
	timeNow := time.Now().In(loc)
	timeNowMillis := timeNow.UnixNano() / 1000000
	log.Debug("Current time       : ", timeNow)
	log.Debug("Current timestamp  : ", timeNowMillis)
	log.Debug("Start Time         : ", startTime)
	log.Debug("Next roll time     : ", tm)
	log.Debug("Next Roll timestamp: ", nextRoll)
	log.Debug("New Filename       : ", filename)

	for {
		msg := <-tasks

		//Split into Individual messages
		dataArray := strings.Split(msg, "\n")

		for _, radarLine := range dataArray {
			lineSplit := strings.Split(radarLine, ",")

			// Last line is empry after split
			if len(lineSplit) == 1 {
				continue
			}

			timestampStr := lineSplit[1]

			timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				log.Error("Error converting time")
			}

			//fmt.Printf("CurrentTimestamp: %d", timestamp)

			if timestamp >= nextRoll {
				log.Info("Rolling the storage file now.")
				file.Close()
				e := os.Rename(fullpath+".tmp", fullpath)
				if e != nil {
					log.Fatal(e)
				}

				nextRoll, startTime = nextRollOver()
				filename = genFileName(startTime)

				//Debug
				tm := time.Unix(nextRoll/1000, 0).In(loc)
				timeNow := time.Now().In(loc)
				timeNowMillis := timeNow.UnixNano() / 1000000
				log.Debug("Current time       : ", timeNow)
				log.Debug("Current timestamp  : ", timeNowMillis)
				log.Debug("Start Time         : ", startTime)
				log.Debug("Next roll time     : ", tm)
				log.Debug("Next Roll timestamp: ", nextRoll)
				log.Debug("New Filename       : ", filename)

				fullpath := filepath.Join(configuration.FilesPath, filename)
				file, err = os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
				if err != nil {
					panic(err)
				}
			}

			//Write to the file
			file.WriteString(radarLine + "\n")

		}

	}
}

func main() {

	// Setup the logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.Info(">>>>>>>>>> STARTING the Dump1090 Store Subscriber <<<<<<<<<<<<<")

	// Read the configuration
	configuration = Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		log.Error("Error reading configuration: ", err.Error())
		os.Exit(1)
	}

	// Setup the Log Level
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

	// Channel for MQTT subscription
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	// Read from configuration variables - OPTIMIZE IN THE FUTURE
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
		log.Error("Error connecting to MQTT Servrer: ", token.Error())
		os.Exit(1)
	} else {
		log.Info("Connected to MQTT Server:", server)
	}

	// Start the consume goroutine
	go consume()

	<-c
}
