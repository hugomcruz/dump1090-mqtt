// ----------------------------------------------------------------------------
// Dump1090 messages processor and publisher to MQTT
// Main file
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

// Global variables
var configuration Configuration

//Plain Dump1090 port 30003 (default) structure
type radarRawLine struct {
	status           string
	errorMessage     string
	messageType      string
	transmissionType string
	sessionID        string
	aircraftID       string
	hexIdent         string
	flightID         string
	timestamp        int64
	callSign         string
	altitude         int64
	groundSpeed      float64
	track            float64
	latitude         float64
	longitude        float64
	verticalRate     int64
	squak            string
	alert            string
	emergency        string
	spiIdent         string
	isOnGround       string
}

//Configuration Data
type Configuration struct {
	MQTTServerURL   string
	MQTTClientID    string
	MQTTTopic       string
	MQTTQos         int
	MQTTUsername    string
	MQTTPassword    string
	Dump1090Server  string
	Dump1090Port    int
	BatchTimeWindow int
	LogLevel        string
}

func main() {

	// Setup the logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	// Set the log level
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

	log.Info("Starting Dump1090 processor and Publisher to MQTT")

	// Read the configuration file using gonfig package
	configuration = Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		log.Error("Error reading configuration file: " + err.Error())
		log.Error("Exiting now.")
		os.Exit(1)

	}

	//Connect to MQTT
	client := connect(configuration)

	// Error connecting to MQTT
	if client == nil {
		log.Error("Error during connection to MQTT. Exiting now...")
		os.Exit(1)
	}

	ip := configuration.Dump1090Server
	port := strconv.Itoa(configuration.Dump1090Port)

	log.Info("Connecting to dump1090: " + ip + ":" + port)

	//Connect socket
	conn, err := net.Dial("tcp", ip+":"+port)

	if err != nil {
		disconnect(client)
		log.Error("Error connecting to DUMP1090. Exiting now...")
		os.Exit(1)
	}

	log.Info("Connection to DUMP1090 started...")

	//message, _ := bufio.NewReader(conn).ReadString('\n')
	scanner := bufio.NewScanner(conn)
	scanner.Split(ScanCRLF)

	tmpMessageBuffer := make([]string, 0)

	timeWindow := int64(configuration.BatchTimeWindow)

	// Initiate the first start tme
	startTime := time.Now().Unix()

	for scanner.Scan() {

		if err := scanner.Err(); err != nil {
			log.Warn("Invalid input:" + err.Error())
			continue
		}

		radarLine := scanner.Text()

		tmpMessageBuffer = append(tmpMessageBuffer, radarLine)

		// Change to a variable - 3 seconds no
		endTime := time.Now().Unix()
		timeDelta := endTime - startTime

		//log.Debug("Time Delta: ", timeDelta)

		// Check if time window for batch is exceeded
		if timeDelta >= timeWindow {
			log.Debug("Batch window completed. Preparing to send data.")
			// FUTURE: Change here to send to a multithreaded worker

			// Compress (GZIP) the batched message
			compressedMessage := compress(tmpMessageBuffer)

			topic := configuration.MQTTTopic
			send(client, topic, compressedMessage)

			// Reset variables for next time window batch
			startTime = time.Now().Unix()
			tmpMessageBuffer = make([]string, 0)
		}
	}
}

// Compress the message pyloading with GZIP
func compress(messageList []string) []byte {

	decodedMessages := make([]string, 0)

	for _, s := range messageList {
		rawLine := processLine(s)
		processedLine := decodeData(rawLine)

		if processedLine == "" {
			continue
		} else {
			decodedMessages = append(decodedMessages, processedLine)
		}
	}

	superString := ""
	for _, s := range decodedMessages {
		superString = superString + s + "\n"

	}

	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	if _, err := gz.Write([]byte(superString)); err != nil {
		log.Fatal(err)
	}
	if err := gz.Close(); err != nil {
		log.Fatal(err)
	}

	log.Debug("Batch original size: ", len(superString), ". Batch compressed size:", len(b.Bytes()))

	return b.Bytes()

}

// Decode the DUMP1090 messages by message type.
func decodeData(radarData radarRawLine) string {
	message := ""

	if radarData.messageType == "AIR" {
		processAIRMessage(radarData)
	} else if radarData.messageType == "ID" {
		processIDMessage(radarData)
	} else if radarData.messageType == "STA" {
		processSTAMessage(radarData)
	} else if radarData.messageType == "MSG" && radarData.transmissionType == "1" {
		message = processMsg1(radarData)

	} else if radarData.messageType == "MSG" && radarData.transmissionType == "2" {
		message = processMsg2(radarData)

	} else if radarData.messageType == "MSG" && radarData.transmissionType == "3" {
		message = processMsg3(radarData)

	} else if radarData.messageType == "MSG" && radarData.transmissionType == "4" {
		message = processMsg4(radarData)

	} else if radarData.messageType == "MSG" && radarData.transmissionType == "5" {
		message = processMsg5(radarData)

	} else if radarData.messageType == "MSG" && radarData.transmissionType == "6" {
		message = processMsg6(radarData)
	}

	return message
}

func processLine(line string) radarRawLine {
	stringArray := strings.Split(line, ",")

	var rawline radarRawLine

	// Common to All Messages
	rawline.messageType = stringArray[0]
	rawline.transmissionType = stringArray[1]
	rawline.sessionID = stringArray[2]
	rawline.aircraftID = stringArray[3]
	rawline.hexIdent = stringArray[4]
	rawline.flightID = stringArray[5]
	rawline.timestamp = dateStringToTimestamp(stringArray[6] + "T" + stringArray[7])

	// Add on callsign for ID message
	if len(stringArray) == 11 {

		rawline.callSign = stringArray[10]
	}

	// Add data for MSG
	if len(stringArray) == 22 {

		rawline.callSign = stringArray[10]

		// ALTITUDE - Convert from String to Int64
		altitude, error := strconv.ParseInt(stringArray[11], 10, 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			altitude = 0
		}
		rawline.altitude = altitude

		// GROUND SPEED - Convert from String to Float64
		groundSpeed, error := strconv.ParseFloat(stringArray[12], 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			groundSpeed = 0.0
		}
		rawline.groundSpeed = groundSpeed

		// TRACK - Convert from String to Float64
		track, error := strconv.ParseFloat(stringArray[13], 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			track = 0.0
		}
		rawline.track = track

		// LATITUDE - Convert from String to Float64
		latitude, error := strconv.ParseFloat(stringArray[14], 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			latitude = 0.0
		}
		rawline.latitude = latitude

		// LONGITUDE - Convert from String to Float64
		longitude, error := strconv.ParseFloat(stringArray[15], 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			longitude = 0.0
		}
		rawline.longitude = longitude

		// VERTICAL RATE - Convert from String to Float64
		verticalRate, error := strconv.ParseInt(stringArray[16], 10, 64)
		if error != nil {
			rawline.status = "ERROR"
			rawline.errorMessage = error.Error()
			verticalRate = 0
		}
		rawline.verticalRate = verticalRate

		rawline.squak = stringArray[17]
		rawline.alert = stringArray[18]
		rawline.emergency = stringArray[19]
		rawline.spiIdent = stringArray[20]
		rawline.isOnGround = stringArray[21]
	}
	return rawline

}

func processAIRMessage(messageData radarRawLine) {
	// Ignoring AIR messages for now.

}

func processIDMessage(messageData radarRawLine) {
	// Ignoring AIR messages for now.
}

func processSTAMessage(messageData radarRawLine) {
	// Ignoring AIR messages for now.
}

func processMsg1(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	callSign := messageData.callSign
	return "1," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + callSign
}

func processMsg2(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	altitude := messageData.altitude
	latitude := messageData.latitude
	longitude := messageData.longitude
	onground := messageData.isOnGround
	return "2," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + strconv.FormatInt(altitude, 10) + "," + strconv.FormatFloat(latitude, 'f', 5, 64) + "," + strconv.FormatFloat(longitude, 'f', 5, 64) + "," + onground
}

// Message type 3 - Airborne Location and Altitude
func processMsg3(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	altitude := messageData.altitude
	latitude := messageData.latitude
	longitude := messageData.longitude
	onground := messageData.isOnGround
	return ("3," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + strconv.FormatInt(altitude, 10) + "," + strconv.FormatFloat(latitude, 'f', 5, 64) + "," + strconv.FormatFloat(longitude, 'f', 5, 64) + "," + onground)
}

func processMsg4(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	speed := messageData.groundSpeed
	track := messageData.track
	verticalRate := messageData.verticalRate
	return ("4," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + strconv.FormatFloat(speed, 'f', 1, 64) + "," + strconv.FormatFloat(track, 'f', 1, 64) + "," + strconv.FormatInt(verticalRate, 10))
}

func processMsg5(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	altitude := messageData.altitude
	onground := messageData.isOnGround

	//Add ons for emergency - NOT IMPLEMENTED
	//emergency := messageData.emergency
	//spiIdent := messageData.spiIdent

	return ("5," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + strconv.FormatInt(altitude, 10) + "," + onground)

}

func processMsg6(messageData radarRawLine) string {
	timestamp := messageData.timestamp
	aircraftHexCode := messageData.hexIdent
	altitude := messageData.altitude
	squak := messageData.squak

	//Add ons for emergency - NOT IMPLEMENTED
	//emergency := messageData.emergency
	//spiIdent := messageData.spiIdent
	//alert := messageData.alert
	//onground := messageData.isOnGround

	return ("6," + strconv.FormatInt(timestamp, 10) + "," + aircraftHexCode + "," + strconv.FormatInt(altitude, 10) + "," + squak)

}

// Convert the date format in dump1090 to a Unix times
func dateStringToTimestamp(dateString string) int64 {

	// Currently only works on clocks with UTC timezone
	loc, _ := time.LoadLocation("UTC")
	layout := "2006/01/02T15:04:05.000"

	t, err := time.ParseInLocation(layout, dateString, loc)

	if err != nil {
		log.Error("Error parsing record time: ", err.Error())
	}

	timestamp := t.UnixNano() / int64(time.Millisecond)

	return timestamp

}

func recreateMessage(stringArray []string) string {
	var tmpString string

	for i := 0; i < (len(stringArray) - 1); i++ {
		tmpString = tmpString + stringArray[i] + ","
	}

	finalString := tmpString + stringArray[len(stringArray)-1]

	return finalString
}

// dropCR drops a terminal \r from the data.
func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

func ScanCRLF(data []byte, atEOF bool) (advance int, token []byte, err error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.Index(data, []byte{'\r', '\n'}); i >= 0 {
		// We have a full newline-terminated line.
		return i + 2, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil
}

// Abs returns the absolute value of x.
func Abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
