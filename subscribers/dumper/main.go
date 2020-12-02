// ----------------------------------------------------------------------------
// Dump1090 MQTT subscriber
// Subscribed from MQTT and prints to STDOUT
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io/ioutil"

	"os"
	"os/signal"
	"syscall"

	MQTT "github.com/eclipse/paho.mqtt.golang"
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
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {

	byteData := message.Payload()

	//Decompress the payload message
	r, _ := gzip.NewReader(bytes.NewReader(byteData))
	result, _ := ioutil.ReadAll(r)

	data := string(result)

	fmt.Print(data)

}

func main() {

	// Read the configuration file using gonfig package
	configuration := Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		fmt.Println("Error reading configuration file: " + err.Error())
		fmt.Println("Exiting now.")
		os.Exit(1)

	}

	// Create channel for subscription
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	connOpts := MQTT.NewClientOptions().AddBroker(configuration.MQTTServerURL).SetClientID(configuration.MQTTClientID).SetCleanSession(true)

	if configuration.MQTTUsername != "" {
		connOpts.SetUsername(configuration.MQTTUsername)
		if configuration.MQTTPassword != "" {
			connOpts.SetPassword(configuration.MQTTPassword)
		}
	}
	tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
	connOpts.SetTLSConfig(tlsConfig)

	connOpts.OnConnect = func(c MQTT.Client) {
		if token := c.Subscribe(configuration.MQTTTopic, byte(configuration.MQTTQos), onMessageReceived); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}
	}

	client := MQTT.NewClient(connOpts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	<-c
}
