// ----------------------------------------------------------------------------
// MQTT Functions
//
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------

package main

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	log "github.com/sirupsen/logrus"
)

// Global varibales
const keepalive = 2
const pingTimeout = 1

var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func connect(configuration Configuration) mqtt.Client {
	log.Info("Connecting to MQTT: ", configuration.MQTTServerURL)

	opts := mqtt.NewClientOptions().AddBroker(configuration.MQTTServerURL).SetClientID(configuration.MQTTClientID)
	opts.SetUsername(configuration.MQTTUsername)
	opts.SetPassword(configuration.MQTTPassword)
	opts.SetKeepAlive(keepalive * time.Second)
	//opts.SetDefaultPublishHandler(f)
	opts.SetPingTimeout(pingTimeout * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		//panic(token.Error())
		log.Error("Error connecting to MQTT: ", token.Error())
		return nil
	}

	return c
}

func send(client mqtt.Client, topic string, message []byte) {

	client.Publish(topic, 0, false, message)
	//token := c.Publish("go-mqtt/sample", 0, false, message)
	//token.Wait()

}

func disconnect(c mqtt.Client) {
	c.Disconnect(250)

}
