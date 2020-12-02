package main

import (
	"fmt"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var f mqtt.MessageHandler = func(client mqtt.Client, msg mqtt.Message) {
	fmt.Printf("TOPIC: %s\n", msg.Topic())
	fmt.Printf("MSG: %s\n", msg.Payload())
}

func connect(message string) mqtt.Client {

	//mqtt.DEBUG = log.New(os.Stdout, "", 0)
	//mqtt.ERROR = log.New(os.Stdout, "", 0)
	opts := mqtt.NewClientOptions().AddBroker("tcp://scw-01.berzuk.com:1883").SetClientID("fr-raspberry")
	opts.SetUsername("hcruz")
	opts.SetPassword("pcxcy872")
	opts.SetKeepAlive(2 * time.Second)
	//opts.SetDefaultPublishHandler(f)
	opts.SetPingTimeout(1 * time.Second)

	c := mqtt.NewClient(opts)
	if token := c.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
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
