package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type InfluxJsonLnsDown struct {
	Name      string        `json:"name"`
	Fields    LnsDownFields `json:"fields"`
	Tags      LnsDownTags   `json:"tags"`
	Timestamp uint64        `json:"timestamp"`
}
type LnsDownFields struct {
	Data      string `json:"data"`
	Confirmed bool   `json:"confirmed"`
	FPort     uint64 `json:"fPort"`
}
type LnsDownTags struct {
	Application string `json:"application"`
	DeviceId    string `json:"deviceId"`
	DeviceType  string `json:"deviceType"`
	Direction   string `json:"direction"`
	Host        string `json:"host"`
	Origin      string `json:"origin"`
	Reference   string `json:"reference"`
}

// type LnsDown struct {
// 	Measurement string `json:"measurement"`
// 	Reference   string `json:"reference"`
// 	DeviceId    string `json:"deviceId"`
// 	Confirmed   bool   `json:"confirmed"`
// 	FPort       uint64 `json:"fPort"`
// 	Data        string `json:"data"`
// 	Timestamp   uint64 `json:"timestamp"`
// 	// Object any
// }

type LnsImtDown struct {
	Application string
	DeviceId    string
	Reference   string
	Confirmed   bool
	FPort       uint64
	Data        string
	// Object any
}

func connLostHandler(c MQTT.Client, err error) {
	fmt.Printf("Connection lost, reason: %v\n", err)
	os.Exit(1)
}

func main() {
	id := uuid.New().String()

	var influxJsonLnsDown InfluxJsonLnsDown
	var lnsImtDown LnsImtDown

	var sbMqttSubClientId strings.Builder
	var sbMqttPubClientId strings.Builder
	var sbPubTopic strings.Builder
	sbMqttSubClientId.WriteString("mqtt-rewrite-to-lns-imt-")
	sbMqttSubClientId.WriteString(id)
	sbMqttPubClientId.WriteString("mqtt-rewrite-to-lns-imt-")
	sbMqttPubClientId.WriteString(id)

	mqttSubBroker := "mqtt://mqtt.maua.br:1883"
	mqttSubClientId := sbMqttSubClientId.String()
	mqttSubUser := ""
	mqttSubPassword := ""
	mqttSubQos := 0

	mqttSubOpts := MQTT.NewClientOptions()
	mqttSubOpts.AddBroker(mqttSubBroker)
	mqttSubOpts.SetClientID(mqttSubClientId)
	mqttSubOpts.SetUsername(mqttSubUser)
	mqttSubOpts.SetPassword(mqttSubPassword)
	mqttSubOpts.SetConnectionLostHandler(connLostHandler)

	mqttSubTopics := map[string]byte{
		"IMT/LNS/Command/+/down/imt": byte(mqttSubQos),
	}

	mqttPubBroker := "mqtt://networkserver.maua.br:1883"
	mqttPubClientId := sbMqttPubClientId.String()
	mqttPubUser := "PUBLIC"
	mqttPubPassword := "public"
	mqttPubQos := 0

	mqttPubOpts := MQTT.NewClientOptions()
	mqttPubOpts.AddBroker(mqttPubBroker)
	mqttPubOpts.SetClientID(mqttPubClientId)
	mqttPubOpts.SetUsername(mqttPubUser)
	mqttPubOpts.SetPassword(mqttPubPassword)

	c := make(chan [2]string)

	mqttSubOpts.SetDefaultPublishHandler(func(mqttSubClient MQTT.Client, msg MQTT.Message) {
		c <- [2]string{msg.Topic(), string(msg.Payload())}
	})

	mqttSubClient := MQTT.NewClient(mqttSubOpts)
	if token := mqttSubClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttSubBroker)
	}

	pClient := MQTT.NewClient(mqttPubOpts)
	if token := pClient.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	} else {
		fmt.Printf("Connected to %s\n", mqttPubBroker)
	}

	if token := mqttSubClient.SubscribeMultiple(mqttSubTopics, nil); token.Wait() && token.Error() != nil {
		fmt.Println(token.Error())
		os.Exit(1)
	}

	for {
		incoming := <-c
		// Parse the Topic
		t := strings.Split(incoming[0], "/")

		// Parse the Message
		json.Unmarshal([]byte(incoming[1]), &influxJsonLnsDown)
		lnsImtDown.Application = influxJsonLnsDown.Tags.Application
		lnsImtDown.DeviceId = influxJsonLnsDown.Tags.DeviceId
		lnsImtDown.Confirmed = influxJsonLnsDown.Fields.Confirmed
		lnsImtDown.Reference = influxJsonLnsDown.Tags.Reference
		// a := influxJsonLnsDown.Fields
		lnsImtDown.FPort = influxJsonLnsDown.Fields.FPort
		lnsImtDown.Data = influxJsonLnsDown.Fields.Data
		// fmt.Printf("RECEIVED MESSAGE DeviceId: %s\n", lnsImtDown.DeviceId)
		// fmt.Printf("RECEIVED MESSAGE Confirmed: %v\n", influxJsonLnsDown.Fields.Confirmed)
		// fmt.Printf("RECEIVED MESSAGE FPort: %s\n", lnsImtDown.FPort)
		// fmt.Printf("RECEIVED MESSAGE Data: %s\n", lnsImtDown.Data)
		// fmt.Printf("RECEIVED MESSAGE RAW: %s\n", incoming[1])

		var imtApplicationId string
		switch lnsImtDown.Application {
		case "DET":
			imtApplicationId = "1"

		case "SmartLight":
			imtApplicationId = "6"

		case "EnergyMeter":
			imtApplicationId = "9"

		case "WeatherStation":
			imtApplicationId = "13"

		case "WaterTankLevel":
			imtApplicationId = "18"

		case "GaugePressure":
			imtApplicationId = "19"

		case "Hydrometer":
			imtApplicationId = "20"
		}

		var sbPubMessage strings.Builder
		sbPubMessage.WriteString(`{`)
		sbPubMessage.WriteString(`"reference":"`)
		sbPubMessage.WriteString(lnsImtDown.Reference)
		sbPubMessage.WriteString(`","confirmed":`)
		sbPubMessage.WriteString(strconv.FormatBool(lnsImtDown.Confirmed))
		sbPubMessage.WriteString(`,"fPort":`)
		sbPubMessage.WriteString(strconv.FormatUint(uint64(lnsImtDown.FPort), 10))
		sbPubMessage.WriteString(`,"data":"`)
		sbPubMessage.WriteString(lnsImtDown.Data)
		sbPubMessage.WriteString(`"}`)

		deviceId := t[3]
		sbPubTopic.Reset()
		sbPubTopic.WriteString("application/")
		sbPubTopic.WriteString(imtApplicationId)
		sbPubTopic.WriteString("/node/")
		sbPubTopic.WriteString(deviceId)
		sbPubTopic.WriteString("/tx")
		// fmt.Printf("RECEIVED TOPIC: %s MESSAGE: %s\n", incoming[0], incoming[1])
		// fmt.Printf("Pub NS Topic: %s\n", sbPubTopic.String())
		// fmt.Printf("Pub NS Message: %s\n", sbPubMessage.String())

		token := pClient.Publish(sbPubTopic.String(), byte(mqttPubQos), false, sbPubMessage.String())
		token.Wait()
	}
}
