package main

import (
	"log"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/go-pay/xmqtt"
)

func main1() {
	// 初始化参数和配置
	emqx := xmqtt.New(&xmqtt.Config{
		Broker:       "127.0.0.1",
		TcpPort:      1883,
		ClientId:     "clientid_consumer",
		Uname:        "uname",
		Password:     "password",
		CleanSession: true,
	})
	// 设置Mqtt连接监听
	emqx.OnConnectListener(emqx.DefaultOnConnectFunc)
	// 设置Mqtt断开连接监听
	emqx.OnConnectLostListener(func(client mqtt.Client, err error) {
		log.Printf("[%s]IsConnected[%t] lost connection, err: %+v.\n", emqx.Ops.ClientID, client.IsConnected(), err)
		_ = emqx.UnSubscribe(emqx.Topics...)
	})
	// 启动连接
	if err := emqx.StartAndConnect(); err != nil {
		panic(err)
	}
	// 批量注册 Consumer topic 监听
	cs := []*xmqtt.Consumer{
		{
			Topic:   "topic1",
			QosType: xmqtt.QosAtMostOne,
			Callback: func(client mqtt.Client, message mqtt.Message) {
				log.Printf("topic1: %+v.\n", string(message.Payload()))
			},
		}, {
			Topic:   "topic2",
			QosType: xmqtt.QosAtLeastOne,
			Callback: func(client mqtt.Client, message mqtt.Message) {
				log.Printf("topic2: %+v.\n", string(message.Payload()))
			},
		}, {
			Topic:   "topic3",
			QosType: xmqtt.QosOnlyOne,
			Callback: func(client mqtt.Client, message mqtt.Message) {
				log.Printf("topic3: %+v.\n", string(message.Payload()))
			},
		},
	}
	emqx.RegisterConsumers(cs)

	// end
	time.Sleep(time.Minute)

	emqx.Close()
}
