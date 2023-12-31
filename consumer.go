package xmqtt

import (
	"fmt"
	"log"
	"runtime"

	"github.com/eclipse/paho.mqtt.golang"
)

func subCallbackKey(topic string, qos QosType) string {
	return fmt.Sprintf("%s#%v", topic, qos)
}

func (c *Client) sub(topic string, qos QosType, callback mqtt.MessageHandler) error {
	token := c.Mqtt.Subscribe(topic, byte(qos), callback)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (c *Client) goSubConsumer(consumer *Consumer) {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			buf = buf[:runtime.Stack(buf, false)]
			log.Printf("goSubConsumer: panic recovered: %s\n%s.\n", r, buf)
		}
	}()
	err := c.Subscribe(consumer.Topic, consumer.QosType, consumer.Callback)
	if err != nil {
		log.Printf("[%s] Subscribe.Topic[%s], Qos[%d], err:%+v.\n", c.Ops.ClientID, consumer.Topic, consumer.QosType, err)
		return
	}
	log.Printf("[%s] Subscribe.Topic[%s].\n", c.Ops.ClientID, consumer.Topic)
}

// Subscribe 订阅topic
func (c *Client) Subscribe(topic string, qos QosType, callback mqtt.MessageHandler) error {
	// callback 缓存，断开连接后，重新注册订阅
	c.mu.Lock()
	c.SubFuncs[subCallbackKey(topic, qos)] = callback
	c.Topics = append(c.Topics, topic)
	c.mu.Unlock()
	return c.sub(topic, qos, callback)
}

// UnSubscribe 取消订阅topic
func (c *Client) UnSubscribe(topics ...string) error {
	token := c.Mqtt.Unsubscribe(topics...)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

// RegisterConsumers 批量注册消费者
func (c *Client) RegisterConsumers(consumers []*Consumer) {
	for _, consumer := range consumers {
		go c.goSubConsumer(consumer)
	}
}
