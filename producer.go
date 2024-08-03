package xmqtt

// Publish 推送消息
func (c *Client) Publish(topic string, qos QosType, payload any) error {
	var (
		wait bool
		err  error
	)
	token := c.Mqtt.Publish(topic, byte(qos), false, payload)
	if c.Timeout > 0 {
		wait = token.WaitTimeout(c.Timeout)
	} else {
		wait = token.Wait()
	}
	err = token.Error()
	if wait && err != nil {
		return err
	}
	return nil
}
