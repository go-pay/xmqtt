package xmqtt

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/eclipse/paho.mqtt.golang"
	"github.com/go-pay/smap"
	"github.com/go-pay/util/retry"
	"github.com/go-pay/xlog"
)

type Client struct {
	c       *Config
	Ops     *mqtt.ClientOptions
	Timeout time.Duration // connection、subscribe、publish Timeout time, default 10s
	Mqtt    mqtt.Client
	Topics  []string
	SubFunc smap.Map[string, mqtt.MessageHandler] // key:topic#qos, value: callback func
}

// New 1、New
func New(c *Config) (mc *Client) {
	// 日志
	mqtt.ERROR = log.New(os.Stderr, "[MQTT.ERROR] >> ", log.Lmsgprefix|log.Lshortfile|log.Ldate|log.Lmicroseconds)
	var (
		clientId = c.ClientId
		ops      = mqtt.NewClientOptions()
	)
	ops.AddBroker(fmt.Sprintf("tcp://%s:%d", c.Broker, c.TcpPort))
	ops.SetClientID(clientId)
	ops.SetUsername(c.Uname)
	ops.SetPassword(c.Password)
	if c.KeepAlive > 0 {
		ops.SetKeepAlive(time.Duration(c.KeepAlive))
	}
	ops.SetCleanSession(c.CleanSession)
	mc = &Client{
		c:       c,
		Timeout: 10 * time.Second,
		Ops:     ops,
	}
	if c.Timeout > 0 {
		mc.Timeout = time.Duration(c.Timeout)
	}
	return mc
}

// OnConnectListener 2、设置链接监听
func (c *Client) OnConnectListener(fun mqtt.OnConnectHandler) (mc *Client) {
	if fun != nil {
		c.Ops.OnConnect = fun
	}
	return c
}

// OnConnectLostListener 3、设置断开链接监听
func (c *Client) OnConnectLostListener(fun mqtt.ConnectionLostHandler) (mc *Client) {
	if fun != nil {
		c.Ops.OnConnectionLost = fun
	}
	return c
}

// StartAndConnect 4、真实创建Client并连接mqtt
func (c *Client) StartAndConnect() (err error) {
	if c.Ops.OnConnect == nil {
		c.Ops.OnConnect = c.DefaultOnConnectFunc
	}
	// new
	c.Mqtt = mqtt.NewClient(c.Ops)
	// connect with retry
	err = retry.Retry(func() error {
		var (
			wait bool
			e    error
		)
		token := c.Mqtt.Connect()
		if c.Timeout > 0 {
			wait = token.WaitTimeout(c.Timeout)
		} else {
			wait = token.Wait()
		}
		e = token.Error()
		if wait && e != nil {
			return e
		}
		return nil
	}, 3, 2*time.Second)
	if err != nil {
		return err
	}
	return nil
}

// Close 主动断开连接
func (c *Client) Close() {
	if len(c.Topics) > 0 {
		_ = c.UnSubscribe(c.Topics...)
		c.Topics = nil
	}
	c.Mqtt.Disconnect(1000)
}

func (c *Client) DefaultOnConnectFunc(cli mqtt.Client) {
	xlog.Warnf("Clientid [%s] Connected.", c.Ops.ClientID)
	// 若 c.SubFuncs 不为空，连接后注册订阅
	c.SubFunc.Range(func(key string, handler mqtt.MessageHandler) bool {
		// 协程
		go func(k string, cb mqtt.MessageHandler) {
			defer func() {
				if r := recover(); r != nil {
					buf := make([]byte, 64<<10)
					buf = buf[:runtime.Stack(buf, false)]
					xlog.Errorf("reSubscribe: panic recovered: %s\n%s.", r, buf)
				}
			}()
			split := strings.Split(k, "#")
			if len(split) == 2 {
				var qos QosType
				switch split[1] {
				case "0":
					qos = QosAtMostOne
				case "1":
					qos = QosAtLeastOne
				case "2":
					qos = QosOnlyOne
				default:
					qos = QosAtMostOne
				}
				err := retry.Retry(func() error {
					return c.sub(split[0], qos, cb)
				}, 3, 2*time.Second)
				if err != nil {
					xlog.Errorf("topic[%s] sub callback register err:%+v.", split[0], err)
				}
			}
		}(key, handler)
		return true
	})
}
