package rabbitmq

import (
	"errors"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ChannelManager interface {
	getChannel() *amqp.Channel
	onChannelRestart() chan error
	listenToRestart()
	listenToFlow()
	listenToBlockConnection()
	checkFlow() error
	checkBlockConnection() error
	reconnect()
	close() error
}

type channelManager struct {
	logger     Logger
	url        string
	channel    *amqp.Channel
	connection *amqp.Connection

	channelMutex      *sync.RWMutex
	channelRestart    chan error
	reconnectInterval time.Duration

	flowPause bool
	flowMutex *sync.RWMutex

	blockedConn    bool
	blockConnMutex *sync.RWMutex
}

func (c *channelManager) getChannel() *amqp.Channel {
	c.channelMutex.RLock()
	defer c.channelMutex.RUnlock()
	return c.channel
}

func (c *channelManager) onChannelRestart() chan error {
	return c.channelRestart
}

// listenToRestart listens on the channel's cancel or close notify.
// When it detects a problem, it attempts to reconnect.
// Once reconnect done, it sends an error back on channelRestart channel.
func (c *channelManager) listenToRestart() {
	notifyClose := c.channel.NotifyClose(make(chan *amqp.Error))
	notifyCancel := c.channel.NotifyCancel(make(chan string))
	select {
	case err := <-notifyClose:
		if err != nil {
			c.logger.Error("attempting to reconnect to amqp server after close with error: %v", err)
			c.reconnect()
			c.logger.Warn("successfully reconnected to amqp server after close")
			c.channelRestart <- err
		} else {
			c.logger.Info("amqp channel closed gracefully")
		}
	case err := <-notifyCancel:
		c.logger.Error("attempting to reconnect to amqp server after cancel with error: %s", err)
		c.reconnect()
		c.logger.Warn("successfully reconnected to amqp server after cancel")
		c.channelRestart <- errors.New(err)
	}
}

func (c *channelManager) listenToFlow() {
	notifyFlow := c.channel.NotifyFlow(make(chan bool))
	c.flowMutex.Lock()
	c.flowPause = false
	c.flowMutex.Unlock()

	for ok := range notifyFlow {
		c.flowMutex.Lock()
		if ok {
			c.logger.Warn("pausing publishing due to flow request from server")
			c.flowPause = true
		} else {
			c.flowPause = false
			c.logger.Warn("resuming publishing due to flow request from server")
		}
		c.flowMutex.Unlock()
	}
}

func (c *channelManager) checkFlow() error {
	c.flowMutex.RLock()
	defer c.flowMutex.RUnlock()
	if c.flowPause {
		return errors.New("publishing blocked due to high flow on the server")
	}
	return nil
}

func (c *channelManager) listenToBlockConnection() {
	notifyBlocked := c.connection.NotifyBlocked(make(chan amqp.Blocking))
	c.blockConnMutex.Lock()
	c.blockedConn = false
	c.blockConnMutex.Unlock()

	for b := range notifyBlocked {
		c.blockConnMutex.Lock()
		if b.Active {
			c.logger.Warn("pausing publishing due to TCP blocking from server")
			c.blockedConn = true
		} else {
			c.blockedConn = false
			c.logger.Warn("resuming publishing due to TCP blocking from server")
		}
		c.blockConnMutex.Unlock()
	}
}

func (c *channelManager) checkBlockConnection() error {
	c.blockConnMutex.RLock()
	defer c.blockConnMutex.RUnlock()
	if c.blockedConn {
		return errors.New("publishing blocked due to TCP block on the server")
	}
	return nil
}

// reconnect safely closes the current channel and connection and obtains a new solution
func (c *channelManager) reconnect() {
	c.channelMutex.Lock()
	defer c.channelMutex.Unlock()
	for {
		c.logger.Info("waiting %s seconds to attempt to reconnect to amqp server", c.reconnectInterval)
		time.Sleep(c.reconnectInterval)

		newConn, newChannel, err := getNewChannel(c.url)
		if err != nil {
			c.logger.Error("error reconnecting to amqp server: %v", err)
		} else {
			c.channel.Close()
			c.connection.Close()
			c.connection = newConn
			c.channel = newChannel
			go c.listenToRestart()
			go c.listenToFlow()
			go c.listenToBlockConnection()
			break
		}
	}
}

// close safely closes the current channel and connection
func (c *channelManager) close() error {
	c.channelMutex.Lock()
	defer c.channelMutex.Unlock()

	err := c.channel.Close()
	if err != nil {
		return err
	}

	err = c.connection.Close()
	if err != nil {
		return err
	}
	return nil
}

func getNewChannel(url string) (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, nil, err
	}
	return conn, ch, nil
}

func newChannelManager(url string, log Logger, reconnectInterval time.Duration) (ChannelManager, error) {
	conn, ch, err := getNewChannel(url)
	if err != nil {
		return nil, err
	}
	chManager := channelManager{
		logger:            log,
		url:               url,
		channel:           ch,
		connection:        conn,
		channelMutex:      &sync.RWMutex{},
		channelRestart:    make(chan error),
		reconnectInterval: reconnectInterval,
		flowPause:         false,
		flowMutex:         &sync.RWMutex{},
		blockedConn:       false,
		blockConnMutex:    &sync.RWMutex{},
	}
	go chManager.listenToRestart()
	go chManager.listenToFlow()
	go chManager.listenToBlockConnection()
	return &chManager, nil
}
