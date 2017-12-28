package observer

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/kakilangit/validator"
	"github.com/streadway/amqp"
)

//ConsumerName const
const ConsumerName = ""

type (
	//Observer interface
	Observer interface {
		Dial() (*amqp.Connection, error)
		Open(conn *amqp.Connection) (*amqp.Channel, error)
		Subscribe(ch *amqp.Channel, model, exchange, queue, topic string) (<-chan amqp.Delivery, error)
		Timeout() time.Duration
	}

	//Object struct, access this struct through Observer
	Object struct {
		host, user, password string
		port                 int
		timeout              int
		tlsConfig            *tls.Config
	}

	Option func(*Object) error
)

//New is the new Object
func New(host, user, password string, port, timeout int) *Object {
	return &Object{
		host:     host,
		user:     user,
		password: password,
		port:     port,
		timeout:  timeout,
	}
}

//WithTLSConfig configuration for TLS
func WithTLSConfig(tlsConfig *tls.Config) Option {
	return func(obs *Object) error {
		if obs == nil {
			errors.New("missing observer")
		}
		obs.tlsConfig = tlsConfig
		return nil
	}
}

//Set Option for Object
func (obs *Object) Set(opts ...Option) *Object {
	for _, fn := range opts {
		if e := fn(obs); e != nil {
			panic(e)
		}
	}

	return obs
}

//Available ...
func (obs *Object) Available() bool {
	if e := validator.Exist(obs.host, obs.user, obs.password); e != nil {
		return false
	}
	if e := validator.Positive(obs.port); e != nil {
		return false
	}
	return true
}

//Dial pubsub
func (obs *Object) Dial() (*amqp.Connection, error) {
	if !obs.Available() {
		return nil, errors.New("Observer not available")
	}

	if obs.tlsConfig != nil {
		connect := fmt.Sprintf("amqps://%s:%s@%s:%d/", obs.user, obs.password, obs.host, obs.port)
		return amqp.DialTLS(connect, obs.tlsConfig)
	}

	connect := fmt.Sprintf("amqp://%s:%s@%s:%d/", obs.user, obs.password, obs.host, obs.port)
	return amqp.Dial(connect)
}

//Open pubsub
func (obs *Object) Open(conn *amqp.Connection) (*amqp.Channel, error) {
	if !obs.Available() {
		return nil, errors.New("Observer not available")
	}
	return conn.Channel()
}

//Subscribe ...
func (obs *Object) Subscribe(ch *amqp.Channel, model, exchange, queue, topic string) (<-chan amqp.Delivery, error) {
	if !obs.Available() {
		return nil, errors.New("Observer not available")
	}

	err := ch.ExchangeDeclare(
		exchange, // name
		model,    // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // no-wait
		nil,      // arguments
	)
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		queue, // name
		false, // durable
		false, // delete when usused
		true,  // exclusive
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	err = ch.QueueBind(
		q.Name,   // queue name
		topic,    // routing key
		exchange, // exchange
		false,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return ch.Consume(
		q.Name,       // queue
		ConsumerName, // consumer
		true,         // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
}

//Timeout ...
func (obs *Object) Timeout() time.Duration {
	return time.Duration(obs.timeout)
}
