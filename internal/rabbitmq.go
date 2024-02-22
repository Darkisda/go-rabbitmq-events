package internal

import (
	"context"
	"fmt"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitClient struct {
	// Conexão utilizada pelo client
	conn *amqp.Connection
	// Channel é utilizado para processar ou enviar mensagens
	ch *amqp.Channel
}

func ConnectRabbitMQ(username, password, host, vHost string) (*amqp.Connection, error) {
	return amqp.Dial(fmt.Sprintf("amqp://%s:%s@%s/%s", username, password, host, vHost))
}

func NewRabbitMQClient(conn *amqp.Connection) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}
	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}
	return RabbitClient{
		conn: conn,
		ch: ch,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

func (rc RabbitClient) CreateQueue(queueName string, durable, autoDelete bool) (amqp.Queue, error) {
	q, err := rc.ch.QueueDeclare(queueName, durable, autoDelete, false, false, nil)
	if err != nil {
		return amqp.Queue{}, nil
	}

	return q, err
}

func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

// Mandatory é usado para determinar se um erro vai ser retornado caso a mensagem falhe
func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, true, false, options)
	if err != nil {
		return err;
	}
	log.Println(confirmation.Wait())
	return nil
}

// Consumer é usado para identificar qual service em específico consumiu aquela mensagem
func (rc RabbitClient) Consume(queueName, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queueName, consumer, autoAck, false, false, false, nil)
}

func (rc RabbitClient) ApplyQos(count, size int, global bool) error {
	return rc.ch.Qos(count, size, global)
}