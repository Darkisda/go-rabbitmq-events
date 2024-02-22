package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/darkisda/go-rabbitmq/internal"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("luan", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//RPC
	consumeConn, err := internal.ConnectRabbitMQ("luan", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer consumeConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// RPC
	consumeClient, err := internal.NewRabbitMQClient(consumeConn)
	if err != nil {
		panic(err)
	}
	defer consumeClient.Close()

	queue, err := consumeClient.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}
	if err := consumeClient.CreateBinding(queue.Name, queue.Name, "customers_callback"); err != nil {
		panic(err)
	}
	messageBus, err := consumeClient.Consume(queue.Name, "customer_api", true)
	if err != nil {
		panic(err)
	}

	go func() {
		for message := range messageBus {
			log.Printf("Message Callback %s", message.CorrelationId)
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5 * time.Second)
	defer cancel()
	
	for i:= 0; i<10; i++ {
		if err := client.Send(ctx, "customers_event", "customers.created.us", amqp091.Publishing{
			ContentType: "text/plain",
			DeliveryMode: amqp091.Persistent,
			Body: []byte("An cool message between services"),
			ReplyTo: queue.Name,
			CorrelationId: fmt.Sprintf("customer_created_%d", i),
		}); err != nil {
			panic(err)
		}

	}
	var blocking chan struct{}
	<-blocking
}