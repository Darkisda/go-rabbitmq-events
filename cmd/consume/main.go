package main

import (
	"context"
	"log"
	"time"

	"github.com/darkisda/go-rabbitmq/internal"
	"github.com/rabbitmq/amqp091-go"
	"golang.org/x/sync/errgroup"
)

func main() {
	conn, err := internal.ConnectRabbitMQ("luan", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	//RPC
	publishConn, err := internal.ConnectRabbitMQ("luan", "secret", "localhost:5672", "customers")
	if err != nil {
		panic(err)
	}
	defer publishConn.Close()

	client, err := internal.NewRabbitMQClient(conn)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	//RPC
	publishClient, err := internal.NewRabbitMQClient(publishConn)
	if err != nil {
		panic(err)
	}
	defer publishClient.Close()

	queue, err := client.CreateQueue("", true, true)
	if err != nil {
		panic(err)
	}
	if err := client.CreateBinding(queue.Name, "", "customers_event"); err != nil {
		panic(err)
	}
	messageBus, err := client.Consume(queue.Name, "email_service", false)
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	var blocking chan struct{}
	ctx, cancel := context.WithTimeout(ctx, 15 * time.Second)
	defer cancel()
	
	g, _ := errgroup.WithContext(ctx)
	if err := client.ApplyQos(10, 0, true); err != nil {
		panic(err)
	}
	g.SetLimit(10)

	go func() {
		for message := range messageBus	 {
			msg := message
			g.Go(func () error {
				log.Printf("New Message: %v \n", msg)
				time.Sleep(10 *time.Second)
				if err := msg.Ack(false); err != nil {
					log.Panicln("Ack message failed")
					return err
				}
				if err := publishClient.Send(ctx, "customers_callback", msg.ReplyTo, amqp091.Publishing{
					ContentType: "text/plan",
					DeliveryMode: amqp091.Persistent,
					Body: []byte("RPC Completed"),
					CorrelationId: msg.CorrelationId,
				}); err != nil {
					log.Println("Failed to RPC")
					return err
				}
				log.Printf("Acknowledged message: %s \n", msg.MessageId)
				return nil
			})
		}
	}()
	log.Println("Consuming, to close the program press CTRL + C")
	<-blocking
}