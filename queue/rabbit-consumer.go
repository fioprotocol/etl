package queue

/*
	using a rabbitmq to ensure that we have a durable store for messages, this helps prevent data loss.
 */

import (
	"context"
	"github.com/streadway/amqp"
)

func StartConsumer(ctx context.Context, channel string, messages chan []byte, errs chan error, quit chan interface{}) {
	ilog.Println(channel, "consumer starting")
	defer func() {
		ilog.Println(channel, "consumer exiting")
	}()
	exitOn := func(err error) bool {
		if err != nil {
			elog.Println(channel, " rabbit consumer: ", err)
			close(quit)
			errs <-err
			return true
		}
		return false
	}

	conn, err := amqp.Dial("amqp://guest:guest@rabbit:5672/")
	if exitOn(err) {
		return
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if exitOn(err) {
		return
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		channel,
		true,
		false,
		false,
		false,
		nil,
	)
	if exitOn(err) {
		return
	}

	msgs, err := ch.Consume(
		q.Name,
		channel+"-consumer",
		true,
		false,
		false,
		false,
		nil,
	)
	if exitOn(err) {
		return
	}

	for {
		select {
		case d := <-msgs:
			// messages should be a buffered channel, matching our worker count to prevent data loss.
			messages <-d.Body
		case <-ctx.Done():
			close(quit)
			return
		}
	}
}
