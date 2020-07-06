package queue

import (
	"context"
	"github.com/streadway/amqp"
	"log"
)

func StartProducer(ctx context.Context, channel string, messages chan []byte, quit chan interface{}) {
	exitOn := func(err error) bool {
		if err != nil {
			log.Println(channel, "- rabbit producer: ", err)
			close(quit)
			return true
		}
		return false
	}

	defer func() {
		if r := recover(); r != nil {
			log.Println("panic in ", channel, r)
			close(quit)
		}
	}()

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

	for {
		select {
		case <-ctx.Done():
			close(quit)
			return
		case d := <-messages:
			if d == nil || len(d) == 0 {
				continue
			}
			err = ch.Publish(
				"",
				q.Name,
				false,
				false,
				amqp.Publishing {
					ContentType: "application/octet-stream",
					Body:        d,
				},
			)
			if exitOn(err) {
				return
			}
		}
	}
}
