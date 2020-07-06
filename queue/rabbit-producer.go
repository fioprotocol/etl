package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
)

func StartProducer(ctx context.Context, channel string, messages chan []byte, errs chan error, quit chan interface{}) {
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
			errs <- errors.New(fmt.Sprintf("%v", r))
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

	mux := sync.Mutex{}
	for {
		select {
		case <-ctx.Done():
			close(quit)
			return
		case d := <-messages:
			if d == nil || len(d) == 0 {
				continue
			}
			mux.Lock()
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
			mux.Unlock()
		}
	}
}
