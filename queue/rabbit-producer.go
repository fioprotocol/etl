package queue

import (
	"context"
	"errors"
	"fmt"
	"github.com/streadway/amqp"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"time"
)

// StartProducer sets up the connection to the message queue, and publishes to a channel when a message arrives over
// the messages channel receives a message.
func StartProducer(ctx context.Context, channel string, messages chan []byte, errs chan error, quit chan interface{}) {
	exitOn := func(err error) bool {
		if err != nil {
			elog.Println(channel, "- rabbit producer: ", err)
			close(quit)
			return true
		}
		return false
	}

	defer func() {
		if r := recover(); r != nil {
			elog.Println("panic in ", channel, r)
			errs <- errors.New(fmt.Sprintf("%v", r))
			close(quit)
		}
	}()

	conn, err := amqp.Dial(rabbitUrl)
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

	printTick := time.NewTicker(30 * time.Second)
	var sent uint64
	p := message.NewPrinter(language.AmericanEnglish)
	for {
		select {
		case <-ctx.Done():
			close(quit)
			return
		case <-printTick.C:
			dlog.Println(p.Sprintf("%8s : sent total of %d messages", channel, sent))
		case d := <-messages:
			if d == nil || len(d) == 0 {
				continue
			}
			err = ch.Publish(
				"",
				q.Name,
				false,
				false,
				amqp.Publishing{
					ContentType: "application/octet-stream",
					Body:        d,
				},
			)
			if exitOn(err) {
				return
			}
			sent += 1
		}
	}
}
