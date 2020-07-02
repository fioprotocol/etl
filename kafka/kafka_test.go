package kafka

import (
	"fmt"
	"github.com/Shopify/sarama"
	"testing"
)

func TestKafka(t *testing.T) {
	brokerList := []string{"fioetl.servicebus.windows.net:9093"}
	producer, err := sarama.NewSyncProducer(brokerList, getConfig())
	if err != nil {
		t.Error("Failed to start Sarama producer:", err)
		return
	}
	defer producer.Close()

	p, o, err := producer.SendMessage(&sarama.ProducerMessage{
		Topic:     "test",
		Key:       sarama.ByteEncoder([]byte("hello")),
		Value:     sarama.ByteEncoder([]byte("there")),
	})
	if err != nil {
		t.Error("failed to write", err)
		return
	}
	fmt.Println(p, o)

}
