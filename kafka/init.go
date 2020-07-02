package kafka

import (
	"github.com/Shopify/sarama"
	"log"
	"os"
)

func init()  {
	if os.Getenv("DEBUG") == "true" {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.Lshortfile|log.LstdFlags)
	}
}
