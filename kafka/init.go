package kafka

import (
	"github.com/Shopify/sarama"
	"github.com/dapixio/fio.etl/logging"
	"log"
	"os"
)

var (
	elog *log.Logger
	ilog *log.Logger
	dlog *log.Logger
)

func init() {
	if os.Getenv("DEBUG") == "true" {
		sarama.Logger = log.New(os.Stderr, "[sarama] ", log.Lshortfile|log.LstdFlags)
	}
	elog, ilog, dlog = logging.Setup("[fioetl-kafka] ")
}
