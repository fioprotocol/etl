package queue

import (
	"github.com/fioprotocol/fio.etl/logging"
	"log"
	"os"
)

var (
	elog *log.Logger
	dlog *log.Logger
	rabbitUrl string
)

func init() {
	elog, _, dlog = logging.Setup("[fioetl-queue] ")
	rabbitUrl = os.Getenv("RABBIT")
	if rabbitUrl == "" {
		rabbitUrl = "amqp://guest:guest@rabbit:5672/"
	}
}
