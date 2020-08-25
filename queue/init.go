package queue

import (
	"github.com/fioprotocol/fio.etl/logging"
	"log"
)

var (
	elog *log.Logger
	dlog *log.Logger
)

func init() {
	elog, _, dlog = logging.Setup("[fioetl-queue] ")
}
