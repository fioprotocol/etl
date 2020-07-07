package transform

import (
	l "github.com/dapixio/fio.etl/logging"
	"log"
)

var (
	abis *abiMap
	elog *log.Logger
)

func init()  {
	var err error
	abis, err = newAbiMap()
	if err != nil {
		log.Fatal("building abi map: ", err)
	}
	elog, _, _ = l.Setup("[fioetl-transform] ")
}

