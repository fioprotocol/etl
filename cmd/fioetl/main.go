package main

import (
	"github.com/dapixio/fio.etl/chronicle"
	"github.com/dapixio/fio.etl/logging"
	"github.com/gorilla/mux"
	"net/http"
)

/*
fioetl accepts messages from a chronicle daemon, applies normalization, and sends to a message queue for consumption
*/

func main() {
	elog, ilog, _ := logging.Setup(" [fioetl-consumer] ")
	ilog.Println("fioetl starting")

	c := chronicle.NewConsumer("")
	router := mux.NewRouter()
	router.HandleFunc("/chronicle", c.Handler)
	elog.Fatal(http.ListenAndServe(":8844", router))
}
