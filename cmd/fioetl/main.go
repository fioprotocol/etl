package main

import (
	"github.com/dapixio/fio.etl/chronicle"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func main() {
	log.Println("fioetl starting")

	c := chronicle.NewConsumer("")
	//if err != nil {
	//	log.Fatal(err)
	//}
	router := mux.NewRouter()
	router.HandleFunc("/chronicle", c.Handler)
	log.Fatal(http.ListenAndServe(":8844", router))
}
