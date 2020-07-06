package main

import (
	"context"
	"github.com/dapixio/fio.etl/kafka"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	log.SetPrefix(" [fioetl-producer] ")
	log.SetFlags(log.LstdFlags|log.Lshortfile|log.Lmsgprefix)

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	kQuit := make(chan interface{})
	errs := make(chan error)

	go func() {
		kafka.StartProducers(ctx, errs, kQuit)
		wg.Done()
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)
	for {
		select {
		case s := <-sigc:
			log.Println("exiting on ", s)
			cancel()
			go func() {
				<-time.After(5*time.Second)
				os.Exit(2)
			}()
			wg.Wait()
			os.Exit(0)
		case e := <-errs:
			log.Println(e)
			cancel()
			go func() {
				<-time.After(5*time.Second)
				os.Exit(2)
			}()
			wg.Wait()
			os.Exit(1)
		case <-kQuit:
			log.Println("kafka producers exited, quitting")
			os.Exit(0)
		}
	}
}
