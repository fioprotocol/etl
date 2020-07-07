package main

import (
	"context"
	"github.com/dapixio/fio.etl/kafka"
	"github.com/dapixio/fio.etl/logging"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func main() {
	elog, ilog, _ := logging.Setup(" [fioetl-publisher] ")
	ilog.Println("starting")

	wg := sync.WaitGroup{}
	wg.Add(1)
	ctx, cancel := context.WithCancel(context.Background())
	kQuit := make(chan interface{})
	errs := make(chan error)

	go func() {
		kafka.StartProducers(ctx, errs, kQuit)
		wg.Done()
		ilog.Println("kafka publishers have exited")
	}()

	sigc := make(chan os.Signal, 1)
	signal.Notify(sigc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT,
	)

	for {
		select {
		case s := <-sigc:
			elog.Println("exiting on signal ", s)
			cancel()
			go func() {
				<-time.After(5 * time.Second)
				os.Exit(2)
			}()
			wg.Wait()
			os.Exit(0)
		case e := <-errs:
			elog.Println(e)
			cancel()
			go func() {
				<-time.After(5 * time.Second)
				os.Exit(2)
			}()
			wg.Wait()
			os.Exit(1)
		case <-kQuit:
			ilog.Println("kafka producers exited, quitting")
			os.Exit(0)
		}
	}
}
