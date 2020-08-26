package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	fiograph "github.com/fioprotocol/fio.etl/graph"
	"github.com/fioprotocol/fio.etl/queue"
	"github.com/fioprotocol/fio.etl/transform"
	"github.com/sasha-s/go-deadlock"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	connected, stopped bool
	// FIXME hard-coded while testing
	sendGraph = true
)

type Consumer struct {
	Seen        uint32 `json:"confirmed"`
	Sent        uint32 `json:"sent"`
	Fetch       int    `json:"fetch"`
	Interactive bool   `json:"interactive"`

	fileName string

	w    http.ResponseWriter
	r    *http.Request
	ws   *websocket.Conn
	last time.Time
	mux  deadlock.Mutex
	wg   sync.WaitGroup

	ctx       context.Context
	cancel    func()
	errs      chan error
	miscChan  chan []byte
	blockChan chan []byte
	txChan    chan []byte
	rowChan   chan []byte
}

func NewConsumer(file string) *Consumer {
	consumer := &Consumer{}
	var isNew bool
	if file == "" {
		file = "chronicle.json"
	}
	func() {
		if f, err := os.OpenFile(file, os.O_RDONLY, 0644); err == nil {
			defer f.Close()
			b, err := ioutil.ReadAll(f)
			if err != nil {
				elog.Println(err)
				isNew = true
				return
			}
			err = json.Unmarshal(b, consumer)
			if err != nil {
				isNew = true
				return
			}
		}
	}()
	if isNew {
		consumer.Fetch = 100
		consumer.last = time.Now()
	}
	consumer.ctx, consumer.cancel = context.WithCancel(context.Background())
	consumer.errs = make(chan error)
	consumer.txChan = make(chan []byte, 1)
	consumer.rowChan = make(chan []byte, 1)
	consumer.miscChan = make(chan []byte, 1)
	consumer.blockChan = make(chan []byte, 1)
	consumer.fileName = file
	return consumer
}

func (c *Consumer) Handler(w http.ResponseWriter, r *http.Request) {
	c.w, c.r = w, r
	if connected {
		c.err()
		return
	}
	connected = true
	defer func() {
		connected = false
	}()
	var upgrader = websocket.Upgrader{
		ReadBufferSize: 8192,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	var err error
	c.ws, err = upgrader.Upgrade(w, r, nil)
	if err != nil {
		c.err()
		return
	}
	defer c.ws.Close()
	ilog.Println("connected")
	go func() {
		e := <-c.errs
		c.cancel()
		dlog.Println("delaying 30s exit on err to allow rate limiting to cool off")
		elog.Println(e)
		time.Sleep(30 * time.Second)
		os.Exit(1)
	}()

	blockQuit := make(chan interface{})
	txQuit := make(chan interface{})
	rowQuit := make(chan interface{})
	miscQuit := make(chan interface{})
	pCtx, pClose := context.WithCancel(context.Background())
	go queue.StartProducer(pCtx, "block", c.blockChan, c.errs, blockQuit)
	go queue.StartProducer(pCtx, "tx", c.txChan, c.errs, txQuit)
	go queue.StartProducer(pCtx, "row", c.rowChan, c.errs, rowQuit)
	go queue.StartProducer(pCtx, "misc", c.miscChan, c.errs, miscQuit)

	panicked := func() {
		stopped = true
		pClose()
		c.cancel()
		time.Sleep(2 * time.Second)
		os.Exit(1)
	}

	fg := &fiograph.Client{}
	fgRowChan := make(chan []byte)
	fgTraceChan := make(chan []byte)
	if sendGraph {
		fg = fiograph.NewClient(fgTraceChan, fgRowChan)
		c := fg.Pool.Get()
		_, err = c.Do("PING")
		if err != nil {
			elog.Println(err)
			panicked()
		}
		c.Close()
	}

	go func() {
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-blockQuit:
				panicked()
			case <-txQuit:
				panicked()
			case <-rowQuit:
				panicked()
			case <-miscQuit:
				panicked()
			}
		}
	}()
	err = c.consume()
	exitCode := 0
	if err != nil {
		exitCode = 1
		elog.Println(err)
	}
	os.Exit(exitCode)
}

type msgSummary struct {
	Msgtype string `json:"msgtype"`
	Data    struct {
		BlockNum       string `json:"block_num"`
		BlockTimestamp string `json:"block_timestamp"`
	} `json:"data"`
}

func (c *Consumer) consume() error {
	alive := time.NewTicker(time.Minute)
	p := message.NewPrinter(language.AmericanEnglish)
	var size uint64
	var t int
	var a, d []byte
	var e error
	var fin transform.BlockFinished
	// deleteme debug:
	var currentMsgs int
	counterChan := make(chan int)

	waitForQueue := func() {
		ilog.Println("waiting up to 180s for queue to empty")
		go func() {
			time.Sleep(180 * time.Second)
			c.cancel()
		}()
		for currentMsgs > 0 {
			time.Sleep(100 * time.Millisecond)
		}
	}

	sizes := make(chan uint64)
	wgMux := deadlock.Mutex{}
	wgAdd := func(i int) {
		wgMux.Lock()
		c.wg.Add(i)
		wgMux.Unlock()
	}
	wgDone := func() {
		wgMux.Lock()
		c.wg.Done()
		wgMux.Unlock()
	}
	fallback := "http://" + os.Getenv("HOST") + ":" + os.Getenv("FALLBACK_PORT")
	go func() {
		for {
			if stopped {
				return
			}
			for currentMsgs > 256 {
				dlog.Println("paused.")
				time.Sleep(2 * time.Second)
			}
			t, d, e = c.ws.ReadMessage()
			if e != nil {
				elog.Println(e)
				_ = c.ws.Close()
				waitForQueue()
				c.cancel()
				return
			}
			if t != websocket.BinaryMessage {
				continue
			}
			c.last = time.Now()
			s := &msgSummary{}
			e = json.Unmarshal(d, s)
			if e != nil {
				elog.Println(e)
				continue
			}
			sizes <- uint64(len(d))
			_ = c.ws.SetReadDeadline(time.Now().Add(time.Minute))
			switch s.Msgtype {
			case "ENCODER_ERROR", "RCVR_PAUSE", "FORK":
				continue
			case "TBL_ROW":
				wgAdd(1)
				go func(d []byte) {
					counterChan <- 1
					defer wgDone()
					a, e := transform.Table(d)
					if e != nil {
						elog.Println("process row:", e)
						counterChan <- -1
						return
					}
					c.rowChan <- a
					counterChan <- -1
				}(d)
			case "BLOCK":
				wgAdd(1)
				go func(data []byte) {
					counterChan <- 1
					defer wgDone()
					a, b, e := transform.Block(data, fallback)
					if e != nil {
						elog.Println(e)
					}
					if a != nil {
						c.blockChan <- a
					}
					if b != nil {
						c.blockChan <- b
					}
					counterChan <- -1
				}(d)
			case "BLOCK_COMPLETED":
				e = json.Unmarshal(d, &fin)
				if e == nil && fin.Data.BlockNum != "" {
					var fb int
					fb, e = strconv.Atoi(fin.Data.BlockNum)
					if e == nil {
						c.Sent = uint32(fb)
					}
				}
			case "PERMISSION", "PERMISSION_LINK", "ACC_METADATA":
				wgAdd(1)
				go func(data []byte, s *msgSummary) {
					counterChan <- 1
					defer wgDone()
					a, e := transform.Account(data, s.Msgtype)
					if e != nil || a == nil {
						counterChan <- -1
						return
					}
					c.miscChan <- a
					counterChan <- -1
				}(d, s)
			case "ABI_UPD":
				// we'll want this one to block for abi updates:
				a, e = transform.Abi(d)
				if e != nil {
					elog.Println(e)
					continue
				}
				c.miscChan <- a
			case "TX_TRACE":
				wgAdd(1)
				go func(data []byte) {
					counterChan <- 1
					defer wgDone()
					a, e := transform.Trace(data)
					if e != nil || a == nil {
						counterChan <- -1
						return
					}
					c.txChan <- a
					counterChan <- -1
				}(d)
			}
			d = nil
		}
	}()

	go func() {
		printStat := time.NewTicker(5 * time.Second)
		t := time.NewTicker(500 * time.Millisecond)
		var err error
		for {
			select {
			case <-c.ctx.Done():
				return
			case <-printStat.C:
				dlog.Println(p.Sprintf("Block: %d, processed %d MiB", c.Seen, size/1024/1024))
				if currentMsgs > 0 {
					dlog.Println(p.Sprintf("%d  routines are waiting for buffer", currentMsgs))
				}
			case s := <-sizes:
				size += s
			case m := <-counterChan:
				currentMsgs += m
			case <-c.ctx.Done():
				return
			case <-t.C:
				if c.Sent > c.Seen {
					c.Seen = c.Sent
					err = c.ack()
					if err != nil {
						elog.Println(err)
					}
				}
			}
		}
	}()

	memStats := &runtime.MemStats{}
	var finalErr error
	for {
		select {
		case <-c.ctx.Done():
			stopped = true
			ilog.Println("consumer cleaning up")
			c.wg.Wait()
			ilog.Println("consumer exiting")
			runtime.GC()
			_ = c.ws.SetReadDeadline(time.Now().Add(-1 * time.Second))
			return finalErr
		case <-alive.C:
			// check if we aren't getting messages
			if c.last.Before(time.Now().Add(-1*time.Minute)) && currentMsgs == 0 {
				_ = c.ws.SetReadDeadline(time.Now().Add(-1 * time.Second))
				waitForQueue()
				c.cancel()
				finalErr = errors.New("no data for > 1 minute, closing")
			}
			// if we are taking more than 4gb of RAM, we should probably restart.
			runtime.ReadMemStats(memStats)
			if memStats.HeapInuse > 4*1024*1024*1024 {
				stopped = true
				elog.Println("Exceeded 4gb heap, clearing existing queue")
				waitForQueue()
				ilog.Println("cleared queue, restarting.")
				c.cancel()
				_ = c.ws.SetReadDeadline(time.Now().Add(-1 * time.Second))
			}
		}
	}
}

func (c *Consumer) err() {
	c.r.Body.Close()
	c.w.WriteHeader(500)
}

func (c *Consumer) save() error {
	f, err := os.OpenFile(c.fileName, os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	j, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	_, err = f.Write(j)
	return err
}

func (c *Consumer) ack() error {
	// always return -256 of what has been seen, this is the max number of blocked routines allowed.
	if c.Seen <= 256 {
		return nil
	}
	return c.ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d", c.Seen-256)))
}

func (c *Consumer) request(start uint32, end uint32) error {
	if !c.Interactive {
		return errors.New("must be interactive to request blocks")
	}
	if start > end {
		return errors.New("invalid request range")
	}
	return c.ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d-%d", start, end)))
}
