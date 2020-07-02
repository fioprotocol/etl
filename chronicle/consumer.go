package chronicle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/dapixio/fio.etl/kafka"
	"github.com/dapixio/fio.etl/transform"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	connected bool
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
	mux  sync.Mutex

	ctx       context.Context
	cancel    func()
	errs      chan error
	sent      chan uint32
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
				log.Println(err)
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
	consumer.sent = make(chan uint32)
	consumer.txChan = make(chan []byte)
	consumer.rowChan = make(chan []byte)
	consumer.miscChan = make(chan []byte)
	consumer.blockChan = make(chan []byte)
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
	log.Println("connected")
	go func() {
		e := <-c.errs
		c.cancel()
		log.Println("delaying 30s exit on err to allow rate limiting to cool off")
		log.Println(e)
		time.Sleep(30 * time.Second)
		os.Exit(1)
	}()

	go kafka.Setup(c.ctx, c.blockChan, c.txChan, c.rowChan, c.miscChan, c.sent, c.errs)
	err = c.consume()
	if err != nil {
		log.Println(err)
	}
}

type msgSummary struct {
	Msgtype string `json:"msgtype"`
	Data    struct {
		BlockNum       string `json:"block_num"`
		BlockTimestamp string `json:"block_timestamp"`
	} `json:"data"`
}

func (c *Consumer) consume() error {
	go func() {
		// track highest successfully sent, never ack higher.
		for {
			c.Sent = <-c.sent
		}
	}()
	alive := time.NewTicker(time.Minute)
	exit := make(chan interface{}, 1)
	//types := make(map[string]int)
	p := message.NewPrinter(language.AmericanEnglish)
	var size uint64
	var t int
	var d []byte
	var e error
	var fin transform.BlockFinished
	go func() {
		for {
			t, d, e = c.ws.ReadMessage()
			if e != nil {
				log.Println(e)
				_ = c.ws.Close()
				close(exit)
				return
			}
			if t != websocket.BinaryMessage {
				continue
			}
			c.last = time.Now()
			s := &msgSummary{}
			err := json.Unmarshal(d, s)
			if err != nil {
				log.Println(err)
				continue
			}
			//types[s.Msgtype] += 1
			size += uint64(len(d))
			_ = c.ws.SetReadDeadline(time.Now().Add(time.Minute))
			switch s.Msgtype {
			case "BLOCK_COMPLETED":
				err = json.Unmarshal(d, &fin)
				if err == nil && fin.Data.BlockNum != "" {
					var fb int
					fb, err = strconv.Atoi(fin.Data.BlockNum)
					if err == nil {
						c.Sent = uint32(fb)
					}
				}
			case "ENCODER_ERROR", "RCVR_PAUSE", "FORK":
				continue
			case "TBL_ROW":
				go func(d []byte) {
					out, e := transform.Table(d)
					if e != nil {
						log.Println("process row:", e)
						return
					}
					c.rowChan <- out
				}(d)
			case "BLOCK":
				go func(data []byte) {
					h, sum, e := transform.Block(data)
					if e != nil {
						log.Println(e)
					}
					if h != nil {
						c.blockChan <- h
					}
					if sum != nil {
						c.blockChan <- sum
					}
					if c.Seen == 0 {

					}
				}(d)
			case "PERMISSION", "PERMISSION_LINK", "ACC_METADATA":
				go func(data []byte, s *msgSummary) {
					a, e := transform.Account(data, s.Msgtype)
					if e != nil || a == nil {
						return
					}
					c.miscChan <- a
				}(d, s)
			case "ABI_UPD":
				// we'll want this one to block for abi updates:
				a, err := transform.Abi(d)
				if err != nil {
					log.Println(err)
					continue
				}
				c.miscChan <- a
			case "TX_TRACE":
				go func(data []byte) {
					t, e := transform.Trace(data)
					if e != nil || t == nil {
						return
					}
					c.txChan <- t
				}(d)
			}
			d = nil
		}
	}()

	go func() {
		for {
			time.Sleep(5 * time.Second)
			log.Println(p.Sprintf("Block: %d, processed %d MiB", c.Seen, size/1024/1024))
		}
	}()
	go func() {
		var err error
		for {
			time.Sleep(500 * time.Millisecond)
			if c.Sent > c.Seen {
				c.Seen = c.Sent
				err = c.ack()
				if err != nil {
					log.Println(err)
				}
			}
		}
	}()

	for {
		select {
		case <-c.ctx.Done():
			return nil
		case <-exit:
			return nil
		case <-alive.C:
			if c.last.Before(time.Now().Add(-1 * time.Minute)) {
				_ = c.ws.SetReadDeadline(time.Now().Add(-1 * time.Second))
				return errors.New("no data for > 1 minute, closing")
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
	return c.ws.WriteMessage(websocket.TextMessage, []byte(fmt.Sprintf("%d", c.Seen)))
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
