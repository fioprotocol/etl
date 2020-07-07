package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"github.com/Shopify/sarama"
	"github.com/dapixio/fio.etl/queue"
	"github.com/sasha-s/go-deadlock"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"os"
	"sync"
	"time"
)

type Settings struct {
	BrokerList []string `yaml:"broker_list"`
	SaslUser string `yaml:"sasl_user"`
	SaslPassword string `yaml:"sasl_password"`
}

var brokerList []string

func getConfig() *sarama.Config {
	// TODO: make this a command line option:
	f, err := os.OpenFile("auth.yml", os.O_RDONLY, 0644)
	if err != nil {
		log.Println("Please ensure auth.yml is present and contains connection information")
		log.Fatal(err)
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		log.Fatal(err)
	}
	auth := &Settings{}
	err = yaml.Unmarshal(b, auth)
	if err != nil {
		log.Fatal(err)
	}
	config := sarama.NewConfig()
	config.Net.DialTimeout = 10 * time.Second

	config.Net.SASL.Enable = true
	config.Net.SASL.User = auth.SaslUser
	config.Net.SASL.Password = auth.SaslPassword
	brokerList = auth.BrokerList
	config.Net.SASL.Mechanism = "PLAIN"

	config.Net.TLS.Enable = true
	config.Net.TLS.Config = &tls.Config{
		InsecureSkipVerify: true,
		ClientAuth:         0,
	}
	config.Version = sarama.V2_0_0_0
	config.ClientID = `fio.etl`
	//config.Producer.Flush.Frequency = 500 * time.Millisecond
	config.Producer.Flush.MaxMessages = 20
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Retry.Max = 10
	config.Producer.Retry.Backoff = time.Second
	return config
}

type postProcessing struct {
	BlockNum uint32 `json:"block_num"`
}

type pChan struct {
	payload []byte
	topic   string
}

func StartProducers(ctx context.Context, errs chan error, done chan interface{}) {

	cCtx, cCancel := context.WithCancel(context.Background())
	blockChan := make(chan []byte, 1)
	txChan := make(chan []byte, 1)
	rowChan := make(chan []byte, 1)
	miscChan := make(chan []byte, 1)

	blockQuit := make(chan interface{})
	txQuit := make(chan interface{})
	rowQuit := make(chan interface{})
	miscQuit := make(chan interface{})
	go queue.StartConsumer(cCtx, "block", blockChan, errs, blockQuit)
	go queue.StartConsumer(cCtx, "tx", txChan, errs, txQuit)
	go queue.StartConsumer(cCtx, "row", rowChan, errs, rowQuit)
	go queue.StartConsumer(cCtx, "misc", miscChan, errs, miscQuit)

	iwg := sync.WaitGroup{}
	mux := deadlock.Mutex{}
	send := func(pc *pChan, producer sarama.AsyncProducer) {
		if pc.payload == nil || producer == nil {
			return
		}
		mux.Lock()
		defer mux.Unlock()
		b := bytes.NewBuffer(nil)
		gz := gzip.NewWriter(b)
		_, err := gz.Write(pc.payload)
		if err != nil {
			log.Println(err)
			return
		}
		_ = gz.Close()

		producer.Input() <- &sarama.ProducerMessage{
			Topic: pc.topic,
			Value: sarama.ByteEncoder(b.Bytes()),
		}
	}

	cfgMux := deadlock.Mutex{}
	publisher := func(c chan *pChan) {
		defer iwg.Done()
		cfgMux.Lock()
		producer, err := sarama.NewAsyncProducer(brokerList, getConfig())
		if err != nil {
			errs <- err
			cfgMux.Unlock()
			return
		}
		cfgMux.Unlock()
		defer producer.AsyncClose()
		go func() {
			for {
				select {
				case err := <-producer.Errors():
					if err != nil {
						errs <- err
					}
					return
				case <-ctx.Done():
					return
				}
			}
		}()
		for {
			select {
			case <-cCtx.Done():
				log.Println("kafka producer exiting, upstream consumer exited")
				return
			case <-ctx.Done():
				log.Println("kafka producer exiting")
				return
			case msg := <-c:
				send(msg, producer)
			}
		}
	}
	const workers int = 4
	iwg.Add(workers)
	c := make(chan *pChan, 1)
	for i := 0; i < workers; i++ {
		go publisher(c)
	}

	printTick := time.NewTicker(30*time.Second)
	p := message.NewPrinter(language.AmericanEnglish)
	var sentRow, sentBlock, sentTx, sentMisc uint64
	for {
		select {
		case r := <-rowChan:
			// use a closure to dereference
			sentRow += 1
			func(d []byte){
				c <- &pChan{
					payload: d,
					topic: "row",
				}
			}(r)
		case header := <-blockChan:
			sentBlock += 1
			func(d []byte){
				c <- &pChan{
					payload: d,
					topic: "block",
				}
			}(header)
		case tx := <-txChan:
			sentTx += 1
			func(d []byte){
				c <- &pChan{
					payload: d,
					topic: "tx",
				}
			}(tx)
		case account := <-miscChan:
			sentMisc += 1
			func(d []byte){
				c <- &pChan{
					payload: d,
					topic: "misc",
				}
			}(account)
		case <-ctx.Done():
			cCancel()
			iwg.Wait()
			log.Println("kafka workers exited")
			close(done)
			return
		case <-printTick.C:
			log.Println(p.Sprintf("kafka publisher has sent: block %d, row %d, tx %d, misc %d", sentBlock, sentRow, sentTx, sentMisc))
		case <-blockQuit:
			cCancel()
		case <-rowQuit:
			cCancel()
		case <-txQuit:
			cCancel()
		case <-miscQuit:
			cCancel()
		}
	}
}
