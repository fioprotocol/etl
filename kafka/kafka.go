package kafka

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"
	"github.com/Shopify/sarama"
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

func Setup(ctx context.Context, headerChan chan []byte, txChan chan []byte, rowChan chan []byte,
	miscChan chan []byte, errs chan error, done chan interface{}) {

	var pause bool
	iwg := sync.WaitGroup{}
	send := func(payload []byte, channel string, producer sarama.AsyncProducer) {
		if payload == nil || producer == nil {
			return
		}
		l := len(payload)
		b := bytes.NewBuffer(nil)
		gz := gzip.NewWriter(b)
		if l != len(payload) {
			log.Println("payload size changed during processing, this is weird and shouldn't happen")
			return
		}
		_, err := gz.Write(payload)
		if err != nil {
			log.Println(err)
			return
		}
		_ = gz.Close()

		producer.Input() <- &sarama.ProducerMessage{
			Topic: channel,
			Value: sarama.ByteEncoder(b.Bytes()),
		}
		b = nil
	}

	publisher := func(c chan *pChan) {
		defer iwg.Done()
		producer, err := sarama.NewAsyncProducer(brokerList, getConfig())
		if err != nil {
			errs <- err
			return
		}
		defer producer.AsyncClose()
		go func() {
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-producer.Errors():
					if err != nil {
						errs <- err
					}
					return
				}
			}
		}()
		for {
			select {
			case <-ctx.Done():
				log.Println("kafka producer exiting")
				return
			case msg := <-c:
				for pause {
					time.Sleep(100 * time.Millisecond)
				}
				send(msg.payload, msg.topic, producer)
			}
		}
	}
	const workers int = 4
	iwg.Add(workers)
	//c := make([]chan *pChan, workers)
	c := make(chan *pChan)
	for i := 0; i < workers; i++ {
		//c[i] = make(chan *pChan)
		//go publisher(c[i])
		go publisher(c)
	}

	for {
		select {
		case r := <-rowChan:
			//c[0] <- &pChan{
			c <- &pChan{
				payload: r,
				topic: "row",
			}
		case header := <-headerChan:
			//c[1] <- &pChan{
			c <- &pChan{
				payload: header,
				topic: "block",
			}
		case tx := <-txChan:
			//c[2] <- &pChan{
			c <- &pChan{
				payload: tx,
				topic: "tx",
			}
		case account := <-miscChan:
			//c[3] <- &pChan{
			c <- &pChan{
				payload: account,
				topic: "misc",
			}
		case <-ctx.Done():
			iwg.Wait()
			log.Println("kafka workers exited")
			close(done)
			return
		}
	}
}
