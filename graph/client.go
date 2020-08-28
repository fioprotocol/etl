package fiograph

import (
	"context"
	"github.com/gomodule/redigo/redis"
	rg "github.com/redislabs/redisgraph-go"
	"log"
	"time"
)

const (
	graphName = "fio"
)

type Client struct {
	Pool   *redis.Pool
	Traces chan []byte
	Rows   chan []byte
}

func NewClient(traces chan []byte, rows chan []byte) *Client {
	return &Client{
		Pool: &redis.Pool{
			MaxIdle:     5,
			IdleTimeout: 120 * time.Second,
			Dial:        func() (redis.Conn, error) { return redis.Dial("tcp", redisHost) },
		},
		Traces: traces,
		Rows:   rows,
	}
}

func (c *Client) Run(ctx context.Context) {
	test := c.Pool.Get()
	if _, e := test.Do("PING"); e != nil {
		log.Fatal(e)
	}
	test.Close()

	for {
		select {
		case <-ctx.Done():
			_ = c.Pool.Close()
			return
		case t := <-c.Traces:
			// TODO:
			go func() {
				_, _, _ = c.parseTrace(t)
			}()
		case r := <-c.Rows:
			// TODO:
			go func() {
				if e := c.putObtKvo(r); e != nil {
					log.Println(e)
				}
			}()
		}
	}
}

func (c *Client) graph() *grf {
	client := c.Pool.Get()
	return &grf{
		client: client,
		graph:  rg.GraphNew(graphName, client),
	}
}

type grf struct {
	client redis.Conn
	graph  rg.Graph
}

func (g *grf) done() (err error) {
	_, err = g.graph.Commit()
	if err != nil {
		_ = g.client.Close()
		return
	}
	return g.client.Close()
}
