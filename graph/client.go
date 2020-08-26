package fiograph

import (
	"context"
	"fmt"
	"github.com/gomodule/redigo/redis"
	rg "github.com/redislabs/redisgraph-go"
	"log"
	"time"
)

const (
	graphName = "fio"
	// FIXME: hard-coded to docker service name
	redisHost = "redis:6379"
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
			return
		case t := <-c.Traces:
			// TODO:
			go func() {
				fmt.Println(c.parseTrace(t))
			}()
		case r := <-c.Rows:
			// TODO:
			go func() {
				fmt.Println(c.putObtKvo(r))
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
