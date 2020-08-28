package fiograph

import (
	"errors"
	"fmt"
	"github.com/fioprotocol/fio-go"
	"log"
)

/*
kvs.go handles adding key-values to redis for fast lookups, since not all of our transactions have the needed
metadata to build a node->edge->node tuple, we need a lookup mechanism. Caching all address-to-pubkey mappings isn't
viable especially if we have to restart, so since we *already* have redis, may as well use it :)
*/

func (c *Client) getFioAddress(address string) (account string, pubkey string, err error) {
	client := c.Pool.Get()
	defer client.Close()
	reply, err := client.Do("GET", address)
	if err != nil {
		return "", "", err
	}
	switch reply.(type) {
	case nil:
		return "", "", errors.New("not found")
	case string, []byte:
		pubkey = string(reply.([]byte))
		a, err := fio.ActorFromPub(pubkey)
		if err != nil {
			return "", "", err
		}
		return string(a), pubkey, nil
	default:
		return "", "", errors.New(fmt.Sprintf("unknown type %t for address lookup", reply))
	}
}

func (c *Client) putFioAddress(address string, pubkey string) error {
	client := c.Pool.Get()
	defer client.Close()
	_, err := client.Do("SET", address, pubkey)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}
