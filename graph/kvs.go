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

type payeeOrPayer uint8

const (
	invalid payeeOrPayer = iota
	payee
	payer
)

func (c *Client) getObtAccount(id uint32, who payeeOrPayer) (account string, err error) {
	var prefix string
	switch who {
	case payee:
		prefix = "payee"
	case payer:
		prefix = "payer"
	default:
		return "", errors.New("invalid payeeOrPayer type")
	}
	client := c.Pool.Get()
	defer client.Close()
	var resp interface{}
	resp, err = client.Do("GET", fmt.Sprintf("%s-%d", prefix, id))
	if err != nil {
		return "", err
	}
	switch resp.(type) {
	case string:
		return resp.(string), nil
	default:
		return "", errors.New(fmt.Sprintf("unexpected %t -- %v", resp, resp))
	}
}

// putReq expects an account not a pubkey, use fio.ActorFromPub to get this from pubkey before using....
func (c *Client) putObtReq(id uint32, payeeAccount, payerAccount string) error {
	client := c.Pool.Get()
	defer client.Close()
	_, err := client.Do("PUT", fmt.Sprintf("payee-%d", id), payeeAccount)
	if err != nil {
		return err
	}
	_, err = client.Do("PUT", fmt.Sprintf("payer-%d", id), payerAccount)
	if err != nil {
		return err
	}
	return nil
}
