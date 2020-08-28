package fiograph

import (
	"encoding/json"
	"github.com/fioprotocol/fio-go"
	"log"
)

type obtKvo struct {
	Value struct {
		Table        string `json:"table"`
		PayeeKey     string `json:"payee_key"`
		PayerKey     string `json:"payer_key"`
		FioRequestId uint32 `json:"fio_request_id"`
		Addresses []struct {
			PublicAddress string `json:"public_address"`
			ChainCode     string `json:"chain_code"`
		} `json:"addresses"`
		FioName string `json:"name"`
	} `json:"value"`
}

func (c *Client) putObtKvo(b []byte) error {
	o := &obtKvo{}
	err := json.Unmarshal(b, o)
	if err != nil {
		log.Println(err)
		return err
	}

	switch o.Value.Table {
	case "fionames":
		for _, a := range o.Value.Addresses {
			if a.ChainCode == "FIO" {
				client := c.Pool.Get()
				_, err = client.Do("PUT", o.Value.FioName, a.PublicAddress)
				client.Close()
				if err != nil {
					log.Println(err)
					return err
				}
				return nil
			}
		}
	case "fioreqctxts":
		erAcc, err := fio.ActorFromPub(o.Value.PayerKey)
		if err != nil {
			log.Println(err)
			return err
		}
		eeAcc, err := fio.ActorFromPub(o.Value.PayeeKey)
		if err != nil {
			log.Println(err)
			return err
		}
		err = c.putObtReq(o.Value.FioRequestId, string(eeAcc), string(erAcc))
		if err != nil {
			log.Println(err)
			return err
		}
	}
	return nil
}
