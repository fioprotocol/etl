package fiograph

import (
	"encoding/json"
	"github.com/fioprotocol/fio-go"
)

type obtKvo struct {
	Value struct {
		Table        string `json:"table"`
		PayeeKey     string `json:"payee_key"`
		PayerKey     string `json:"payer_key"`
		FioRequestId uint32 `json:"fio_request_id"`
	} `json:"value"`
}

func (c *Client) putObtKvo(b []byte) error {
	o := &obtKvo{}
	err := json.Unmarshal(b, o)
	if err != nil {
		return err
	}
	if o.Value.Table != "fioreqctxts" {
		return nil
	}
	erAcc, err := fio.ActorFromPub(o.Value.PayerKey)
	if err != nil {
		return err
	}
	eeAcc, err := fio.ActorFromPub(o.Value.PayeeKey)
	if err != nil {
		return err
	}
	err = c.putObtReq(o.Value.FioRequestId, string(eeAcc), string(erAcc))
	if err != nil {
		return err
	}
	return nil
}
