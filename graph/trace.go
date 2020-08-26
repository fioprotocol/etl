package fiograph

import (
	"encoding/json"
	"errors"
	"github.com/fioprotocol/fio-go"
	rg "github.com/redislabs/redisgraph-go"
	"log"
	"strconv"
	"time"

	"github.com/mitchellh/mapstructure"
)

// trimmed down copy of a full trace to get only what's needed to build the edge
type trace struct {
	Id             string    `json:"id"`
	BlockTimestamp time.Time `json:"block_timestamp"`
	BlockNum       uint32    `json:"block_num"`

	ActionTraces []struct {
		Receiver string                 `json:"receiver"`
		Data     map[string]interface{} `json:"data"`
		Name     string                 `json:"name"`

		Act struct {
			Authorization []struct {
				Permission string `json:"permission"`
				Actor      string `json:"actor"`
			} `json:"authorization"`
		} `json:"act"`
	} `json:"action_traces"`
}

func (c *Client) parseTrace(b []byte) (src *rg.Node, dst *rg.Node, edg *rg.Edge, err error) {
	t := &trace{}
	err = json.Unmarshal(b, t)
	if err != nil {
		return
	}
	if len(t.ActionTraces) == 0 {
		return nil, nil, nil, errors.New("no traces to decode")
	}

	var source, dest account
	var from, to party
	var amt float64

	switch t.ActionTraces[0].Name {
	case "newfundsreq":
		nfr := &fio.FundsReq{}
		err = mapstructure.Decode(t.ActionTraces[0].Data, nfr)
		if err != nil {
			return nil, nil, nil, err
		}
		// these are the nodes
		source.Account = nfr.Actor
		dest.Pubkey = nfr.PayerFioAddress

		// metadata
		from.Pubkey = nfr.PayeeFioAddress
		from.Account = nfr.Actor
		from.FioAddress = nfr.PayeeFioAddress
		to.FioAddress = nfr.PayerFioAddress
		to.Account, to.Pubkey, err = c.getFioAddress(to.FioAddress)
		if err != nil {
			log.Println(err)
		}

	// responses require a lookup to see who the payee was
	case "recordobt", "rejectfndreq":
		// a reject will fit in this struct despite being different, just will have empty fio names
		robt := &fio.RecordSend{}
		err = mapstructure.Decode(t.ActionTraces[0].Data, robt)
		if err != nil {
			return nil, nil, nil, err
		}
		id, err := strconv.ParseInt(robt.FioRequestId, 10, 32)
		if err != nil {
			return nil, nil, nil, err
		}

		source.Account = robt.Actor
		dest.Account, err = c.getObtAccount(uint32(id), payee)
		if err != nil {
			log.Println(err)
		}

		from.Account = source.Account
		from.FioAddress = robt.PayerFioAddress
		to.Account = dest.Account
		to.FioAddress = robt.PayeeFioAddress

	case "trnsfiopubky":
		tfp := &fio.TransferTokensPubKey{}
		err = mapstructure.Decode(t.ActionTraces[0].Data, tfp)
		if err != nil {
			return nil, nil, nil, err
		}

		source.Account = string(tfp.Actor)
		dest.Pubkey = tfp.PayeePublicKey

		amt = float64(tfp.Amount) / 1_000_000_000.0
		from.Account = source.Account
		a, err := fio.ActorFromPub(tfp.PayeePublicKey)
		if err != nil {
			log.Println(err)
		}
		to.Account = string(a)
		to.Pubkey = tfp.PayeePublicKey

	default:
		return
	}

	trace := trans{
		TxId:     t.Id,
		Code:     t.ActionTraces[0].Receiver,
		Name:     t.ActionTraces[0].Name,
		BlockNum: t.BlockNum,
		Time:     t.BlockTimestamp.UTC().Unix(),
		Amount:   amt,
		From:     from,
		To:       to,
	}

	src = source.toNode()
	dst = dest.toNode()
	edg = trace.toEdge(src, dst)

	switch nil {
	case src, dst, edg:
		return nil, nil, nil, errors.New("invalid graph: a node, or edge is missing")
	}
	return
}
