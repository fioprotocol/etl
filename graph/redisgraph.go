package fiograph

import (
	"encoding/json"
	"fmt"
	"github.com/fioprotocol/fio-go"
	"github.com/fioprotocol/fio.etl/transform"
	"github.com/mitchellh/mapstructure"
	rg "github.com/redislabs/redisgraph-go"
	"log"
)

// trimmed down copy of a full trace to get only what's needed to build the edge
type trace struct {
	Id             string `json:"id"`
	BlockTimestamp string `json:"block_timestamp"`
	BlockNum       uint32 `json:"block_num"`

	Trace struct {
		ActionTraces []struct {
			Receiver string `json:"receiver"`

			Act struct {
				Data          map[string]interface{} `json:"data"`
				Name          string                 `json:"name"`
				Authorization []struct {
					Permission string `json:"permission"`
					Actor      string `json:"actor"`
				} `json:"authorization"`
			} `json:"act"`
		} `json:"action_traces"`
	} `json:"trace"`
}

func (c *Client) parseTrace(b []byte) (src *rg.Node, dst *rg.Node, edg *rg.Edge) {
	var err error
	t := &trace{}
	err = json.Unmarshal(b, t)
	if err != nil {
		log.Println(err)
		return
	}
	if len(t.Trace.ActionTraces) == 0 {
		return
	}

	var source, dest account
	var from, to party
	var amt float64

	switch t.Trace.ActionTraces[0].Act.Name {
	case "regaddress":
		if t.Trace.ActionTraces[0].Act.Data != nil {
			err = c.putFioAddress(t.Trace.ActionTraces[0].Act.Data["fio_address"].(string), t.Trace.ActionTraces[0].Act.Data["owner_fio_public_key"].(string))
			if err != nil {
				log.Println(err)
			}
		}
		return

	case "newfundsreq":
		nfr := &Requests{}
		err = mapstructure.Decode(t.Trace.ActionTraces[0].Act.Data, nfr)
		if err != nil {
			log.Println(err)
			return
		}
		// these are the nodes
		source.Account = nfr.Actor
		_, pubkey, err := c.getFioAddress(nfr.PayerFioAddress)
		if err != nil {
			return
		}
		dest.Pubkey = pubkey

		// metadata
		from.Account = nfr.Actor
		from.FioAddress = nfr.PayeeFioAddress
		to.FioAddress = nfr.PayerFioAddress
		to.Account, to.Pubkey, err = c.getFioAddress(to.FioAddress)
		if err != nil {
			// log.Println(err)
			fmt.Println(string(b))
			log.Fatal(to.FioAddress, err)
		}

	// responses require a lookup to see who the payee was
	case "recordobt", "rejectfndreq":
		// a reject will fit in this struct despite being different, just will have empty fio names
		robt := &Requests{}
		err = mapstructure.Decode(t.Trace.ActionTraces[0].Act.Data, robt)
		if err != nil {
			log.Println(err)
			return
		}
		source.Account = robt.Actor

		//id, err := strconv.ParseInt(robt.FioRequestId.(string), 10, 32)
		//if err != nil {
		//	log.Println(err)
		//	fmt.Printf("%+v\n", robt)
		//	return
		//}
		//dest.Account, err = c.getObtAccount(uint32(id), payee)
		//if err != nil {
		//	log.Println(err)
		//}
		dest.Account, dest.Pubkey, err = c.getFioAddress(robt.PayeeFioAddress)

		from.Account = source.Account
		from.FioAddress = robt.PayerFioAddress
		to.Account = dest.Account
		to.FioAddress = robt.PayeeFioAddress

	case "trnsfiopubky":
		tfp := &TransferTokensPubKey{}
		err = mapstructure.Decode(t.Trace.ActionTraces[0].Act.Data, tfp)
		if err != nil {
			log.Println(err)
			return
		}

		source.Account = tfp.Actor
		dest.Pubkey = tfp.PayeePublicKey

		amt = float64(tfp.Amount) / 1_000_000_000.0
		from.Account = source.Account
		if len(tfp.PayeePublicKey) > 53 {
			_, tfp.PayeePublicKey, _ = transform.BadK1SumToPub(tfp.PayeePublicKey)
		}
		a, err := fio.ActorFromPub(tfp.PayeePublicKey)
		if err != nil {
			log.Println(err)
		}
		to.Account = string(a)
		to.Pubkey = tfp.PayeePublicKey

	default:
		//log.Printf("unrecognized type %+v\n", t)
		return
	}

	trace := trans{
		TxId:     t.Id,
		Code:     t.Trace.ActionTraces[0].Receiver,
		Name:     t.Trace.ActionTraces[0].Act.Name,
		BlockNum: t.BlockNum,
		Time:     t.BlockTimestamp,
		Amount:   amt,
		From:     from,
		To:       to,
	}

	g := c.graph()
	src = source.toNode()
	dst = dest.toNode()
	edg = trace.toEdge(src, dst)
	if q := MergePath(src, dst, edg); q != "" {
		_, err = g.graph.Query(q)
		if err != nil {
			log.Println(err)
		}
	}
	g.done()
	return
}

// FIXME: mapstructure allows specifying what tag to use, no need to re-create these.
type TransferTokensPubKey struct {
	PayeePublicKey string `mapstructure:"payee_public_key,omitempty"`
	Amount         uint64 `mapstructure:"amount,omitempty"`
	MaxFee         uint64 `mapstructure:"max_fee,omitempty"`
	Actor          string `mapstructure:"actor,omitempty"`
	Tpid           string `mapstructure:"tpid,omitempty"`
}

// Requests is a composite of the obt fields we want.
type Requests struct {
	Actor             string        `mapstructure:"actor,omitempty"`
	FioRequestId      interface{}   `mapstructure:"fio_request_id,omitempty"`
	PayeeFioAddress   string        `mapstructure:"payee_fio_address,omitempty"`
	PayeeFioPublicKey string        `mapstructure:"payee_fio_public_key,omitempty"`
	PayerFioAddress   string        `mapstructure:"payer_fio_address,omitempty"`
	PayerFioPublicKey string        `mapstructure:"payer_fio_public_key,omitempty"`
	TimeStamp         string        `mapstructure:"time_stamp,omitempty"`

}
