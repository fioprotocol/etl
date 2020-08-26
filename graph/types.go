package fiograph

/*
fiograph uses redisgraph to provide a fast way to find relationships between accounts. Will be useful for spotting
fraud and abuse, as well as quickly identifying networks of accounts with shared attributes (such as those participating
in contest fraud.)

Presently it only indexes the following actions, though it may be expanded in the future.
- newfundsreq
- recordobt
- rejectfndreq
- trnsfiopubky

*/

import (
	"github.com/fioprotocol/fio-go"
	"github.com/fioprotocol/fio-go/eos"
	"github.com/fioprotocol/fio.etl/transform"
	"log"
	"strings"

	rg "github.com/redislabs/redisgraph-go"
)

// account is used to build our nodes, nodes are limited in what information they can contain, and the only
// consistent identifier is an account. If only a pubkey is present it's converted to an eos.Accountname and
// used here. The node itself will only contain the account, and will use the i64 value as the ID.
type account struct {
	Account string
	Pubkey  string
}

// toNode converts an account to a Node
func (a *account) toNode() *rg.Node {
	// nothing.
	if a.Account == "" && a.Pubkey == "" {
		return nil
	}
	if a.Account == "" {
		acc, err := fio.ActorFromPub(a.Pubkey)
		if err != nil {
			log.Println(err)
			return nil
		}
		a.Account = string(acc)
	}

	// explicitly set the ID by using the i64 representation of the name
	id, err := eos.StringToName(a.Account)
	if err != nil {
		log.Println(err)
		return nil
	}
	return &rg.Node{
		ID:    id,
		Label: a.Account,
	}
}

// trans is a abstract representation of a transaction that links two accounts, it holds minimal info assuming
// that a client will lookup the trace if additional details are needed. The only exception is the Amount field
// as a convenience for looking at trnsfiopubky. These are sourced from action traces.
type trans struct {
	TxId     string  `json:"txid"`
	Code     string  `json:"code"`
	Name     string  `json:"name"` // aka "action"
	BlockNum uint32  `json:"block_num"`
	BlockId  string  `json:"block_id"`
	Time     int64   `json:"time"`
	Amount   float64 `json:"amount,omitempty"`
	From     party   `json:"from"`
	To       party   `json:"to"`
}

// toEdge converts a trans to Edge, because the parties pubkeys are presented in PUB_K1 format by chronicle,
// and likely have bad checksums, it fixes those before the conversion.
func (t *trans) toEdge(source *rg.Node, destination *rg.Node) *rg.Edge {
	if source == nil || destination == nil {
		return nil
	}
	var err error
	props := make(map[string]interface{})
	props["txid"] = t.TxId
	props["code"] = t.Code
	props["name"] = t.Name
	props["block_num"] = t.BlockNum
	props["block_id"] = t.BlockId
	props["time"] = t.Time
	props["amount"] = t.Amount

	// fix pubkey, add account if missing
	parties := func(p party) (k, a string) {
		var acc eos.AccountName
		if strings.HasPrefix(t.From.Pubkey, "PUB_K1") {
			_, k, err = transform.BadK1SumToPub(t.From.Pubkey)
			if err != nil {
				log.Println(err)
				k = t.From.Pubkey
			}
		}
		if t.From.Account == "" {
			acc, err = fio.ActorFromPub(t.From.Pubkey)
			if err != nil {
				log.Println(err)
			}
		}
		return k, string(acc)
	}

	from := make(map[string]string)
	from["pubkey"], from["account"] = parties(t.From)
	from["fio_address"] = t.From.FioAddress
	props["from"] = from

	to := make(map[string]string)
	to["pubkey"], to["account"] = parties(t.To)
	to["fio_address"] = t.To.FioAddress
	props["from"] = to

	return rg.EdgeNew(t.Name, source, destination, props)
}

// party is either the To or From in a trans
type party struct {
	Account    string // should always be present
	Pubkey     string
	FioAddress string
}
