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
	"fmt"
	"github.com/fioprotocol/fio-go"
	"github.com/fioprotocol/fio-go/eos"
	"github.com/fioprotocol/fio.etl/transform"
	rg "github.com/redislabs/redisgraph-go"
	"log"
	"strconv"
	"strings"
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

	// if we only have a pubkey, get the hashed account name
	if a.Account == "" {
		// gotta have one or the other!
		if len(a.Pubkey) < 53 {
			log.Println("invalid pubkey", a.Pubkey)
			return nil
		}
		switch a.Pubkey[:3] {
		case "FIO", "PUB":
			break
		default:
			log.Println("invalid pub key:", a.Pubkey)
			return nil
		}
		acc, err := fio.ActorFromPub(a.Pubkey)
		if err != nil {
			log.Println(err, a)
			return nil
		}
		a.Account = string(acc)
	}

	id, err := eos.StringToName(a.Account)
	if err != nil {
		log.Println(err)
		return nil
	}

	return &rg.Node{
		Label: "Account",
		Alias: "_"+a.Account,
		Properties: map[string]interface{}{"actor":a.Account, "name": strconv.FormatUint(id, 10)},
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
	Time     string  `json:"time"`
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
	props["block_num"] = strconv.FormatInt(int64(t.BlockNum), 10)
	props["block_id"] = t.BlockId
	props["time"] = t.Time
	props["amount"] = strconv.FormatFloat(t.Amount, 'f', 9, 64)

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

	props["from_pubkey"], props["from_account"] = parties(t.From)
	props["from_fio_address"] = t.From.FioAddress

	props["to_pubkey"], props["to_account"] = parties(t.To)
	props["to_fio_address"] = t.To.FioAddress

	return rg.EdgeNew(t.Name, source, destination, props)
}

// party is either the To or From in a trans
type party struct {
	Account    string // should always be present
	Pubkey     string
	FioAddress string
}

// MergePath builds a query that ensures we don't end up creating new nodes every time we create a path,
// for some reason redisgraph-go doesn't have an option for doing this, so we avoid using their standard
// functions for building paths. Seems kinda fundamental, no?
func MergePath(n1, n2 *rg.Node, edg *rg.Edge) string {
	if n1 == nil || n2 == nil || edg == nil {
		return ""
	}
	var s string
	s += fmt.Sprintf("MERGE %s\n", n1.Encode())
	s += fmt.Sprintf("MERGE %s\n", n2.Encode())
	s += strings.Replace(fmt.Sprintf("MERGE %s\n", edg.Encode()), `[:`, fmt.Sprintf(`[_%s:`, edg.GetProperty("txid").(string)), 1)
	return s
}
