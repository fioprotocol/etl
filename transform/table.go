package transform

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/fioprotocol/fio-go/eos"
	"math"
	"strconv"
)

type TableData struct {
	Id             string       `json:"id"`
	RecordType     string       `json:"record_type"`
	BlockNum       interface{}  `json:"block_num"`
	BlockTimeStamp eos.JSONTime `json:"block_timestamp"`
	Added          interface{}  `json:"added"`
	Kvo            *Kvo         `json:"kvo"`
}

type Kvo struct {
	Code           string      `json:"code"`
	Scope          string      `json:"scope"`
	Table          string      `json:"table"`
	PrimaryKey     interface{} `json:"primary_key"`
	PrimaryKeyName string      `json:"primary_key_name"`
	Value          interface{} `json:"value"`
}

func (k *Kvo) fixTable() {
	// see if there was an abi error, and attempt to deal with it
	switch k.Value.(type) {
	case string, []byte:
		if s, ok := k.Value.(string); ok {
			k.Value = abis.lookup(k.Code, k.Table, s)
		}
	}

	// see if key is an integer value, and try to derive a name if appropriate
	i, err := strconv.ParseUint(k.PrimaryKey.(string), 10, 64)
	if err != nil {
		return
	}
	if i > uint64(math.MaxUint32) {
		// looks like a name, try to parse it:
		name := eos.NameToString(i)
		k.PrimaryKeyName = name
	}
}

func Table(b []byte) (j json.RawMessage, err error) {
	msg := &MsgData{}
	err = json.Unmarshal(b, msg)
	if err != nil || msg.Data == nil {
		return
	}
	td := &TableData{}
	err = json.Unmarshal(msg.Data, td)
	if err != nil || td.Kvo == nil {
		return
	}
	td.Kvo.fixTable()
	h := sha256.New()
	h.Write(b)
	td.Id = hex.EncodeToString(h.Sum(nil))
	td.RecordType = "table_row"
	td.BlockNum, _ = strconv.ParseUint(td.BlockNum.(string), 10, 32)
	j, err = json.Marshal(td)
	return j, err
}
