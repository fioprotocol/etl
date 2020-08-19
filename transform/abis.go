package transform

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"github.com/fioprotocol/fio-go/eos"
	"strconv"
	"sync"
)

type AbiUpdate struct {
	Id             string          `json:"id"`
	RecordType     string          `json:"record_type"`
	BlockNum       interface{}     `json:"block_num"`
	BlockTimestamp string          `json:"block_timestamp"`
	Account        string          `json:"account"`
	Abi            json.RawMessage `json:"abi"`
	AbiBytes       string          `json:"abi_bytes"`
}

func Abi(b []byte) (abi json.RawMessage, err error) {
	msg := &MsgData{}
	err = json.Unmarshal(b, msg)
	if err != nil || msg.Data == nil {
		return
	}
	a := &AbiUpdate{}
	err = json.Unmarshal(msg.Data, a)
	if err != nil {
		return
	}
	h := sha256.New()
	h.Write(a.Abi)
	a.Id = hex.EncodeToString(h.Sum(nil))
	a.BlockNum, _ = strconv.ParseUint(a.BlockNum.(string), 10, 32)
	a.RecordType = "abi"
	abis.add(a.Account, a.Abi)
	abi, err = json.Marshal(a)
	return
}

type abiMap struct {
	abi map[string]*eos.ABI
	sync.RWMutex
}

func newAbiMap() (*abiMap, error) {
	var err error
	a := &abiMap{}
	a.abi = make(map[string]*eos.ABI)
	for k, v := range map[string][]byte{
		"eosio":        eosioAbi,
		"eosio.msig":   eosioMsigAbi,
		"fio.address":  fioAddressAbi,
		"fio.fee":      fioFeeAbi,
		"fio.reqobt":   fioReqobtAbi,
		"fio.token":    fioTokenAbi,
		"fio.tpid":     fioTpidAbi,
		"fio.treasury": fioTreasuryAbi,
	} {
		a.abi[k], err = eos.NewABI(bytes.NewReader(v))
		if err != nil {
			return nil, err
		}
	}
	return a, nil
}

func (a *abiMap) add(account string, abi []byte) {
	na, err := eos.NewABI(bytes.NewReader(abi))
	if err != nil {
		elog.Println("adding new ABI:", err)
		return
	}
	a.Lock()
	a.abi[account] = na
	a.Unlock()
}

func (a abiMap) lookup(account string, table string, s string) json.RawMessage {
	// already json?
	if s[0] == '{' {
		return []byte(`"` + s + `"`)
	}
	a.RLock()
	abi := a.abi[account]
	a.RUnlock()
	if abi == nil {
		return []byte(`"` + s + `"`)
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return []byte(`"` + s + `"`)
	}
	j, err := abi.DecodeTableRow(eos.TableName(table), b)
	if err != nil {
		return []byte(`"` + s + `"`)
	}
	return j
}

var eosioAbi = []byte(`{
    "version": "eosio::abi/1.1",
    "types": [],
    "structs": [
      {
        "name": "abi_hash",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "hash",
            "type": "checksum256"
          }
        ]
      },
      {
        "name": "addlocked",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "amount",
            "type": "int64"
          },
          {
            "name": "locktype",
            "type": "int16"
          }
        ]
      },
      {
        "name": "authority",
        "base": "",
        "fields": [
          {
            "name": "threshold",
            "type": "uint32"
          },
          {
            "name": "keys",
            "type": "key_weight[]"
          },
          {
            "name": "accounts",
            "type": "permission_level_weight[]"
          },
          {
            "name": "waits",
            "type": "wait_weight[]"
          }
        ]
      },
      {
        "name": "block_header",
        "base": "",
        "fields": [
          {
            "name": "timestamp",
            "type": "uint32"
          },
          {
            "name": "producer",
            "type": "name"
          },
          {
            "name": "confirmed",
            "type": "uint16"
          },
          {
            "name": "previous",
            "type": "checksum256"
          },
          {
            "name": "transaction_mroot",
            "type": "checksum256"
          },
          {
            "name": "action_mroot",
            "type": "checksum256"
          },
          {
            "name": "schedule_version",
            "type": "uint32"
          },
          {
            "name": "new_producers",
            "type": "producer_schedule?"
          }
        ]
      },
      {
        "name": "blockchain_parameters",
        "base": "",
        "fields": [
          {
            "name": "max_block_net_usage",
            "type": "uint64"
          },
          {
            "name": "target_block_net_usage_pct",
            "type": "uint32"
          },
          {
            "name": "max_transaction_net_usage",
            "type": "uint32"
          },
          {
            "name": "base_per_transaction_net_usage",
            "type": "uint32"
          },
          {
            "name": "net_usage_leeway",
            "type": "uint32"
          },
          {
            "name": "context_free_discount_net_usage_num",
            "type": "uint32"
          },
          {
            "name": "context_free_discount_net_usage_den",
            "type": "uint32"
          },
          {
            "name": "max_block_cpu_usage",
            "type": "uint32"
          },
          {
            "name": "target_block_cpu_usage_pct",
            "type": "uint32"
          },
          {
            "name": "max_transaction_cpu_usage",
            "type": "uint32"
          },
          {
            "name": "min_transaction_cpu_usage",
            "type": "uint32"
          },
          {
            "name": "max_transaction_lifetime",
            "type": "uint32"
          },
          {
            "name": "deferred_trx_expiration_window",
            "type": "uint32"
          },
          {
            "name": "max_transaction_delay",
            "type": "uint32"
          },
          {
            "name": "max_inline_action_size",
            "type": "uint32"
          },
          {
            "name": "max_inline_action_depth",
            "type": "uint16"
          },
          {
            "name": "max_authority_depth",
            "type": "uint16"
          }
        ]
      },
      {
        "name": "burnaction",
        "base": "",
        "fields": [
          {
            "name": "fioaddrhash",
            "type": "uint128"
          }
        ]
      },
      {
        "name": "canceldelay",
        "base": "",
        "fields": [
          {
            "name": "canceling_auth",
            "type": "permission_level"
          },
          {
            "name": "trx_id",
            "type": "checksum256"
          }
        ]
      },
      {
        "name": "crautoproxy",
        "base": "",
        "fields": [
          {
            "name": "proxy",
            "type": "name"
          },
          {
            "name": "owner",
            "type": "name"
          }
        ]
      },
      {
        "name": "deleteauth",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "permission",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "eosio_global_state",
        "base": "blockchain_parameters",
        "fields": [
          {
            "name": "last_producer_schedule_update",
            "type": "block_timestamp_type"
          },
          {
            "name": "last_pervote_bucket_fill",
            "type": "time_point"
          },
          {
            "name": "pervote_bucket",
            "type": "int64"
          },
          {
            "name": "perblock_bucket",
            "type": "int64"
          },
          {
            "name": "total_unpaid_blocks",
            "type": "uint32"
          },
          {
            "name": "total_voted_fio",
            "type": "int64"
          },
          {
            "name": "thresh_voted_fio_time",
            "type": "time_point"
          },
          {
            "name": "last_producer_schedule_size",
            "type": "uint16"
          },
          {
            "name": "total_producer_vote_weight",
            "type": "float64"
          },
          {
            "name": "last_name_close",
            "type": "block_timestamp_type"
          },
          {
            "name": "last_fee_update",
            "type": "block_timestamp_type"
          }
        ]
      },
      {
        "name": "eosio_global_state2",
        "base": "",
        "fields": [
          {
            "name": "last_block_num",
            "type": "block_timestamp_type"
          },
          {
            "name": "total_producer_votepay_share",
            "type": "float64"
          },
          {
            "name": "revision",
            "type": "uint8"
          }
        ]
      },
      {
        "name": "eosio_global_state3",
        "base": "",
        "fields": [
          {
            "name": "last_vpay_state_update",
            "type": "time_point"
          },
          {
            "name": "total_vpay_share_change_rate",
            "type": "float64"
          }
        ]
      },
      {
        "name": "incram",
        "base": "",
        "fields": [
          {
            "name": "accountmn",
            "type": "name"
          },
          {
            "name": "amount",
            "type": "int64"
          }
        ]
      },
      {
        "name": "inhibitunlck",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "value",
            "type": "uint32"
          }
        ]
      },
      {
        "name": "init",
        "base": "",
        "fields": [
          {
            "name": "version",
            "type": "varuint32"
          },
          {
            "name": "core",
            "type": "symbol"
          }
        ]
      },
      {
        "name": "key_weight",
        "base": "",
        "fields": [
          {
            "name": "key",
            "type": "public_key"
          },
          {
            "name": "weight",
            "type": "uint16"
          }
        ]
      },
      {
        "name": "linkauth",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "code",
            "type": "name"
          },
          {
            "name": "type",
            "type": "name"
          },
          {
            "name": "requirement",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "locked_token_holder_info",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "total_grant_amount",
            "type": "uint64"
          },
          {
            "name": "unlocked_period_count",
            "type": "uint32"
          },
          {
            "name": "grant_type",
            "type": "uint32"
          },
          {
            "name": "inhibit_unlocking",
            "type": "uint32"
          },
          {
            "name": "remaining_locked_amount",
            "type": "uint64"
          },
          {
            "name": "timestamp",
            "type": "uint32"
          }
        ]
      },
      {
        "name": "newaccount",
        "base": "",
        "fields": [
          {
            "name": "creator",
            "type": "name"
          },
          {
            "name": "name",
            "type": "name"
          },
          {
            "name": "owner",
            "type": "authority"
          },
          {
            "name": "active",
            "type": "authority"
          }
        ]
      },
      {
        "name": "onblock",
        "base": "",
        "fields": [
          {
            "name": "header",
            "type": "block_header"
          }
        ]
      },
      {
        "name": "onerror",
        "base": "",
        "fields": [
          {
            "name": "sender_id",
            "type": "uint128"
          },
          {
            "name": "sent_trx",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "permission_level",
        "base": "",
        "fields": [
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "permission",
            "type": "name"
          }
        ]
      },
      {
        "name": "permission_level_weight",
        "base": "",
        "fields": [
          {
            "name": "permission",
            "type": "permission_level"
          },
          {
            "name": "weight",
            "type": "uint16"
          }
        ]
      },
      {
        "name": "producer_info",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "addresshash",
            "type": "uint128"
          },
          {
            "name": "total_votes",
            "type": "float64"
          },
          {
            "name": "producer_public_key",
            "type": "public_key"
          },
          {
            "name": "is_active",
            "type": "bool"
          },
          {
            "name": "url",
            "type": "string"
          },
          {
            "name": "unpaid_blocks",
            "type": "uint32"
          },
          {
            "name": "last_claim_time",
            "type": "time_point"
          },
          {
            "name": "last_bpclaim",
            "type": "uint32"
          },
          {
            "name": "location",
            "type": "uint16"
          }
        ]
      },
      {
        "name": "producer_key",
        "base": "",
        "fields": [
          {
            "name": "producer_name",
            "type": "name"
          },
          {
            "name": "block_signing_key",
            "type": "public_key"
          }
        ]
      },
      {
        "name": "producer_schedule",
        "base": "",
        "fields": [
          {
            "name": "version",
            "type": "uint32"
          },
          {
            "name": "producers",
            "type": "producer_key[]"
          }
        ]
      },
      {
        "name": "regproducer",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "fio_pub_key",
            "type": "string"
          },
          {
            "name": "url",
            "type": "string"
          },
          {
            "name": "location",
            "type": "uint16"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "regproxy",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "resetclaim",
        "base": "",
        "fields": [
          {
            "name": "producer",
            "type": "name"
          }
        ]
      },
      {
        "name": "rmvproducer",
        "base": "",
        "fields": [
          {
            "name": "producer",
            "type": "name"
          }
        ]
      },
      {
        "name": "setabi",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "abi",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "setautoproxy",
        "base": "",
        "fields": [
          {
            "name": "proxy",
            "type": "name"
          },
          {
            "name": "owner",
            "type": "name"
          }
        ]
      },
      {
        "name": "setcode",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "vmtype",
            "type": "uint8"
          },
          {
            "name": "vmversion",
            "type": "uint8"
          },
          {
            "name": "code",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "setparams",
        "base": "",
        "fields": [
          {
            "name": "params",
            "type": "blockchain_parameters"
          }
        ]
      },
      {
        "name": "setpriv",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "is_priv",
            "type": "uint8"
          }
        ]
      },
      {
        "name": "top_prod_info",
        "base": "",
        "fields": [
          {
            "name": "producer",
            "type": "name"
          }
        ]
      },
      {
        "name": "unlinkauth",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "code",
            "type": "name"
          },
          {
            "name": "type",
            "type": "name"
          }
        ]
      },
      {
        "name": "unlocktokens",
        "base": "",
        "fields": [
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "unregprod",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "unregproxy",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "updateauth",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "permission",
            "type": "name"
          },
          {
            "name": "parent",
            "type": "name"
          },
          {
            "name": "auth",
            "type": "authority"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "updatepower",
        "base": "",
        "fields": [
          {
            "name": "voter",
            "type": "name"
          },
          {
            "name": "updateonly",
            "type": "bool"
          }
        ]
      },
      {
        "name": "updlbpclaim",
        "base": "",
        "fields": [
          {
            "name": "producer",
            "type": "name"
          }
        ]
      },
      {
        "name": "updlocked",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "amountremaining",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "updtrevision",
        "base": "",
        "fields": [
          {
            "name": "revision",
            "type": "uint8"
          }
        ]
      },
      {
        "name": "user_resources",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "net_weight",
            "type": "asset"
          },
          {
            "name": "cpu_weight",
            "type": "asset"
          },
          {
            "name": "ram_bytes",
            "type": "int64"
          }
        ]
      },
      {
        "name": "voteproducer",
        "base": "",
        "fields": [
          {
            "name": "producers",
            "type": "string[]"
          },
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "voteproxy",
        "base": "",
        "fields": [
          {
            "name": "proxy",
            "type": "string"
          },
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "voter_info",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "fioaddress",
            "type": "string"
          },
          {
            "name": "addresshash",
            "type": "uint128"
          },
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "proxy",
            "type": "name"
          },
          {
            "name": "producers",
            "type": "name[]"
          },
          {
            "name": "last_vote_weight",
            "type": "float64"
          },
          {
            "name": "proxied_vote_weight",
            "type": "float64"
          },
          {
            "name": "is_proxy",
            "type": "bool"
          },
          {
            "name": "is_auto_proxy",
            "type": "bool"
          },
          {
            "name": "reserved2",
            "type": "uint32"
          },
          {
            "name": "reserved3",
            "type": "asset"
          }
        ]
      },
      {
        "name": "wait_weight",
        "base": "",
        "fields": [
          {
            "name": "wait_sec",
            "type": "uint32"
          },
          {
            "name": "weight",
            "type": "uint16"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "addlocked",
        "type": "addlocked",
        "ricardian_contract": ""
      },
      {
        "name": "burnaction",
        "type": "burnaction",
        "ricardian_contract": ""
      },
      {
        "name": "canceldelay",
        "type": "canceldelay",
        "ricardian_contract": ""
      },
      {
        "name": "crautoproxy",
        "type": "crautoproxy",
        "ricardian_contract": ""
      },
      {
        "name": "deleteauth",
        "type": "deleteauth",
        "ricardian_contract": ""
      },
      {
        "name": "incram",
        "type": "incram",
        "ricardian_contract": ""
      },
      {
        "name": "inhibitunlck",
        "type": "inhibitunlck",
        "ricardian_contract": ""
      },
      {
        "name": "init",
        "type": "init",
        "ricardian_contract": ""
      },
      {
        "name": "linkauth",
        "type": "linkauth",
        "ricardian_contract": ""
      },
      {
        "name": "newaccount",
        "type": "newaccount",
        "ricardian_contract": ""
      },
      {
        "name": "onblock",
        "type": "onblock",
        "ricardian_contract": ""
      },
      {
        "name": "onerror",
        "type": "onerror",
        "ricardian_contract": ""
      },
      {
        "name": "regproducer",
        "type": "regproducer",
        "ricardian_contract": ""
      },
      {
        "name": "regproxy",
        "type": "regproxy",
        "ricardian_contract": ""
      },
      {
        "name": "resetclaim",
        "type": "resetclaim",
        "ricardian_contract": ""
      },
      {
        "name": "rmvproducer",
        "type": "rmvproducer",
        "ricardian_contract": ""
      },
      {
        "name": "setabi",
        "type": "setabi",
        "ricardian_contract": ""
      },
      {
        "name": "setautoproxy",
        "type": "setautoproxy",
        "ricardian_contract": ""
      },
      {
        "name": "setcode",
        "type": "setcode",
        "ricardian_contract": ""
      },
      {
        "name": "setparams",
        "type": "setparams",
        "ricardian_contract": ""
      },
      {
        "name": "setpriv",
        "type": "setpriv",
        "ricardian_contract": ""
      },
      {
        "name": "unlinkauth",
        "type": "unlinkauth",
        "ricardian_contract": ""
      },
      {
        "name": "unlocktokens",
        "type": "unlocktokens",
        "ricardian_contract": ""
      },
      {
        "name": "unregprod",
        "type": "unregprod",
        "ricardian_contract": ""
      },
      {
        "name": "unregproxy",
        "type": "unregproxy",
        "ricardian_contract": ""
      },
      {
        "name": "updateauth",
        "type": "updateauth",
        "ricardian_contract": ""
      },
      {
        "name": "updatepower",
        "type": "updatepower",
        "ricardian_contract": ""
      },
      {
        "name": "updlbpclaim",
        "type": "updlbpclaim",
        "ricardian_contract": ""
      },
      {
        "name": "updlocked",
        "type": "updlocked",
        "ricardian_contract": ""
      },
      {
        "name": "updtrevision",
        "type": "updtrevision",
        "ricardian_contract": ""
      },
      {
        "name": "voteproducer",
        "type": "voteproducer",
        "ricardian_contract": ""
      },
      {
        "name": "voteproxy",
        "type": "voteproxy",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "abihash",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "abi_hash"
      },
      {
        "name": "global",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "eosio_global_state"
      },
      {
        "name": "global2",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "eosio_global_state2"
      },
      {
        "name": "global3",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "eosio_global_state3"
      },
      {
        "name": "lockedtokens",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "locked_token_holder_info"
      },
      {
        "name": "producers",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "producer_info"
      },
      {
        "name": "topprods",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "top_prod_info"
      },
      {
        "name": "userres",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "user_resources"
      },
      {
        "name": "voters",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "voter_info"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var eosioMsigAbi = []byte(`{
    "version": "eosio::abi/1.1",
    "types": [],
    "structs": [
      {
        "name": "action",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "name",
            "type": "name"
          },
          {
            "name": "authorization",
            "type": "permission_level[]"
          },
          {
            "name": "data",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "approval",
        "base": "",
        "fields": [
          {
            "name": "level",
            "type": "permission_level"
          },
          {
            "name": "time",
            "type": "time_point"
          }
        ]
      },
      {
        "name": "approvals_info",
        "base": "",
        "fields": [
          {
            "name": "version",
            "type": "uint8"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "requested_approvals",
            "type": "approval[]"
          },
          {
            "name": "provided_approvals",
            "type": "approval[]"
          }
        ]
      },
      {
        "name": "approve",
        "base": "",
        "fields": [
          {
            "name": "proposer",
            "type": "name"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "level",
            "type": "permission_level"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          },
          {
            "name": "proposal_hash",
            "type": "checksum256$"
          }
        ]
      },
      {
        "name": "cancel",
        "base": "",
        "fields": [
          {
            "name": "proposer",
            "type": "name"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "canceler",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "exec",
        "base": "",
        "fields": [
          {
            "name": "proposer",
            "type": "name"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          },
          {
            "name": "executer",
            "type": "name"
          }
        ]
      },
      {
        "name": "extension",
        "base": "",
        "fields": [
          {
            "name": "type",
            "type": "uint16"
          },
          {
            "name": "data",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "invalidate",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "invalidation",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "last_invalidation_time",
            "type": "time_point"
          }
        ]
      },
      {
        "name": "old_approvals_info",
        "base": "",
        "fields": [
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "requested_approvals",
            "type": "permission_level[]"
          },
          {
            "name": "provided_approvals",
            "type": "permission_level[]"
          }
        ]
      },
      {
        "name": "permission_level",
        "base": "",
        "fields": [
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "permission",
            "type": "name"
          }
        ]
      },
      {
        "name": "proposal",
        "base": "",
        "fields": [
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "packed_transaction",
            "type": "bytes"
          }
        ]
      },
      {
        "name": "propose",
        "base": "",
        "fields": [
          {
            "name": "proposer",
            "type": "name"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "requested",
            "type": "permission_level[]"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          },
          {
            "name": "trx",
            "type": "transaction"
          }
        ]
      },
      {
        "name": "transaction",
        "base": "transaction_header",
        "fields": [
          {
            "name": "context_free_actions",
            "type": "action[]"
          },
          {
            "name": "actions",
            "type": "action[]"
          },
          {
            "name": "transaction_extensions",
            "type": "extension[]"
          }
        ]
      },
      {
        "name": "transaction_header",
        "base": "",
        "fields": [
          {
            "name": "expiration",
            "type": "time_point_sec"
          },
          {
            "name": "ref_block_num",
            "type": "uint16"
          },
          {
            "name": "ref_block_prefix",
            "type": "uint32"
          },
          {
            "name": "max_net_usage_words",
            "type": "varuint32"
          },
          {
            "name": "max_cpu_usage_ms",
            "type": "uint8"
          },
          {
            "name": "delay_sec",
            "type": "varuint32"
          }
        ]
      },
      {
        "name": "unapprove",
        "base": "",
        "fields": [
          {
            "name": "proposer",
            "type": "name"
          },
          {
            "name": "proposal_name",
            "type": "name"
          },
          {
            "name": "level",
            "type": "permission_level"
          },
          {
            "name": "max_fee",
            "type": "uint64"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "approve",
        "type": "approve",
        "ricardian_contract": ""
      },
      {
        "name": "cancel",
        "type": "cancel",
        "ricardian_contract": ""
      },
      {
        "name": "exec",
        "type": "exec",
        "ricardian_contract": ""
      },
      {
        "name": "invalidate",
        "type": "invalidate",
        "ricardian_contract": ""
      },
      {
        "name": "propose",
        "type": "propose",
        "ricardian_contract": ""
      },
      {
        "name": "unapprove",
        "type": "unapprove",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "approvals",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "old_approvals_info"
      },
      {
        "name": "approvals2",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "approvals_info"
      },
      {
        "name": "invals",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "invalidation"
      },
      {
        "name": "proposal",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "proposal"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioAddressAbi = []byte(`{
    "version": "eosio::abi/1.0",
    "types": [],
    "structs": [
      {
        "name": "fioname",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "namehash",
            "type": "uint128"
          },
          {
            "name": "domain",
            "type": "string"
          },
          {
            "name": "domainhash",
            "type": "uint128"
          },
          {
            "name": "expiration",
            "type": "uint64"
          },
          {
            "name": "owner_account",
            "type": "name"
          },
          {
            "name": "addresses",
            "type": "tokenpubaddr[]"
          },
          {
            "name": "bundleeligiblecountdown",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "domain",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "name",
            "type": "string"
          },
          {
            "name": "domainhash",
            "type": "uint128"
          },
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "is_public",
            "type": "uint8"
          },
          {
            "name": "expiration",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "eosio_name",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "clientkey",
            "type": "string"
          },
          {
            "name": "keyhash",
            "type": "uint128"
          }
        ]
      },
      {
        "name": "regaddress",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "owner_fio_public_key",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "tokenpubaddr",
        "base": "",
        "fields": [
          {
            "name": "token_code",
            "type": "string"
          },
          {
            "name": "chain_code",
            "type": "string"
          },
          {
            "name": "public_address",
            "type": "string"
          }
        ]
      },
      {
        "name": "addaddress",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "public_addresses",
            "type": "tokenpubaddr[]"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "remaddress",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "public_addresses",
            "type": "tokenpubaddr[]"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "remalladdr",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "regdomain",
        "base": "",
        "fields": [
          {
            "name": "fio_domain",
            "type": "string"
          },
          {
            "name": "owner_fio_public_key",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "renewdomain",
        "base": "",
        "fields": [
          {
            "name": "fio_domain",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "tpid",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "renewaddress",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "tpid",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "setdomainpub",
        "base": "",
        "fields": [
          {
            "name": "fio_domain",
            "type": "string"
          },
          {
            "name": "is_public",
            "type": "int8"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "burnexpired",
        "base": "",
        "fields": []
      },
      {
        "name": "decrcounter",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "step",
            "type": "int32"
          }
        ]
      },
      {
        "name": "bind2eosio",
        "base": "",
        "fields": [
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "client_key",
            "type": "string"
          },
          {
            "name": "existing",
            "type": "bool"
          }
        ]
      },
      {
        "name": "xferdomain",
        "base": "",
        "fields": [
          {
            "name": "fio_domain",
            "type": "string"
          },
          {
            "name": "new_owner_fio_public_key",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "tpid",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "xferaddress",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "new_owner_fio_public_key",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "tpid",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "decrcounter",
        "type": "decrcounter",
        "ricardian_contract": ""
      },
      {
        "name": "regaddress",
        "type": "regaddress",
        "ricardian_contract": ""
      },
      {
        "name": "addaddress",
        "type": "addaddress",
        "ricardian_contract": ""
      },
      {
        "name": "remaddress",
        "type": "remaddress",
        "ricardian_contract": ""
      },
      {
        "name": "remalladdr",
        "type": "remalladdr",
        "ricardian_contract": ""
      },
      {
        "name": "regdomain",
        "type": "regdomain",
        "ricardian_contract": ""
      },
      {
        "name": "renewdomain",
        "type": "renewdomain",
        "ricardian_contract": ""
      },
      {
        "name": "renewaddress",
        "type": "renewaddress",
        "ricardian_contract": ""
      },
      {
        "name": "burnexpired",
        "type": "burnexpired",
        "ricardian_contract": ""
      },
      {
        "name": "setdomainpub",
        "type": "setdomainpub",
        "ricardian_contract": ""
      },
      {
        "name": "bind2eosio",
        "type": "bind2eosio",
        "ricardian_contract": ""
      },
      {
        "name": "xferdomain",
        "type": "xferdomain",
        "ricardian_contract": ""
      },
      {
        "name": "xferaddress",
        "type": "xferaddress",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "fionames",
        "index_type": "i64",
        "key_names": [
          "id"
        ],
        "key_types": [
          "string"
        ],
        "type": "fioname"
      },
      {
        "name": "domains",
        "index_type": "i64",
        "key_names": [
          "id"
        ],
        "key_types": [
          "string"
        ],
        "type": "domain"
      },
      {
        "name": "accountmap",
        "index_type": "i64",
        "key_names": [
          "account"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "eosio_name"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioFeeAbi = []byte(`{
    "version": "eosio::abi/1.0",
    "types": [],
    "structs": [
      {
        "name": "createfee",
        "base": "",
        "fields": [
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "type",
            "type": "int64"
          },
          {
            "name": "suf_amount",
            "type": "int64"
          }
        ]
      },
      {
        "name": "feevalue",
        "base": "",
        "fields": [
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "value",
            "type": "int64"
          }
        ]
      },
      {
        "name": "setfeevote",
        "base": "",
        "fields": [
          {
            "name": "fee_ratios",
            "type": "feevalue[]"
          },
          {
            "name": "actor",
            "type": "string"
          }
        ]
      },
      {
        "name": "bundlevote",
        "base": "",
        "fields": [
          {
            "name": "bundled_transactions",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "string"
          }
        ]
      },
      {
        "name": "setfeemult",
        "base": "",
        "fields": [
          {
            "name": "multiplier",
            "type": "float64"
          },
          {
            "name": "actor",
            "type": "string"
          }
        ]
      },
      {
        "name": "mandatoryfee",
        "base": "",
        "fields": [
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          }
        ]
      },
      {
        "name": "bytemandfee",
        "base": "",
        "fields": [
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "account",
            "type": "name"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "bytesize",
            "type": "int64"
          }
        ]
      },
      {
        "name": "updatefees",
        "base": "",
        "fields": []
      },
      {
        "name": "fiofee",
        "base": "",
        "fields": [
          {
            "name": "fee_id",
            "type": "uint64"
          },
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "end_point_hash",
            "type": "uint128"
          },
          {
            "name": "type",
            "type": "uint64"
          },
          {
            "name": "suf_amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "feevoter",
        "base": "",
        "fields": [
          {
            "name": "block_producer_name",
            "type": "name"
          },
          {
            "name": "fee_multiplier",
            "type": "float64"
          },
          {
            "name": "lastvotetimestamp",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "feevote",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "block_producer_name",
            "type": "name"
          },
          {
            "name": "end_point",
            "type": "string"
          },
          {
            "name": "end_point_hash",
            "type": "uint128"
          },
          {
            "name": "suf_amount",
            "type": "uint64"
          },
          {
            "name": "lastvotetimestamp",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bundlevoter",
        "base": "",
        "fields": [
          {
            "name": "block_producer_name",
            "type": "name"
          },
          {
            "name": "bundledbvotenumber",
            "type": "uint64"
          },
          {
            "name": "lastvotetimestamp",
            "type": "uint64"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "createfee",
        "type": "createfee",
        "ricardian_contract": ""
      },
      {
        "name": "setfeevote",
        "type": "setfeevote",
        "ricardian_contract": ""
      },
      {
        "name": "bundlevote",
        "type": "bundlevote",
        "ricardian_contract": ""
      },
      {
        "name": "setfeemult",
        "type": "setfeemult",
        "ricardian_contract": ""
      },
      {
        "name": "mandatoryfee",
        "type": "mandatoryfee",
        "ricardian_contract": ""
      },
      {
        "name": "bytemandfee",
        "type": "bytemandfee",
        "ricardian_contract": ""
      },
      {
        "name": "updatefees",
        "type": "updatefees",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "bundlevoters",
        "index_type": "i64",
        "key_names": [
          "block_producer_name"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "bundlevoter"
      },
      {
        "name": "fiofees",
        "index_type": "i64",
        "key_names": [
          "fee_id"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "fiofee"
      },
      {
        "name": "feevoters",
        "index_type": "i64",
        "key_names": [
          "block_producer_name"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "feevoter"
      },
      {
        "name": "feevotes",
        "index_type": "i64",
        "key_names": [
          "block_producer_name"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "feevote"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioReqobtAbi = []byte(`{
    "version": "eosio::abi/1.0",
    "types": [],
    "structs": [
      {
        "name": "fioreqctxt",
        "base": "",
        "fields": [
          {
            "name": "fio_request_id",
            "type": "uint64"
          },
          {
            "name": "payer_fio_address",
            "type": "uint128"
          },
          {
            "name": "payee_fio_address",
            "type": "uint128"
          },
          {
            "name": "payer_fio_address_hex_str",
            "type": "string"
          },
          {
            "name": "payee_fio_address_hex_str",
            "type": "string"
          },
          {
            "name": "payer_fio_address_with_time",
            "type": "uint128"
          },
          {
            "name": "payee_fio_address_with_time",
            "type": "uint128"
          },
          {
            "name": "content",
            "type": "string"
          },
          {
            "name": "time_stamp",
            "type": "uint64"
          },
          {
            "name": "payer_fio_addr",
            "type": "string"
          },
          {
            "name": "payee_fio_addr",
            "type": "string"
          },
          {
            "name": "payer_key",
            "type": "string"
          },
          {
            "name": "payee_key",
            "type": "string"
          }
        ]
      },
      {
        "name": "recordobt_info",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "payer_fio_address",
            "type": "uint128"
          },
          {
            "name": "payee_fio_address",
            "type": "uint128"
          },
          {
            "name": "payer_fio_address_hex_str",
            "type": "string"
          },
          {
            "name": "payee_fio_address_hex_str",
            "type": "string"
          },
          {
            "name": "payer_fio_address_with_time",
            "type": "uint128"
          },
          {
            "name": "payee_fio_address_with_time",
            "type": "uint128"
          },
          {
            "name": "content",
            "type": "string"
          },
          {
            "name": "time_stamp",
            "type": "uint64"
          },
          {
            "name": "payer_fio_addr",
            "type": "string"
          },
          {
            "name": "payee_fio_addr",
            "type": "string"
          },
          {
            "name": "payer_key",
            "type": "string"
          },
          {
            "name": "payee_key",
            "type": "string"
          }
        ]
      },
      {
        "name": "fioreqsts",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "fio_request_id",
            "type": "uint64"
          },
          {
            "name": "status",
            "type": "uint64"
          },
          {
            "name": "metadata",
            "type": "string"
          },
          {
            "name": "time_stamp",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "recordobt",
        "base": "",
        "fields": [
          {
            "name": "fio_request_id",
            "type": "string"
          },
          {
            "name": "payer_fio_address",
            "type": "string"
          },
          {
            "name": "payee_fio_address",
            "type": "string"
          },
          {
            "name": "content",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "string"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "newfundsreq",
        "base": "",
        "fields": [
          {
            "name": "payer_fio_address",
            "type": "string"
          },
          {
            "name": "payee_fio_address",
            "type": "string"
          },
          {
            "name": "content",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "string"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "rejectfndreq",
        "base": "",
        "fields": [
          {
            "name": "fio_request_id",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "string"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "cancelfndreq",
        "base": "",
        "fields": [
          {
            "name": "fio_request_id",
            "type": "string"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "string"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "recordobt",
        "type": "recordobt",
        "ricardian_contract": ""
      },
      {
        "name": "newfundsreq",
        "type": "newfundsreq",
        "ricardian_contract": ""
      },
      {
        "name": "rejectfndreq",
        "type": "rejectfndreq",
        "ricardian_contract": ""
      },
      {
        "name": "cancelfndreq",
        "type": "cancelfndreq",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "fioreqctxts",
        "index_type": "i64",
        "key_names": [
          "fio_request_id"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "fioreqctxt"
      },
      {
        "name": "recordobts",
        "index_type": "i64",
        "key_names": [
          "id"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "recordobt_info"
      },
      {
        "name": "fioreqstss",
        "index_type": "i64",
        "key_names": [
          "id"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "fioreqsts"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioTokenAbi = []byte(`{
    "version": "eosio::abi/1.1",
    "types": [],
    "structs": [
      {
        "name": "account",
        "base": "",
        "fields": [
          {
            "name": "balance",
            "type": "asset"
          }
        ]
      },
      {
        "name": "create",
        "base": "",
        "fields": [
          {
            "name": "maximum_supply",
            "type": "asset"
          }
        ]
      },
      {
        "name": "currency_stats",
        "base": "",
        "fields": [
          {
            "name": "supply",
            "type": "asset"
          },
          {
            "name": "max_supply",
            "type": "asset"
          },
          {
            "name": "issuer",
            "type": "name"
          }
        ]
      },
      {
        "name": "issue",
        "base": "",
        "fields": [
          {
            "name": "to",
            "type": "name"
          },
          {
            "name": "quantity",
            "type": "asset"
          },
          {
            "name": "memo",
            "type": "string"
          }
        ]
      },
      {
        "name": "mintfio",
        "base": "",
        "fields": [
          {
            "name": "to",
            "type": "name"
          },
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "retire",
        "base": "",
        "fields": [
          {
            "name": "quantity",
            "type": "asset"
          },
          {
            "name": "memo",
            "type": "string"
          }
        ]
      },
      {
        "name": "transfer",
        "base": "",
        "fields": [
          {
            "name": "from",
            "type": "name"
          },
          {
            "name": "to",
            "type": "name"
          },
          {
            "name": "quantity",
            "type": "asset"
          },
          {
            "name": "memo",
            "type": "string"
          }
        ]
      },
      {
        "name": "trnsfiopubky",
        "base": "",
        "fields": [
          {
            "name": "payee_public_key",
            "type": "string"
          },
          {
            "name": "amount",
            "type": "int64"
          },
          {
            "name": "max_fee",
            "type": "int64"
          },
          {
            "name": "actor",
            "type": "name"
          },
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "create",
        "type": "create",
        "ricardian_contract": ""
      },
      {
        "name": "issue",
        "type": "issue",
        "ricardian_contract": ""
      },
      {
        "name": "mintfio",
        "type": "mintfio",
        "ricardian_contract": ""
      },
      {
        "name": "retire",
        "type": "retire",
        "ricardian_contract": ""
      },
      {
        "name": "transfer",
        "type": "transfer",
        "ricardian_contract": ""
      },
      {
        "name": "trnsfiopubky",
        "type": "trnsfiopubky",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "accounts",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "account"
      },
      {
        "name": "stat",
        "index_type": "i64",
        "key_names": [],
        "key_types": [],
        "type": "currency_stats"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioTpidAbi = []byte(`{
    "version": "eosio::abi/1.1",
    "types": [],
    "structs": [
      {
        "name": "tpid",
        "base": "",
        "fields": [
          {
            "name": "id",
            "type": "uint64"
          },
          {
            "name": "fioaddhash",
            "type": "uint128"
          },
          {
            "name": "fioaddress",
            "type": "string"
          },
          {
            "name": "rewards",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bounty",
        "base": "",
        "fields": [
          {
            "name": "tokensminted",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "updatetpid",
        "base": "",
        "fields": [
          {
            "name": "tpid",
            "type": "string"
          },
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "rewardspaid",
        "base": "",
        "fields": [
          {
            "name": "tpid",
            "type": "string"
          }
        ]
      },
      {
        "name": "updatebounty",
        "base": "",
        "fields": [
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "updatetpid",
        "type": "updatetpid",
        "ricardian_contract": ""
      },
      {
        "name": "rewardspaid",
        "type": "rewardspaid",
        "ricardian_contract": ""
      },
      {
        "name": "updatebounty",
        "type": "updatebounty",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "tpids",
        "index_type": "i64",
        "key_names": [
          "id"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "tpid"
      },
      {
        "name": "bounties",
        "index_type": "i64",
        "key_names": [
          "tokensminted"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "bounty"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)

var fioTreasuryAbi = []byte(`{
    "version": "eosio::abi/1.1",
    "types": [],
    "structs": [
      {
        "name": "tpidclaim",
        "base": "",
        "fields": [
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "startclock",
        "base": "",
        "fields": []
      },
      {
        "name": "bpclaim",
        "base": "",
        "fields": [
          {
            "name": "fio_address",
            "type": "string"
          },
          {
            "name": "actor",
            "type": "name"
          }
        ]
      },
      {
        "name": "treasurystate",
        "base": "",
        "fields": [
          {
            "name": "lasttpidpayout",
            "type": "uint64"
          },
          {
            "name": "payschedtimer",
            "type": "uint64"
          },
          {
            "name": "rewardspaid",
            "type": "uint64"
          },
          {
            "name": "reservetokensminted",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bpreward",
        "base": "",
        "fields": [
          {
            "name": "rewards",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "fdtnreward",
        "base": "",
        "fields": [
          {
            "name": "rewards",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "fdtnrwdupdat",
        "base": "",
        "fields": [
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bprewdupdate",
        "base": "",
        "fields": [
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bppoolupdate",
        "base": "",
        "fields": [
          {
            "name": "amount",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bucketpool",
        "base": "",
        "fields": [
          {
            "name": "rewards",
            "type": "uint64"
          }
        ]
      },
      {
        "name": "bppaysched",
        "base": "",
        "fields": [
          {
            "name": "owner",
            "type": "name"
          },
          {
            "name": "abpayshare",
            "type": "uint64"
          },
          {
            "name": "sbpayshare",
            "type": "uint64"
          },
          {
            "name": "votes",
            "type": "float64"
          }
        ]
      }
    ],
    "actions": [
      {
        "name": "tpidclaim",
        "type": "tpidclaim",
        "ricardian_contract": ""
      },
      {
        "name": "bpclaim",
        "type": "bpclaim",
        "ricardian_contract": ""
      },
      {
        "name": "startclock",
        "type": "startclock",
        "ricardian_contract": ""
      },
      {
        "name": "fdtnrwdupdat",
        "type": "fdtnrwdupdat",
        "ricardian_contract": ""
      },
      {
        "name": "bprewdupdate",
        "type": "bprewdupdate",
        "ricardian_contract": ""
      },
      {
        "name": "bppoolupdate",
        "type": "bppoolupdate",
        "ricardian_contract": ""
      }
    ],
    "tables": [
      {
        "name": "clockstate",
        "index_type": "i64",
        "key_names": [
          "lastrun"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "treasurystate"
      },
      {
        "name": "bprewards",
        "index_type": "i64",
        "key_names": [
          "rewards"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "bpreward"
      },
      {
        "name": "fdtnrewards",
        "index_type": "i64",
        "key_names": [
          "rewards"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "fdtnreward"
      },
      {
        "name": "bpbucketpool",
        "index_type": "i64",
        "key_names": [
          "rewards"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "bucketpool"
      },
      {
        "name": "voteshares",
        "index_type": "i64",
        "key_names": [
          "owner"
        ],
        "key_types": [
          "uint64"
        ],
        "type": "bppaysched"
      }
    ],
    "ricardian_clauses": [],
    "error_messages": [],
    "abi_extensions": [],
    "variants": []
  }
`)
