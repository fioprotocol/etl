package transform

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestBuildTrie(t *testing.T) {
	it, _, _ := BuildTrie()
	if !it.Has("/data/amount") {
		t.Error("missing data.amount")
	}
	if it.Has("/data/no_no_no") {
		t.Error("had data.no_no_no")
	}

	itWorks := map[string]interface{} {
		"data": map[string]interface{} {
			"amount": "123456",
		},
	}
	worked := Fixup(itWorks)
	type yes struct {
		Data struct{
			Amount int `json:"amount"`
		} `json:"data"`
	}
	j, err := json.Marshal(worked)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(j))
	y := &yes{}
	err = json.Unmarshal(j, y)
	if err != nil {
		t.Fatal(err)
	}
	switch y.Data.Amount {
	case 123456:
		fmt.Println("woot!")
	default:
		t.Error("y.Data.Amount not correct value")
	}

	tr := make(map[string]interface{})
	err = json.Unmarshal([]byte(traceJson), &tr)
	if err != nil {
		t.Fatal(err)
	}
	newAct := Fixup(tr["_source"].(map[string]interface{})["trace"].(map[string]interface{})["action_traces"].([]interface{})[0].(map[string]interface{}))
	j, err = json.MarshalIndent(newAct, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println(string(j))

}

const traceJson = `{
  "_index": "logstash-trace-2020.03",
  "_type": "_doc",
  "_id": "10a8312347bd5cd2283e7cc6e267060604615a52157ae4883e45c7521deda6c8",
  "_version": 1,
  "_score": null,
  "_source": {
    "@timestamp": "2020-03-25T00:06:21.500Z",
    "block_timestamp": "2020-03-25T00:06:21.500",
    "@version": "1",
    "id": "10a8312347bd5cd2283e7cc6e267060604615a52157ae4883e45c7521deda6c8",
    "block_num": 123,
    "type": "trace",
    "trace": {
      "failed_dtrx_trace": [],
      "status": "executed",
      "account_ram_delta": 0,
      "error_code": null,
      "action_traces": [
        {
          "action_ordinal": "1",
          "receiver": "fio.token",
          "error_code": "",
          "receipt": {
            "act_digest": "ed71eec61c3c8ea0ed81355a1ae28535ab61f5bc2195eb782bef095ce6a14256",
            "global_sequence": "2481",
            "receiver": "fio.token",
            "recv_sequence": "247",
            "code_sequence": "1",
            "abi_sequence": "1",
            "auth_sequence": [
              {
                "sequence": "937",
                "account": "eosio"
              }
            ]
          },
          "elapsed": "1694",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "trnsfiopubky",
            "data": {
              "payee_public_key": "FIO7RGc61PL1zi44MS6gDd6Rk9ts5HFHwNCHwXSefPqkjQprTiK53",
              "tpid": "",
              "actor": "eosio",
              "max_fee": "800000000000",
              "amount": "200000000000000"
            },
            "authorization": [
              {
                "actor": "eosio",
                "permission": "active"
              }
            ],
            "account": "fio.token"
          },
          "console": "",
          "creator_action_ordinal": "0",
          "account_ram_deltas": [
            {
              "delta": "240",
              "account": "eosio"
            }
          ]
        },
        {
          "action_ordinal": "2",
          "receiver": "eosio",
          "error_code": "",
          "receipt": {
            "act_digest": "ffb06d7666c20029d6d03e43703765c26097131cf99852a9f9e88628b8bb8f85",
            "global_sequence": "2483",
            "receiver": "eosio",
            "recv_sequence": "1432",
            "code_sequence": "2",
            "abi_sequence": "2",
            "auth_sequence": [
              {
                "sequence": "1468",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "128",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "newaccount",
            "data": {
              "name": "hcgltvoi23bx",
              "owner": {
                "keys": [
                  {
                    "weight": 1,
                    "key": "PUB_K1_7RGc61PL1zi44MS6gDd6Rk9ts5HFHwNCHwXSefPqkjQpnT4scf"
                  }
                ],
                "accounts": [],
                "threshold": 1,
                "waits": []
              },
              "active": {
                "keys": [
                  {
                    "weight": 1,
                    "key": "PUB_K1_7RGc61PL1zi44MS6gDd6Rk9ts5HFHwNCHwXSefPqkjQpnT4scf"
                  }
                ],
                "accounts": [],
                "threshold": 1,
                "waits": []
              },
              "creator": "fio.token"
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "eosio"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": [
            {
              "delta": "2996",
              "account": "hcgltvoi23bx"
            }
          ]
        },
        {
          "action_ordinal": "3",
          "receiver": "fio.address",
          "error_code": "",
          "receipt": {
            "act_digest": "e33a8c647545cd8561f5bb563319e7bd762c3d7f8b7f6c7344d0271930416cf7",
            "global_sequence": "2484",
            "receiver": "fio.address",
            "recv_sequence": "268",
            "code_sequence": "1",
            "abi_sequence": "1",
            "auth_sequence": [
              {
                "sequence": "1469",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "73",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "bind2eosio",
            "data": {
              "existing": false,
              "client_key": "FIO7RGc61PL1zi44MS6gDd6Rk9ts5HFHwNCHwXSefPqkjQprTiK53",
              "account": "hcgltvoi23bx"
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "fio.address"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": [
            {
              "delta": "334",
              "account": "fio.address"
            }
          ]
        },
        {
          "action_ordinal": "4",
          "receiver": "fio.treasury",
          "error_code": "",
          "receipt": {
            "act_digest": "e296abe6ceb8c0782f753ea44b928e1f0d96f26d3de02d8e62f557209d34c07b",
            "global_sequence": "2485",
            "receiver": "fio.treasury",
            "recv_sequence": "502",
            "code_sequence": "1",
            "abi_sequence": "1",
            "auth_sequence": [
              {
                "sequence": "1470",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "42",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "fdtnrwdupdat",
            "data": {
              "raw": "0000000000000000"
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "fio.treasury"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        },
        {
          "action_ordinal": "5",
          "receiver": "fio.treasury",
          "error_code": "",
          "receipt": {
            "act_digest": "1b72c1e5742f7eef55c725ad48016ac729f98c096a7ca213d94d6dc851587b5b",
            "global_sequence": "2486",
            "receiver": "fio.treasury",
            "recv_sequence": "503",
            "code_sequence": "1",
            "abi_sequence": "1",
            "auth_sequence": [
              {
                "sequence": "1471",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "34",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "bprewdupdate",
            "data": {
              "raw": "0000000000000000"
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "fio.treasury"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        },
        {
          "action_ordinal": "6",
          "receiver": "eosio",
          "error_code": "",
          "receipt": {
            "act_digest": "ed71eec61c3c8ea0ed81355a1ae28535ab61f5bc2195eb782bef095ce6a14256",
            "global_sequence": "2482",
            "receiver": "eosio",
            "recv_sequence": "1431",
            "code_sequence": "1",
            "abi_sequence": "1",
            "auth_sequence": [
              {
                "sequence": "938",
                "account": "eosio"
              }
            ]
          },
          "elapsed": "13",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "trnsfiopubky",
            "data": {
              "payee_public_key": "FIO7RGc61PL1zi44MS6gDd6Rk9ts5HFHwNCHwXSefPqkjQprTiK53",
              "tpid": "",
              "actor": "eosio",
              "max_fee": "800000000000",
              "amount": "200000000000000"
            },
            "authorization": [
              {
                "actor": "eosio",
                "permission": "active"
              }
            ],
            "account": "fio.token"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        },
        {
          "action_ordinal": "7",
          "receiver": "eosio",
          "error_code": "",
          "receipt": {
            "act_digest": "6869bc60bf7eda3aa30e6e1bebbfdf0644b22cecf2076da40064d1d7498e59b6",
            "global_sequence": "2487",
            "receiver": "eosio",
            "recv_sequence": "1433",
            "code_sequence": "2",
            "abi_sequence": "2",
            "auth_sequence": [
              {
                "sequence": "1472",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "61",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "unlocktokens",
            "data": {
              "actor": "eosio"
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "eosio"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        },
        {
          "action_ordinal": "8",
          "receiver": "eosio",
          "error_code": "",
          "receipt": {
            "act_digest": "dde52e65f00003010c3746da5b33f192b2df00b1a553fcba6bb89e01393728cf",
            "global_sequence": "2488",
            "receiver": "eosio",
            "recv_sequence": "1434",
            "code_sequence": "2",
            "abi_sequence": "2",
            "auth_sequence": [
              {
                "sequence": "1473",
                "account": "fio.token"
              }
            ]
          },
          "elapsed": "57",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "updatepower",
            "data": {
              "voter": "eosio",
              "updateonly": true
            },
            "authorization": [
              {
                "actor": "fio.token",
                "permission": "active"
              }
            ],
            "account": "eosio"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        },
        {
          "action_ordinal": "9",
          "receiver": "eosio",
          "error_code": "",
          "receipt": {
            "act_digest": "43ab727f2215ea233d98c5732b486002d6487d48ee98adc2d9fddf09593327c9",
            "global_sequence": "2489",
            "receiver": "eosio",
            "recv_sequence": "1435",
            "code_sequence": "2",
            "abi_sequence": "2",
            "auth_sequence": [
              {
                "sequence": "939",
                "account": "eosio"
              }
            ]
          },
          "elapsed": "57",
          "context_free": "false",
          "except": "",
          "act": {
            "name": "incram",
            "data": {
              "amount": "1024",
              "accountmn": "eosio"
            },
            "authorization": [
              {
                "actor": "eosio",
                "permission": "active"
              }
            ],
            "account": "eosio"
          },
          "console": "",
          "creator_action_ordinal": "1",
          "account_ram_deltas": []
        }
      ],
      "elapsed": 2478,
      "net_usage_words": 22,
      "scheduled": false,
      "cpu_usage_us": 1927,
      "except": "",
      "id": "10a8312347bd5cd2283e7cc6e267060604615a52157ae4883e45c7521deda6c8",
      "partial": {
        "context_free_data": [],
        "max_cpu_usage_ms": 0,
        "delay_sec": 0,
        "max_net_usage_words": 0,
        "transaction_extensions": [],
        "ref_block_prefix": 2933474417,
        "signatures": [
          "SIG_K1_K66oBGWB1uiTMWxEyxAMcjCv6BrRZUTJZ15pSJixadzbKb5KpB8zDYa5LDpBPYtJ8N7NCVpbJGpwnJ4aC6zYcywEj88aKq"
        ],
        "ref_block_num": 118,
        "expiration": "2020-03-25T00:06:49.000"
      },
      "net_usage": 176
    }
  },
  "fields": {
    "@timestamp": [
      "2020-03-25T00:06:21.500Z"
    ],
    "block_timestamp": [
      "2020-03-25T00:06:21.500Z"
    ],
    "trace.partial.expiration": [
      "2020-03-25T00:06:49.000Z"
    ]
  },
  "sort": [
    1585094781500
  ]
}`
