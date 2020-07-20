package transform

import (
	"encoding/json"
	"fmt"
	"strconv"
)

type TraceResult struct {
	Id         uint64          `json:"id"`
	TxId       string          `json:"txid"`
	RecordType string          `json:"record_type"`
	BlockNum   interface{}     `json:"block_num"`
	BlockTime  string          `json:"block_timestamp"`
	Trace      map[string]interface{} `json:"trace"`
}

type TraceActions struct {
	Trace struct {
		ActionTraces []json.RawMessage `json:"action_traces"`
	} `json:"trace"`
}

type TraceSequence struct {
	Receipt struct {
		GlobalSequence string `json:"global_sequence"`
	} `json:"receipt"`
	Act struct {
		Data interface{} `json:"data"`
	} `json:"act"`
}

type traceId struct {
	Trace struct {
		Id string `json:"id"`
	} `json:"trace"`
}

func Trace(b []byte) (traces []json.RawMessage, err error) {
	msg := &MsgData{}
	err = json.Unmarshal(b, msg)
	if err != nil || msg.Data == nil {
		return
	}
	id := &traceId{}
	err = json.Unmarshal(msg.Data, id)
	if err != nil {
		return
	}

	ta := &TraceActions{}
	err = json.Unmarshal(msg.Data, ta)
	if err != nil {
		return
	}
	traces = make([]json.RawMessage, 0)
	for _, t := range ta.Trace.ActionTraces {
		tr := &TraceResult{}
		err = json.Unmarshal(msg.Data, tr)
		if err != nil {
			return
		}
		seq := &TraceSequence{}
		err = json.Unmarshal(t, seq)
		if err != nil {
			return
		}
		tr.Id, err = strconv.ParseUint(seq.Receipt.GlobalSequence, 10, 64)
		if err != nil {
			return
		}
		tr.TxId = id.Trace.Id
		tr.BlockNum, _ = strconv.ParseUint(tr.BlockNum.(string), 10, 32)
		tr.RecordType = "trace"
		tr.Trace["action_traces"] = t
		if seq.Act.Data != nil {
			if seq.Act.Data.([]byte)[0] == '"' {
				seq.Act.Data = []byte(fmt.Sprintf(`{"raw":%s}`, seq.Act.Data.(string)))
			}
		}
		trace := make([]byte, 0)
		trace, err = json.Marshal(tr)
		if err != nil {
			return
		}
		traces = append(traces, trace)
	}
	return
}
