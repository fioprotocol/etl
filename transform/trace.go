package transform

import (
	"encoding/json"
	"strconv"
)

type TraceResult struct {
	Id         string      `json:"id"`
	RecordType string      `json:"record_type"`
	BlockNum   interface{} `json:"block_num"`
	BlockTime  string      `json:"block_timestamp"`
	Trace      FullTrace   `json:"trace"`
}

type FullTrace struct {
	NetUsageWords   string                 `json:"net_usage_words"`
	Scheduled       string                 `json:"scheduled"`
	Partial         map[string]interface{} `json:"partial"`
	AccountRamDelta string                 `json:"account_ram_delta"`
	NetUsage        string                 `json:"net_usage"`
	Elapsed         string                 `json:"elapsed"`
	ErrorCode       interface{}            `json:"error_code"`
	CpuUsageUs      string                 `json:"cpu_usage_us"`
	FailedDtrxTrace interface{}            `json:"failed_dtrx_trace"`
	Except          string                 `json:"except"`
	Status          string                 `json:"status"`
	Id              string                 `json:"id"`
	ActionTraces    []struct {
		ContextFree          string                 `json:"context_free"`
		Act                  map[string]interface{} `json:"act"`
		AccountRamDeltas     interface{}            `json:"account_ram_deltas"`
		ActionOrdinal        string                 `json:"action_ordinal"`
		Elapsed              string                 `json:"elapsed"`
		ErrorCode            string                 `json:"error_code"`
		Except               string                 `json:"except"`
		Receiver             string                 `json:"receiver"`
		CreatorActionOrdinal string                 `json:"creator_action_ordinal"`
		Receipt              map[string]interface{} `json:"receipt"`
		Console              string                 `json:"console"`
	} `json:"action_traces"`
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

func Trace(b []byte) (trace json.RawMessage, err error) {
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
	tr := &TraceResult{}
	err = json.Unmarshal(msg.Data, tr)
	if err != nil {
		return
	}
	tr.Id = id.Trace.Id
	tr.BlockNum, _ = strconv.ParseUint(tr.BlockNum.(string), 10, 32)
	tr.RecordType = "trace"
	for _, t := range tr.Trace.ActionTraces {
		if s, ok := t.Act["data"].(string); ok {
			t.Act["data"] = map[string]string{"raw": s}
		}
	}
	return json.Marshal(tr)
}