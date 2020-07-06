package transform

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/fioprotocol/fio-go/imports/eos-go"
	"github.com/fioprotocol/fio-go/imports/eos-go/ecc"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"
)

type MsgData struct {
	Data json.RawMessage `json:"data"`
}

// Schedule duplicates an eos.OptionalProducerSchedule but adds some metadata
type Schedule struct {
	Id string `json:"id"`
	RecordType string `json:"record_type"`
	ProducerSchedule `json:"producer_schedule"`
	BlockNum interface{} `json:"block_num"`
	BlockTime time.Time `json:"block_time"`
}

type ProducerSchedule struct {
	Version   interface{}        `json:"version"`
	Producers []eos.ProducerKey `json:"producers"`
}


// FullBlock duplicates eos.SignedBlock because the provided json has metadata
type FullBlock struct {
	RecordType string `json:"record_type"`
	BlockTime time.Time `json:"block_time"`
	Block SignedBlock `json:"block"`
	BlockNum interface{} `json:"block_num"`
	BlockId string `json:"id"`
}

type SignedBlock struct {
	RecordType string `json:"record_type"`
	SignedBlockHeader
	Transactions    []json.RawMessage `json:"transactions"`
	BlockExtensions json.RawMessage         `json:"block_extensions"`
}

type SignedBlockHeader struct {
	BlockHeader
	ProducerSignature ecc.Signature `json:"producer_signature"`
}

type Extension struct {
	Type interface{}
	Data eos.HexBytes
}

type BlockHeader struct {
	Timestamp        eos.BlockTimestamp            `json:"timestamp"`
	Producer         eos.AccountName               `json:"producer"`
	Confirmed        interface{}                    `json:"confirmed"`
	Previous         eos.Checksum256               `json:"previous"`
	TransactionMRoot eos.Checksum256               `json:"transaction_mroot"`
	ActionMRoot      eos.Checksum256               `json:"action_mroot"`
	ScheduleVersion  interface{}                    `json:"schedule_version"`
	NewProducers     *ProducerSchedule `json:"new_producers" eos:"optional"`
	HeaderExtensions []*Extension              `json:"header_extensions"`
	sync.Mutex
}

func (b *BlockHeader) BlockNumber() uint32 {
	b.Lock()
	defer b.Unlock()
	return binary.BigEndian.Uint32(b.Previous[:4]) + 1
}

func (b *BlockHeader) BlockID() (string, error) {
	b.Lock()
	defer b.Unlock()
	confirmed, _ := strconv.ParseUint(b.Confirmed.(string), 10, 16)
	b.Confirmed = uint16(confirmed)
	sv, _ := strconv.ParseUint(b.ScheduleVersion.(string), 10, 32)
	b.ScheduleVersion = uint32(sv)
	np := &eos.OptionalProducerSchedule{}
	if b.NewProducers != nil {
		v, _ := strconv.ParseUint(b.NewProducers.Version.(string), 10, 32)
		b.NewProducers.Version = uint32(v)
		np.Version = b.NewProducers.Version.(uint32)
		np.Producers = b.NewProducers.Producers
	} else {
		np = nil
	}
	ebh := &eos.BlockHeader{
		Timestamp:        b.Timestamp,
		Producer:         b.Producer,
		Confirmed:        b.Confirmed.(uint16),
		Previous:         b.Previous,
		TransactionMRoot: b.TransactionMRoot,
		ActionMRoot:      b.ActionMRoot,
		ScheduleVersion:  b.ScheduleVersion.(uint32),
		NewProducers:     np,
		HeaderExtensions: nil,
	}

	cereal, err := eos.MarshalBinary(ebh)
	if err != nil {
		return "", err
	}

	h := sha256.New()
	_, _ = h.Write(cereal)
	hashed := h.Sum(nil)
	binary.BigEndian.PutUint32(hashed, b.BlockNumber())
	return hex.EncodeToString(hashed), nil
}

// Block splits a block into the header and a schedule (if present), it also calculates block number and id
func Block(b []byte) (header json.RawMessage, schedule json.RawMessage, err error) {
	msg := &MsgData{}
	err = json.Unmarshal(b, msg)
	if err != nil || msg.Data == nil {
		return
	}
	block := &FullBlock{}
	err = json.Unmarshal(msg.Data, block)
	if err != nil {
		return
	}
	block.RecordType = "block"
	block.BlockNum, _ = strconv.ParseInt(block.BlockNum.(string), 10, 64)
	block.BlockId, _ = block.Block.BlockHeader.BlockID()
	block.BlockTime = block.Block.BlockHeader.Timestamp.Time
	if block.Block.NewProducers != nil {
		sched := Schedule{
			RecordType: "schedule",
			Id: fmt.Sprintf("sched-%v-%v", block.BlockNum,block.Block.Timestamp.Time),
			ProducerSchedule: *block.Block.NewProducers,
			BlockNum:         block.BlockNum.(int64),
			BlockTime:        block.Block.Timestamp.Time,
		}
		schedule, err = json.Marshal(&sched)
		if err != nil {
			log.Println(err)
			schedule = nil
		}
	}
	header, err = json.Marshal(block)
	return
}


type TraceResult struct {
	Id string `json:"id"`
	RecordType string `json:"record_type"`
	BlockNum interface{} `json:"block_num"`
	BlockTime string `json:"block_timestamp"`
	Trace json.RawMessage `json:"trace"`
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
	tr := &TraceResult{}
	err = json.Unmarshal(msg.Data, tr)
	if err != nil {
		return
	}
	tr.Id = id.Trace.Id
	tr.BlockNum, _ = strconv.ParseUint(tr.BlockNum.(string), 10, 32)
	tr.RecordType = "trace"
	return json.Marshal(tr)
}

type AccountUpdate struct {
	Id string `json:"id"`
	RecordType string `json:"record_type"`
	BlockNum interface{} `json:"block_num"`
	BlockTime string `json:"block_timestamp"`
	Data json.RawMessage `json:"data"`
}

func Account(b []byte, kind string) (trace json.RawMessage, err error) {
	msg := &MsgData{}
	err = json.Unmarshal(b, msg)
	if err != nil || msg.Data == nil {
		return
	}
	au := &AccountUpdate{}
	err = json.Unmarshal(msg.Data, au)
	if err != nil {
		return
	}
	h := sha256.New()
	h.Write(b)
	au.Id = hex.EncodeToString(h.Sum(nil))
	au.RecordType = strings.ToLower(kind)
	au.BlockNum, _ = strconv.ParseUint(au.BlockNum.(string), 10, 32)
	au.Data = msg.Data
	return json.Marshal(au)
}

type BlockFinished struct {
	Data struct {
		BlockNum string `json:"block_num"`
	} `json:"data"`
}

