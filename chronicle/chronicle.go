package chronicle

import (
	"encoding/binary"
	"encoding/json"
	"sync"
)

type Record struct {
	Type string          `json:"type"`
	Fork bool            `json:"fork"`
	Data json.RawMessage `json:"data"`
}

type Block struct {
	Num uint32 `json:"num"`
	Fork bool            `json:"fork"`
	Records []*Record
}

type RecordBuf struct {
	Ring [240]int64
	Blocks map[int64]*Block
	msgTypes
}

func NewBuf() RecordBuf {
	// initialize with negatives so we have a full buffer
	buffer := RecordBuf{
		msgTypes: newMsgTypes(),
	}
	for i := range buffer.Ring {
		buffer.Ring[i] = -1
	}
	return buffer
}

/*
func (rb *RecordBuf) Push(data []byte) {
	// try not to leak memory, don't copy slices, and delete unused maps!
	var block uint32
	newBlock := func(){
		delete(rb.Blocks, rb.Ring[0])
		for i := 0; i < len(rb.Ring)-1; i++ {
			rb.Ring[i] = rb.Ring[i+1]
		}
		rb.Ring[239] = int64(block)
		rb.Blocks[rb.Ring[239]] = &Block{
			Num:     block,
			Records: make([]*Record, 1),
		}
		rb.Blocks[rb.Ring[239]].Records[0] = &Record{
			Type: rb.getType(data[:4]),
			Fork: false,
			Data: nil,
		}
	}
}
*/

type msgTypes struct {
	messages map[[4]byte]string
	msgMux   sync.RWMutex
}

func newMsgTypes() msgTypes {
	msgs := make(map[[4]byte]string)
	for k, v := range map[uint32]string{
		1001: "fork",
		1002: "block",
		1003: "tx",
		1004: "abi",
		1005: "abiRemoved",
		1006: "abiError",
		1007: "tableRow",
		1008: "encoderError",
		1009: "pause",
		1010: "blockCompleted",
		1011: "permission",
		1012: "permissionLink",
		1013: "accMetadata",
	} {
		b := make([]byte, 4, 4)
		binary.LittleEndian.PutUint32(b, k)
		msgs[[4]byte{b[0], b[1], b[2], b[3]}] = v
	}
	return msgTypes{messages: msgs}
}

func (rb RecordBuf) getType(msg []byte) string {
	rb.msgMux.RLock()
	s := rb.messages[[4]byte{msg[0], msg[1], msg[2], msg[3]}]
	rb.msgMux.RUnlock()
	if s == "" {
		s = "unknown"
	}
	return s
}
