package transform

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/fioprotocol/fio-go"
	"github.com/fioprotocol/fio-go/eos"
	"github.com/fioprotocol/fio-go/eos/ecc"
	"github.com/mr-tron/base58"
	"github.com/sasha-s/go-deadlock"
	"golang.org/x/crypto/ripemd160"
	"sort"
	"strconv"
	"strings"
	"time"
)

type MsgData struct {
	Data json.RawMessage `json:"data"`
}

// Schedule duplicates an eos.OptionalProducerSchedule but adds some metadata
type Schedule struct {
	Id              string              `json:"id"`
	RecordType      string              `json:"record_type"`
	Producers       []ProducerKeyString `json:"producers"`
	ScheduleVersion interface{}         `json:"schedule"`
	BlockNum        interface{}         `json:"block_num"`
	BlockTime       time.Time           `json:"block_time"`
}

type ProducerSchedule struct {
	Version   interface{}       `json:"version"`
	Producers []eos.ProducerKey `json:"producers"`
}

type ProducerScheduleString struct {
	Version   uint32              `json:"version"`
	Producers []ProducerKeyString `json:"producers"`
}

type ProducerKeyString struct {
	AccountName     string `json:"producer_name"`
	BlockSigningKey string `json:"block_signing_key"`
}

// FullBlock duplicates eos.SignedBlock because the provided json has metadata
type FullBlock struct {
	RecordType string      `json:"record_type"`
	BlockTime  time.Time   `json:"block_time"`
	Block      SignedBlock `json:"block"`
	BlockNum   interface{} `json:"block_num"`
	BlockId    string      `json:"id"`
}

type SignedBlock struct {
	RecordType string `json:"record_type"`
	SignedBlockHeader
	Transactions    []map[string]interface{} `json:"transactions"`
	BlockExtensions json.RawMessage          `json:"block_extensions"`
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
	Timestamp        eos.BlockTimestamp `json:"timestamp"`
	Producer         eos.AccountName    `json:"producer"`
	Confirmed        interface{}        `json:"confirmed"`
	Previous         eos.Checksum256    `json:"previous"`
	TransactionMRoot eos.Checksum256    `json:"transaction_mroot"`
	ActionMRoot      eos.Checksum256    `json:"action_mroot"`
	ScheduleVersion  interface{}        `json:"schedule_version"`
	//NewProducers     *ProducerSchedule  `json:"new_producers" eos:"optional"`
	NewProducers     map[string]interface{} `json:"new_producers" eos:"optional"`
	HeaderExtensions []*Extension           `json:"header_extensions"`
	deadlock.Mutex
}

// BadK1SumToPub handles an issue where we are getting invalid checksums on public keys
func BadK1SumToPub(pk string) (ecc.PublicKey, string, error) {
	// strip PUB_K1_ prefix, convert to []byte
	bin, err := base58.Decode(pk[7:])
	if err != nil {
		return ecc.PublicKey{}, "", err
	}

	// build a new *valid* checksum
	h := ripemd160.New()
	h.Write(bin[:len(bin)-4])
	sum := h.Sum(nil)

	// convert to string with FIO prefix and new checksum
	pub, err := ecc.NewPublicKey("FIO" + base58.Encode(append(bin[:len(bin)-4], sum[:4]...)))
	if err != nil {
		return ecc.PublicKey{}, "", err
	}
	return pub, "FIO" + base58.Encode(append(bin[:len(bin)-4], sum[:4]...)), err
}

func (b *BlockHeader) BlockNumber() uint32 {
	return binary.BigEndian.Uint32(b.Previous[:4]) + 1
}

func (b *BlockHeader) BlockID() (string, []ProducerKeyString, error) {
	b.Lock()
	defer b.Unlock()
	confirmed, _ := strconv.ParseUint(b.Confirmed.(string), 10, 16)
	b.Confirmed = uint16(confirmed)
	sv, _ := strconv.ParseUint(b.ScheduleVersion.(string), 10, 32)
	b.ScheduleVersion = uint32(sv)
	np := &eos.OptionalProducerSchedule{}
	producerList := make([]eos.ProducerKey, 0)
	newProds := make([]ProducerKeyString, 0)
	if b.NewProducers != nil {
		v, _ := strconv.ParseUint(b.NewProducers["version"].(string), 10, 32)
		b.NewProducers["version"] = uint32(v)
		np.Version = uint32(v)
		npb, err := json.Marshal(b.NewProducers["producers"])
		if err == nil {
			e := json.Unmarshal(npb, &newProds)
			if e != nil {
				elog.Println(e)
				return "", nil, e
			}
			for i, prod := range newProds {
				// since we get PUB_K1 keys, this will force them to the short format
				pub, _, err := BadK1SumToPub(prod.BlockSigningKey)
				if err != nil {
					elog.Println(err)
					continue
				}
				newProds[i].BlockSigningKey = pub.String()
				producerList = append(producerList, eos.ProducerKey{
					AccountName:     eos.AccountName(prod.AccountName),
					BlockSigningKey: pub,
				})
			}
		} else {
			elog.Println(err)
			return "", nil, err
		}
		// has to be in order, remember we came from a map...
		sort.Slice(producerList, func(i, j int) bool {
			in, _ := eos.StringToName(string(producerList[i].AccountName))
			jn, _ := eos.StringToName(string(producerList[j].AccountName))
			return in < jn
		})
		np.Producers = producerList
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
		return "", nil, err
	}

	h := sha256.New()
	_, _ = h.Write(cereal)
	hashed := h.Sum(nil)
	binary.BigEndian.PutUint32(hashed, b.BlockNumber())
	return hex.EncodeToString(hashed), newProds, nil
}

// Block splits a block into the header and a schedule (if present), it also calculates block number and id
func Block(b []byte, fallbackUrl string) (header json.RawMessage, schedule json.RawMessage, err error) {
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
	optProducers := make([]ProducerKeyString, 0)
	block.BlockId, optProducers, err = block.Block.BlockHeader.BlockID()
	if err != nil || block.BlockId == "" {
		func() {
			elog.Printf("ERROR: did not get block id, falling back to api on %s to get block id\n", fallbackUrl)
			api, _, err := fio.NewConnection(nil, fallbackUrl)
			if err != nil {
				elog.Println(err)
				return
			}
			gbn, err := api.GetBlockByNum(block.BlockNum.(uint32))
			if err != nil {
				elog.Println(err)
				return
			}
			bid, err := gbn.BlockID()
			if err != nil {
				elog.Println(err)
				return
			}
			block.BlockId = bid.String()
		}()
	}
	if block.BlockId == "" {
		block.BlockId = fmt.Sprintf("block-id-error-%v", block.BlockNum)
		elog.Println("ERROR: could not derive a block ID for block ", block.BlockNum)
	}
	block.BlockTime = block.Block.BlockHeader.Timestamp.Time
	for _, trx := range block.Block.Transactions {
		if s, ok := trx["trx"].(string); ok {
			trx["trx"] = map[string]string{"bytes": s}
		}
		trx = Fixup(trx)
	}
	if block.Block.NewProducers != nil {
		sched := Schedule{
			RecordType:      "schedule",
			Id:              fmt.Sprintf("sched-%v-%v", block.BlockNum, block.Block.Timestamp.Time),
			Producers:       optProducers,
			ScheduleVersion: block.Block.NewProducers["version"],
			BlockNum:        block.BlockNum.(int64),
			BlockTime:       block.Block.Timestamp.Time,
		}
		if len(optProducers) > 0 {
			block.Block.NewProducers["producers"] = optProducers
		}
		schedule, err = json.Marshal(&sched)
		if err != nil {
			elog.Println(err)
			schedule = nil
		}
	}
	header, err = json.Marshal(block)
	return
}

type AccountUpdate struct {
	Id         string          `json:"id"`
	RecordType string          `json:"record_type"`
	BlockNum   interface{}     `json:"block_num"`
	BlockTime  string          `json:"block_timestamp"`
	Data       json.RawMessage `json:"data"`
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
