package transform

import (
	"encoding/binary"
	"github.com/importcjj/trie-go"
	"log"
	"math"
	"regexp"
	"strconv"
	"strings"
)

var intTrie, floatTrie, boolTrie *trie.Trie

// Fixup applies type casting for fields that need changed. For example an amount that should be an int but is a string
func Fixup(fixme map[string]interface{}) map[string]interface{} {
	for _, t := range []string{"int", "bool", "float"} {
		seekFor(fixme, nil, t)
	}
	return fixme
}

// seekFor is a recurisve function that traverses the trie looking for fields to cast
func seekFor(target map[string]interface{}, leaf []string, kind string) {
	// for tests, if init() not run ....
	if intTrie == nil || boolTrie == nil || floatTrie == nil {
		intTrie, floatTrie, boolTrie = BuildTrie()
	}
	if leaf == nil {
		leaf = make([]string, 0)
	}
	for k := range target {
		s := strings.Join(append(leaf, k), "/")
		var valid bool
		switch kind {
		case "int":
			if intTrie.Has("/" + s) {
				valid = true
			}
		case "float":
			if floatTrie.Has("/" + s) {
				valid = true
			}
		case "bool":
			if boolTrie.Has("/" + s) {
				valid = true
			}
		}
		if !valid {
			continue
		}
		switch target[k].(type) {
		case nil:
			return
		case []interface{}:
			for _, row := range target[k].([]interface{}) {
				switch row.(type) {
				case map[string]interface{}:
					seekFor(row.(map[string]interface{}), append(leaf, k), kind)
				}
			}
		case map[string]interface{}:
			seekFor(target[k].(map[string]interface{}), append(leaf, k), kind)
		default:
			switch kind {
			case "int":
				target[k] = toInt(target[k])
			case "float":
				target[k] = toFloat(target[k])
			case "bool":
				target[k] = toBool(target[k])
			}
		}
	}
}

var (
	stripNotDigit = regexp.MustCompile(`(\d+)\D*?`) // capture the first group of consecutive numbers, will chop a float
	onlyDigit     = regexp.MustCompile(`^\d+$`)
	isAsset       = regexp.MustCompile(` FIO$`)
)

func toInt(v interface{}) interface{} {
	switch v.(type) {
	case int, int8, int16, int32, int64, uint8, uint16, uint32, uint64:
		// nothing to see here... move along.
		return v
	case string:
		if !onlyDigit.MatchString(v.(string)) {
			v = stripNotDigit.FindString(v.(string))
		}
		v, _ = strconv.ParseInt(v.(string), 10, 64)
		return v
	case []byte:
		msb := make([]byte, 8-len(v.([]byte)))
		v = binary.LittleEndian.Uint64(append(v.([]byte), msb...))
		return v
	case float32, float64:
		v = int64(math.Round(v.(float64)))
		return v
	}
	v = int64(0)
	return v
}

func toBool(v interface{}) interface{} {
	switch v.(type) {
	case bool:
		return v
	case string:
		v, _ = strconv.ParseBool(v.(string))
		return v
	}
	v = false
	return v
}

func toFloat(v interface{}) interface{} {
	switch v.(type) {
	case float64, float32:
		return v
	case string:
		if isAsset.MatchString(v.(string)) {
			v, _ = strconv.ParseFloat(strings.Split(v.(string), " ")[0], 64)
			return v
		}
		v, _ = strconv.ParseFloat(v.(string), 64)
		return v
	case int, int8, int16, int32, int64, uint8, uint16, uint32:
		v = float64(v.(int64))
	case uint64:
		if v.(uint64) > uint64(math.Round(math.MaxFloat64)) {
			v = math.MaxFloat64
		}
		v = float64(v.(uint64))
	}
	return v
}

func BuildTrie() (intTrie *trie.Trie, floatTrie *trie.Trie, boolTrie *trie.Trie) {
	intTrie = trie.New()
	_ = intTrie.Put("/", true)
	floatTrie = trie.New()
	_ = floatTrie.Put("/", true)
	boolTrie = trie.New()
	_ = boolTrie.Put("/", true)

	var err error
	mkTrie := func(leafs []string, t *trie.Trie) {
		for _, row := range leafs {
			leaf := strings.Split(row, ".")
			for i := range leaf {
				if !t.Has("/" + strings.Join(leaf[:i+1], "/")) {
					err = t.Put("/"+strings.Join(leaf[:i+1], "/"), true)
					if err != nil {
						elog.Fatal(err)
					}
				}
			}
		}
	}
	mkTrie(wantBool, boolTrie)
	mkTrie(wantFloat, floatTrie)
	mkTrie(wantInt, intTrie)
	if intTrie == nil || boolTrie == nil || floatTrie == nil {
		log.Fatal("could not init tries")
	}
	return
}

// for now only fixing up traces, but flexible enough for generic work:
var (
	wantBool = []string{
		//`trace.scheduled`,
	}
	wantFloat = []string{
		`act.data.quantity`,
		`data.quantity`,
	}
	wantInt = []string{
		`abi_sequence`,
		`account_ram_deltas.delta`,
		`act.data.amount`,
		`act.data.max_fee`,
		`act.data.suf_amount`,
		`action_ordinal`,
		`auth_sequence.sequence`,
		`code_sequence`,
		`creator_action_ordinal`,
		`data.amount`,
		`data.max_fee`,
		`data.quantity`,
		`data.suf_amount`,
		`elapsed`,
		`global_sequence`,
		`recv_sequence`,
		`receipt.global_sequence`,
		`receipt.code_sequence`,
		`receipt.recv_sequence`,
		`receipt.abi_sequence`,
	}
)
