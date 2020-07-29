package transform

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestBuildTrie(t *testing.T) {
	_, _, b := BuildTrie()
	if !b.Has("/data/account_metadata/is_privileged") {
		t.Error("missing data.account_metadata.is_privileged")
	}
	if b.Has("/data/account_metadata/no_no_no") {
		t.Error("had data.account_metadata.no_no_no")
	}

	itWorks := map[string]interface{} {
		"data": map[string]interface{} {
			"added": "true",
		},
	}
	worked := Fixup(itWorks)
	type yes struct {
		Data struct{
			Added bool `json:"added"`
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
	switch y.Data.Added {
	case true:
		fmt.Println("woot!")
	default:
		t.Error("y.Data.Added no a bool")
	}
}
