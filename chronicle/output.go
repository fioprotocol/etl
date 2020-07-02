package chronicle

import (
	"encoding/json"
	"fmt"
	"log"
)

func send(r Record) {
	// TODO: for now, dump to stdout
	j, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		log.Println(err)
	}
	fmt.Println(string(j))
}

