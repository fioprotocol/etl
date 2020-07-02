package transform

import "log"

var abis *abiMap

func init()  {
	var err error
	abis, err = newAbiMap()
	if err != nil {
		log.Fatal("building abi map: ", err)
	}
}

