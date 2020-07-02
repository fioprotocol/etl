package chronicle

import "log"

func init()  {
	log.SetPrefix("fioetl   |    ")
	log.SetFlags(log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
}
