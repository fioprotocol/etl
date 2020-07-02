package chronicle

import "log"

func init()  {
	// a little excessive, but sorta lines up with chronicle which is useful when watching both logs
	log.SetPrefix(" [fioetl] ")
	log.SetFlags(log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
}

