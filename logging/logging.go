package logging

import (
	"log"
	"os"
	"strings"
)

func Setup(prefix string) (err *log.Logger, info *log.Logger, debug *log.Logger) {
	var (
		e *log.Logger
		i *log.Logger
		d *log.Logger
	)
	if len(prefix) < 24 {
		prefix = prefix + strings.Repeat(" ", 24-len(prefix))
	}
	e = log.New(os.Stderr, prefix+" ERROR: ", log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	i = log.New(os.Stdout, prefix+"  INFO: ", log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	d = log.New(os.Stdout, prefix+" DEBUG: ", log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	return e, i, d
}
