package logging

import (
	"log"
	"os"
	"strings"
)


func Setup(prefix string) (err *log.Logger, info *log.Logger, debug *log.Logger)  {
	var (
		e *log.Logger
		i *log.Logger
		d *log.Logger
	)
	if len(prefix) < 18 {
		prefix = prefix + strings.Repeat(" ", 18-len(prefix))
	}
	e = log.New(os.Stderr, prefix, log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	e.SetPrefix(" ERROR: ")
	i = log.New(os.Stdout, prefix, log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	e.SetPrefix(" INFO: ")
	d = log.New(os.Stdout, prefix, log.Lshortfile|log.LstdFlags|log.Lmsgprefix)
	e.SetPrefix(" DEBUG: ")
	return e, i, d
}
