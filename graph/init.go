package fiograph

import (
	"log"
	"os"
)

var redisHost string

func init() {
	log.SetFlags(log.LstdFlags|log.Lshortfile)
	redisHost = os.Getenv("REDIS")
	if redisHost == "" {
		redisHost = "redis:6379"
	}
}
