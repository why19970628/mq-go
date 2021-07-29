package lib

import "log"

func QuerySenderTransMsg(body string) bool {
	return false
}

func ConsumerMsg(body string) {
	log.Println("ConsumerMsg:", body)
}
