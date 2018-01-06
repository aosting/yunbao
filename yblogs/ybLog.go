package yblogs

import (
	"log"
)

var (
	RELEASE = true
)

func INFO(v ...interface{}) {
	if !RELEASE {
		log.Println("INFO", v)
		//fmt.Println(v)
	}
}

func DEBUG(v ...interface{}) {
	log.Println("DEBUG", v)
	//fmt.Println(v)
}

func WARN(v ...interface{}) {
	log.Println("WARN", v)
}

func SUCC(v ...interface{}) {
	log.Println("SUCC", v)
}

func FAIL(v ...interface{}) {
	log.Println("FAIL", v)
}

func WORKLOG(businessName string, requestId string, ms int, responseCode string, Succ bool, v ...interface{}) {
	result := "WORKFAIL"
	if Succ {
		result = "WORKSUCC"
	}
	log.Println(businessName, requestId, ms, responseCode, result, v)
}
