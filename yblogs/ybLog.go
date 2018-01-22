package yblogs

import (
	"log"
	"time"
	"strconv"
	"github.com/twinj/uuid"
	"crypto/md5"
	"encoding/hex"
)

var (
	RELEASE = true
)

const (
	PLATFORM_SSP = "ssp"
	PLATFORM_NW  = "nw"

	DOCKERNAME_NEWLAND = "newland"

	DOCKERNAME_ADX_GDT  = "adx-gdt"
	DOCKERNAME_ADX_PP   = "adx-pp"
	DOCKERNAME_ADX_KS   = "adx-ks"
	DOCKERNAME_ADX_yyb  = "adx-yyb"
	DOCKERNAME_APK_LIST = "nw-apk-list"

	WORKLOG_STORE_NAME = "nw-sys-module-log"
)

func SetRelease(isRelease bool) {
	RELEASE = isRelease
}

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

func ERROR(v ...interface{}) {
	log.Println("ERROR", v)
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

func WORKLOG2(platform string, dockername string, requestId string, slotId string, responseCode string, ms int, Succ bool, msg string) {
	result := "WORKFAIL"
	if Succ {
		result = "WORKSUCC"
	}

	if isBlank(requestId) {
		requestId = Md5(uuid.NewV1().String())
	}
	tmp := make(map[string]string)
	tmp["request_timestamp"] = time.Now().Format("20060102150405")
	tmp["hour"] = time.Now().Format("15")
	tmp["platform"] = platform
	tmp["dockername"] = dockername
	tmp["requestId"] = requestId
	tmp["slotId"] = slotId
	tmp["responseCode"] = responseCode
	tmp["ms"] = strconv.Itoa(ms)
	tmp["result"] = result
	tmp["msg"] = msg

	outLog := OutLogMap{
		LogMap: tmp,
	}
	error := PushLogQueue(WORKLOG_STORE_NAME, outLog)
	if error != nil {
		WARN(error)
	}
}

func WORKLOG3(platform string, dockername string, requestId string, slotId string, responseCode string, ms int, Succ bool, msg string, values []string) {
	result := "WORKFAIL"
	if Succ {
		result = "WORKSUCC"
	}

	if isBlank(requestId) {
		requestId = Md5(uuid.NewV1().String())
	}
	tmp := make(map[string]string)
	tmp["request_timestamp"] = time.Now().Format("20060102150405")
	tmp["hour"] = time.Now().Format("15")
	tmp["platform"] = platform
	tmp["dockername"] = dockername
	tmp["requestId"] = requestId
	tmp["slotId"] = slotId
	tmp["responseCode"] = responseCode
	tmp["ms"] = strconv.Itoa(ms)
	tmp["result"] = result
	tmp["msg"] = msg

	for _, i := range values {
		tmp[i] = i
	}
	outLog := OutLogMap{
		LogMap: tmp,
	}
	error := PushLogQueue(WORKLOG_STORE_NAME, outLog)
	if error != nil {
		WARN(error)
	}
}

func Md5(b string) (tp string) {
	h := md5.New()
	h.Write([]byte(b))
	x := h.Sum(nil)
	y := make([]byte, 32)
	hex.Encode(y, x)

	return string(y)
}
