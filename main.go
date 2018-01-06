package yunbao

import (
	"github.com/aliyun/aliyun-log-go-sdk"
	"fmt"
	"net/http"
	"log"
	"strconv"
	"sync/atomic"
	"time"
	"runtime/debug"
	"yunbao/yblogs"
)

var counter int64 = 0
var outsize = 1000

func main() {

	logstoreNames := make([]string, 0)
	logstoreNames = append(logstoreNames, "text")
	logMapstore := make(map[string]yblogs.StoreMap)
	logMapstore["text"] = yblogs.StoreMap{
		OutSize: 1000,
		Project: sls.LogProject{
			Name:            "api-fc-demo",
			Endpoint:        "cn-shanghai.log.aliyuncs.com",
			AccessKeyID:     "",
			AccessKeySecret: "",
		},
	}

	//初始化云宝日志服务，
	// 参数1：logMapstore，结构是key是logstorename,value是自定义的结构，包含日志上报数目上线，和日志projcet配置
	// 参数2：logstoreNames，就是logMapstore  的所有的key生成的slice
	yblogs.Init(logMapstore, logstoreNames)
	http.HandleFunc("/api/json", safeHandler(DefaultHandler))
	err := http.ListenAndServe(":6001", nil)
	if err != nil {
		log.Fatal("ListenAndServe:", err.Error())
	}
}

func DefaultHandler(w http.ResponseWriter, r *http.Request) {
	tmp := make(map[string]string)
	tmp["index"] = strconv.FormatInt(atomic.AddInt64(&counter, 1), 10)
	tmp["text"] = strconv.FormatInt(time.Now().Unix(), 10)
	addLog(tmp,"text")
	w.Write([]byte("ok"))
}

func addLog(tmp map[string]string, logname string) {
	//外部日志格式为map，key和value都必须为字符串
	outLog := yblogs.OutLogMap{
		LogMap: tmp,
	}
	error:=yblogs.PushLogQueue(logname, outLog)
	if error!=nil{
		yblogs.WARN(error)
	}
}

//闭包避免程序运行时出错崩溃: 所有handler都经过此方法,统一处理handler抛出的异常panic
func safeHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if e, ok := recover().(error); ok {
				http.Error(w, e.Error(), http.StatusInternalServerError)
				fmt.Println(fn, e)
				fmt.Println(string(debug.Stack()))
			}
		}()
		w.Header().Add("Access-Control-Allow-Origin", "*") //支持跨域
		w.Header().Add("Access-Control-Allow-Methods", "POST")
		w.Header().Add("Access-Control-Allow-Headers", "x-requested-with,content-type")
		fn(w, r)
	}
}
