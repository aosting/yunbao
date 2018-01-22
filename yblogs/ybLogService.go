package yblogs

import (
	"time"
	"os"
	"github.com/aliyun/aliyun-log-go-sdk"
	"strings"
	"github.com/gogo/protobuf/proto"
	"errors"
	"sync"
	"github.com/robfig/cron"
)

//日志缓存数组
type ArrayList struct {
	put []*sls.Log
	sync.Mutex
}

//外部传递的日志格式
type OutLogMap struct {
	LogMap map[string]string
}

type StoreMap struct {
	OutSize  int
	LogArray ArrayList
	Project  sls.LogProject
}

var addMute sync.Mutex
var removeMute sync.Mutex
var logMapstore = make(map[string]StoreMap)
var storeNames = make([]string, 0)
var initbool = false

//添加定时任务，用户版本蓝绿发布后将流量都切换到新镜像后，确保内存中缓存的日志会同步日志服务。
func task() {
	DEBUG("定时任务启动...............")
	c := cron.New()
	//2s跑一次
	spec4 := "*/5 * * * * *"
	c.AddFunc(spec4, func() {
		for index, _ := range storeNames {
			logStorename := storeNames[index]
			out, error := PopLogQueueALL(logStorename)
			if error != nil {
				continue
			}
			if out != nil && len(out) > 0 {
				project := logMapstore[logStorename].Project
				go Writelog(&project, logStorename, out)
			}
		}
	})
	c.Start()

}

//初始化配置
func Init(storeMap map[string]StoreMap, names []string) {
	if !initbool {
		logMapstore = storeMap
		storeNames = names
		initbool = true
		DEBUG("ybLogService  init succ")
		task()
	}
}

//var arraylist = ArrayList{}

//入队列
func PushLogQueue(logStorename string, outlog OutLogMap) (err error) {
	addMute.Lock()
	num := 0
	defer func() {
		addMute.Unlock()
		if num > logMapstore[logStorename].OutSize {
			//从缓存抛出日志
			tmp, error := PopLogQueue(logStorename, logMapstore[logStorename].OutSize)
			if error == nil {
				project := logMapstore[logStorename].Project
				//日志组写入日志服务
				go Writelog(&project, logStorename, tmp)
			}
		}
	}()

	if !initbool {
		return errors.New("ybLogService not init")
	}
	content := []*sls.LogContent{}
	if outlog.LogMap == nil {
		errorMsg := "outlog is nil"
		return errors.New(errorMsg)
	}
	for key := range outlog.LogMap {
		if !isBlank(outlog.LogMap[key]) {
			content = append(content, &sls.LogContent{
				Key:   proto.String(key),
				Value: proto.String(outlog.LogMap[key]),
			})
		}
	}
	log := &sls.Log{
		Time:     proto.Uint32(uint32(time.Now().Unix())),
		Contents: content,
	}
	num, error := add(logStorename, log)
	if error != nil {
		return error
	}

	return nil
}

//出队列
func PopLogQueue(logStorename string, size int) ([]*sls.Log, error) {
	return remove(logStorename, size)
}

//出队列
func PopLogQueueALL(logStorename string) ([]*sls.Log, error) {
	return removeALL(logStorename)
}

func Writelog(project *sls.LogProject, logstore_name string, logs []*sls.Log) {

	var retry_times int
	var logstore *sls.LogStore
	var err error
	for retry_times = 0; ; retry_times++ {
		if retry_times > 5 {
			return
		}
		logstore, err = project.GetLogStore(logstore_name)
		if err != nil {
			DEBUG("GetLogStore fail, retry, err:%v\n", retry_times, err)
			if strings.Contains(err.Error(), sls.PROJECT_NOT_EXIST) {
				return
			} else if strings.Contains(err.Error(), sls.LOGSTORE_NOT_EXIST) {
				err = project.CreateLogStore(logstore_name, 1, 2)
				if err != nil {
					WARN("CreateLogStore fail, err: ", err.Error())
				} else {
					INFO("CreateLogStore success")
				}
			}
		} else {
			INFO("GetLogStore success, retry:%d, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", retry_times, logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}
	host, _ := os.Hostname()
	loggroup := &sls.LogGroup{
		Topic:  proto.String(""),
		Source: proto.String(host),
		Logs:   logs,
	}
	// PostLogStoreLogs API Ref: https://intl.aliyun.com/help/doc-detail/29026.htm
	for retry_times = 0; retry_times < 10; retry_times++ {
		err := logstore.PutLogs(loggroup)
		if err == nil {
			INFO("PutLogs success, retry: %d\n", retry_times)
			break
		} else {
			WARN("PutLogs fail, retry: %d, err: %s\n", retry_times, err)
			//handle exception here, you can add retryable erorrCode, set appropriate put_retry
			if strings.Contains(err.Error(), sls.WRITE_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.PROJECT_QUOTA_EXCEED) || strings.Contains(err.Error(), sls.SHARD_WRITE_QUOTA_EXCEED) {
				//mayby you should split shard
				time.Sleep(1000 * time.Millisecond)
			} else if strings.Contains(err.Error(), sls.INTERNAL_SERVER_ERROR) || strings.Contains(err.Error(), sls.SERVER_BUSY) {
				time.Sleep(200 * time.Millisecond)
			}
		}
	}
}

func add(logStorename string, slslog *sls.Log) (log_put_size int, err error) {

	if storeMap, ok := logMapstore[logStorename]; ok {
		storeMap.LogArray.append(slslog)
		num := storeMap.LogArray.size()
		INFO("numis ", num, slslog)
		logMapstore[logStorename] = storeMap
		return num, nil
	} else {

		return -1, errors.New("not have " + logStorename)
	}
}

func remove(logStorename string, size int) ([]*sls.Log, error) {

	if storeMap, ok := logMapstore[logStorename]; ok {
		putsize := storeMap.LogArray.size()
		if putsize >= size {
			out := storeMap.LogArray.getSizeBefore(size)
			logMapstore[logStorename] = storeMap
			return out, nil
		} else {
			out := storeMap.LogArray.getAll()
			logMapstore[logStorename] = storeMap
			return out, nil
		}
	} else {
		return nil, errors.New("not have " + logStorename)
	}

}

func removeALL(logStorename string) ([]*sls.Log, error) {
	if storeMap, ok := logMapstore[logStorename]; ok {
		a := storeMap.LogArray.getAll()
		logMapstore[logStorename] = storeMap
		return a, nil
	} else {
		return nil, errors.New("not have " + logStorename)
	}
}

//判断字符串是否为空串
func isBlank(str string) bool {
	str = strings.TrimSpace(str)
	if len(str) < 1 {
		return true
	}
	return false
}

// Appends an item to the concurrent slice
func (cs *ArrayList) append(put *sls.Log) {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	cs.put = append(cs.put, put)
}

func (cs *ArrayList) init() {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	cs.put = make([]*sls.Log, 0)
}

func (cs *ArrayList) set(put []*sls.Log) {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	cs.put = put
}

func (cs *ArrayList) size() int {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
		return 0
	}
	return len(cs.put)
}

func (cs *ArrayList) getSizeBefore(size int) []*sls.Log {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	out := cs.put[:size]
	cs.put = cs.put[size:]
	return out
}

//func (cs *ArrayList) getSizeAfter(size int) {
//	cs.Lock()
//	defer cs.Unlock()
//	if cs == nil {
//		cs = &ArrayList{}
//		cs.put = make([]*sls.Log, 0)
//	}
//	cs.put = cs.put[size:]
//}

func (cs *ArrayList) getAll() []*sls.Log {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	tmp := cs.put
	cs.put = make([]*sls.Log, 0)
	return tmp
}
