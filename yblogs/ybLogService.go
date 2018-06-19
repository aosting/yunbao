package yblogs

import (
	"time"
	"github.com/aliyun/aliyun-log-go-sdk"
	"strings"
	"github.com/gogo/protobuf/proto"
	"errors"
	"sync"
	"github.com/robfig/cron"
	"os"
)

const (
	PUSH_TRY_NUMS = 10
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



var addMute sync.Mutex
var removeMute sync.Mutex
var logMapstore = MutexMap{
	realMap: make(map[string]StoreMap),
}

var storeNames = make([]string, 0)
var initbool = false

//添加定时任务，用户版本蓝绿发布后将流量都切换到新镜像后，确保内存中缓存的日志会同步日志服务。
func task() {
	DEBUG("定时任务启动...............")
	c := cron.New()
	//10s跑一次
	spec4 := "*/10 * * * * *"
	c.AddFunc(spec4, func() {
		for index, _ := range storeNames {
			logStorename := storeNames[index]
			out, error := PopLogQueueALL(logStorename)
			if error != nil {
				continue
			}
			if out != nil && len(out) > 0 {
				project := logMapstore.getRealMap()[logStorename].Project
				go Writelog(&project, logStorename, out)
			}
		}
	})
	c.Start()

}

//初始化配置
func Init(storeMap map[string]StoreMap, names []string) {
	if !initbool {
		logMapstore = MutexMap{
			realMap: storeMap,
		}
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
		if num > logMapstore.getRealMap()[logStorename].OutSize {
			//从缓存抛出日志
			tmp, error := PopLogQueue(logStorename, logMapstore.getRealMap()[logStorename].OutSize)
			if error == nil {
				project := logMapstore.getRealMap()[logStorename].Project
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
		//if !isBlank(outlog.LogMap[key]) {
		content = append(content, &sls.LogContent{
			Key:   proto.String(key),
			Value: proto.String(outlog.LogMap[key]),
		})
		//}
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
	for retry_times = 0; retry_times < PUSH_TRY_NUMS; retry_times++ {
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
			if retry_times == (PUSH_TRY_NUMS - 1) {
				DEBUG("PutLogs back ,nums is", len(logs))
				adds(logstore_name, logs)
			}
		}
	}
}

func adds(logStorename string, logs []*sls.Log) (log_put_size int, err error) {
	if storeMap, ok := logMapstore.getRealMap()[logStorename]; ok {
		storeMap.LogArray.appends(logs)
		num := storeMap.LogArray.size()
		logMapstore.setRealMap(logStorename,storeMap)
		return num, nil
	} else {

		return -1, errors.New("not have " + logStorename)
	}
}

func add(logStorename string, slslog *sls.Log) (log_put_size int, err error) {

	if storeMap, ok := logMapstore.getRealMap()[logStorename]; ok {
		storeMap.LogArray.append(slslog)
		num := storeMap.LogArray.size()
		INFO("numis ", num, slslog)
		logMapstore.setRealMap(logStorename,storeMap)
		return num, nil
	} else {

		return -1, errors.New("not have " + logStorename)
	}
}

func remove(logStorename string, size int) ([]*sls.Log, error) {

	if storeMap, ok := logMapstore.getRealMap()[logStorename]; ok {
		putsize := storeMap.LogArray.size()
		if putsize >= size {
			out := storeMap.LogArray.getSizeBefore(size)
			logMapstore.setRealMap(logStorename,storeMap)
			return out, nil
		} else {
			out := storeMap.LogArray.getAll()

			logMapstore.setRealMap(logStorename,storeMap)
			return out, nil
		}
	} else {
		return nil, errors.New("not have " + logStorename)
	}

}

func removeALL(logStorename string) ([]*sls.Log, error) {
	if storeMap, ok := logMapstore.getRealMap()[logStorename]; ok {
		a := storeMap.LogArray.getAll()
		logMapstore.setRealMap(logStorename,storeMap)
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

func (cs *ArrayList) appends(puts []*sls.Log) {
	cs.Lock()
	defer cs.Unlock()
	if cs == nil {
		cs = &ArrayList{}
		cs.put = make([]*sls.Log, 0)
	}
	cs.put = append(cs.put, puts...)
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

func Getlog(logstore_name string, t uint32, query string) []map[string]string {
	// pull logs from logstore
	project := logMapstore.getRealMap()[logstore_name].Project
	begin_time := uint32(time.Now().Unix())
	begin_time = begin_time - t
	end_time := uint32(time.Now().Unix())
	var retry_times int

	var logstore *sls.LogStore

	var err error
	list := make([]map[string]string, 0)
	for retry_times = 0; ; retry_times++ {
		if retry_times > 5 {
			return list
		}
		logstore, err = project.GetLogStore(logstore_name)
		if err != nil {
			WARN("GetLogStore fail, retry:%d, err:%v\n", retry_times, err)
			if strings.Contains(err.Error(), sls.PROJECT_NOT_EXIST) {
				return list
			} else if strings.Contains(err.Error(), sls.LOGSTORE_NOT_EXIST) {
				err = project.CreateLogStore(logstore_name, 1, 2)
				if err != nil {
					WARN("CreateLogStore fail, err: ", err.Error())
				} else {
					WARN("CreateLogStore success")
				}
			}
		} else {
			WARN("GetLogStore success, retry:%d, name: %s, ttl: %d, shardCount: %d, createTime: %d, lastModifyTime: %d\n", retry_times, logstore.Name, logstore.TTL, logstore.ShardCount, logstore.CreateTime, logstore.LastModifyTime)
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// search logs from index on logstore
	totalCount := int64(0)
	for {
		// GetHistograms API Ref: https://intl.aliyun.com/help/doc-detail/29030.htm
		ghResp, err := logstore.GetHistograms("", int64(begin_time), int64(end_time), query)
		if err != nil {
			WARN("GetHistograms fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		//fmt.Printf("complete: %s, count: %d, histograms: %v\n", ghResp.Progress, ghResp.Count, ghResp.Histograms)
		totalCount += ghResp.Count
		if ghResp.Progress == "Complete" {
			break
		}
	}
	offset := int64(0)
	// get logs repeatedly with (offset, lines) parameters to get complete result

	for offset < totalCount {
		// GetLogs API Ref: https://intl.aliyun.com/help/doc-detail/29029.htm
		glResp, err := logstore.GetLogs("", int64(begin_time), int64(end_time), query, 100, offset, false)
		if err != nil {
			WARN("GetLogs fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		WARN("Progress:%s, Count:%d, offset: %d\n", glResp.Progress, glResp.Count, offset)
		offset += glResp.Count
		if glResp.Count > 0 {
			//WARN("logs: %v\n", glResp.Logs)
			for _, v := range glResp.Logs {
				list = append(list, v)
			}
		}
		if glResp.Progress == "Complete" && glResp.Count == 0 {
			break
		}
	}
	return list
}

func Getlogbyday(logstore_name string, day int64, query string) []map[string]string {
	// pull logs from logstore

	t := time.Now()
	//tm1 := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	//today:=tm1.Unix()

	//begin_time := today-day*3600*24 day * (3600*24)
	//end_time := today-(day-1)*3600*24
	//更改为当前时间向前推24小时为单位. 
	begin_time := t.Unix() - (day * 86400)
	end_time := t.Unix()

	project := logMapstore.getRealMap()[logstore_name].Project
	var retry_times int

	var logstore *sls.LogStore

	var err error
	list := make([]map[string]string, 0)
	for retry_times = 0; ; retry_times++ {
		if retry_times > 5 {
			return list
		}
		logstore, err = project.GetLogStore(logstore_name)
		if err != nil {
			WARN("GetLogStore fail, retry:%d, err:%v\n", retry_times, err)
			if strings.Contains(err.Error(), sls.PROJECT_NOT_EXIST) {
				return list
			} else if strings.Contains(err.Error(), sls.LOGSTORE_NOT_EXIST) {
				err = project.CreateLogStore(logstore_name, 1, 2)
				if err != nil {
					WARN("CreateLogStore fail, err: ", err.Error())
				} else {
				}
			}
		} else {
			break
		}
		time.Sleep(200 * time.Millisecond)
	}

	// search logs from index on logstore
	totalCount := int64(0)
	for {
		// GetHistograms API Ref: https://intl.aliyun.com/help/doc-detail/29030.htm
		ghResp, err := logstore.GetHistograms("", int64(begin_time), int64(end_time), query)
		if err != nil {
			WARN("GetHistograms fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		//fmt.Printf("complete: %s, count: %d, histograms: %v\n", ghResp.Progress, ghResp.Count, ghResp.Histograms)
		totalCount += ghResp.Count
		if ghResp.Progress == "Complete" {
			break
		}
	}
	offset := int64(0)
	// get logs repeatedly with (offset, lines) parameters to get complete result

	for offset < totalCount {
		// GetLogs API Ref: https://intl.aliyun.com/help/doc-detail/29029.htm
		glResp, err := logstore.GetLogs("", int64(begin_time), int64(end_time), query, 1000, offset, false)
		if err != nil {
			WARN("GetLogs fail, err: %v\n", err)
			time.Sleep(10 * time.Millisecond)
			continue
		}
		offset += glResp.Count
		if glResp.Count > 0 {
			for _, v := range glResp.Logs {
				list = append(list, v)
			}
		}
		if glResp.Progress == "Complete" && glResp.Count == 0 {
			break
		}
	}
	return list
}
