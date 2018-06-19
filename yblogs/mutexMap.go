package yblogs

import (
	"github.com/aliyun/aliyun-log-go-sdk"
	"sync"
)

type StoreMap struct {
	OutSize  int
	LogArray ArrayList
	Project  sls.LogProject
}

type MutexMap struct {
	sync.RWMutex
	realMap map[string]StoreMap
}

func (mmap *MutexMap) getRealMap() map[string]StoreMap {
	mmap.RLock()
	defer mmap.RUnlock()
	return mmap.realMap
}

func (mmap *MutexMap) setRealMap(key string, storeMap StoreMap) {
	mmap.Lock()
	mmap.realMap[key] = storeMap
	mmap.Unlock()
}
