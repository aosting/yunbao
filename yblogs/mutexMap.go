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
	return mmap.realMap
}

func (mmap *MutexMap) setRealMap(key string, storeMap StoreMap) {
	mmap.RLock()
	mmap.realMap[key] = storeMap
	mmap.RUnlock()
}
