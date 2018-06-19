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
	Mu      sync.RWMutex
	RealMap map[string]StoreMap
}

func (mmap *MutexMap) getRealMap() map[string]StoreMap {
	return mmap.RealMap
}

func (mmap *MutexMap) setRealMap(key string, storeMap StoreMap) {
	mmap.Mu.Lock()
	mmap.RealMap[key] = storeMap
	mmap.Mu.Unlock()
}

