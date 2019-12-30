package main

import (
	"sync"
	"time"
)

type PhantomObjectInfo struct {
	Key          Path
	LastModified time.Time
	Size         int64
	Mtx          sync.Mutex
}

func (info *PhantomObjectInfo) GetOne() PhantomObjectInfo {
	info.Mtx.Lock()
	defer info.Mtx.Unlock()
	return *info
}

func (info *PhantomObjectInfo) setKey(v Path) {
	info.Mtx.Lock()
	defer info.Mtx.Unlock()
	info.Key = v
}

func (info *PhantomObjectInfo) SetLastModified(v time.Time) {
	info.Mtx.Lock()
	defer info.Mtx.Unlock()
	info.LastModified = v
}

func (info *PhantomObjectInfo) SetSizeIfGreater(v int64) {
	info.Mtx.Lock()
	defer info.Mtx.Unlock()
	if v > info.Size {
		info.Size = v
	}
}

type phantomObjectInfoMap map[string]*PhantomObjectInfo

type PhantomObjectMap struct {
	perPrefixObjects map[string]phantomObjectInfoMap
	ptrToPOIMMapMap  map[*PhantomObjectInfo]phantomObjectInfoMap
	mtx              sync.Mutex
}

func (pom *PhantomObjectMap) add(info *PhantomObjectInfo) bool {
	prefix := info.Key.Prefix().String()
	m := pom.perPrefixObjects[prefix]
	if m == nil {
		m = phantomObjectInfoMap{}
		pom.perPrefixObjects[prefix] = m
	}
	prevInfo := m[info.Key.Base()]
	m[info.Key.Base()] = info
	pom.ptrToPOIMMapMap[info] = m
	if prevInfo != nil {
		delete(pom.ptrToPOIMMapMap, prevInfo)
	}
	return prevInfo == nil
}

func (pom *PhantomObjectMap) Add(info *PhantomObjectInfo) bool {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()
	return pom.add(info)
}

func (pom *PhantomObjectMap) remove(key Path) *PhantomObjectInfo {
	prefix := key.Prefix().String()
	m := pom.perPrefixObjects[prefix]
	if m == nil {
		return nil
	}
	info := m[key.Base()]
	if info == nil {
		return nil
	}
	delete(m, key.Base())
	if len(m) == 0 {
		delete(pom.perPrefixObjects, prefix)
	}
	delete(pom.ptrToPOIMMapMap, info)
	return info
}

func (pom *PhantomObjectMap) Remove(key Path) *PhantomObjectInfo {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()
	return pom.remove(key)
}

func (pom *PhantomObjectMap) removeByInfoPtr(info *PhantomObjectInfo) bool {
	m := pom.ptrToPOIMMapMap[info]
	if m == nil {
		return false
	}
	delete(m, info.Key.Base())
	if len(m) == 0 {
		delete(pom.perPrefixObjects, info.Key.Prefix().String())
	}
	delete(pom.ptrToPOIMMapMap, info)
	return true
}

func (pom *PhantomObjectMap) RemoveByInfoPtr(info *PhantomObjectInfo) bool {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()
	return pom.removeByInfoPtr(info)
}

func (pom *PhantomObjectMap) rename(old, new Path) bool {
	info := pom.remove(old)
	if info == nil {
		return false
	}
	info.setKey(new)
	pom.add(info)
	return true
}

func (pom *PhantomObjectMap) Rename(old, new Path) bool {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()
	return pom.rename(old, new)
}

func (pom *PhantomObjectMap) get(p Path) *PhantomObjectInfo {
	m := pom.perPrefixObjects[p.Prefix().String()]
	if m == nil {
		return nil
	}
	return m[p.Base()]
}

func (pom *PhantomObjectMap) Get(p Path) *PhantomObjectInfo {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()
	return pom.get(p)
}

func (pom *PhantomObjectMap) List(p Path) []*PhantomObjectInfo {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()

	m := pom.perPrefixObjects[p.String()]
	retval := make([]*PhantomObjectInfo, 0, len(m))
	for _, info := range m {
		retval = append(retval, info)
	}
	return retval
}

func (pom *PhantomObjectMap) Size() int {
	pom.mtx.Lock()
	defer pom.mtx.Unlock()

	return len(pom.ptrToPOIMMapMap)
}

func NewPhantomObjectMap() *PhantomObjectMap {
	return &PhantomObjectMap{
		perPrefixObjects: map[string]phantomObjectInfoMap{},
		ptrToPOIMMapMap:  map[*PhantomObjectInfo]phantomObjectInfoMap{},
	}
}
