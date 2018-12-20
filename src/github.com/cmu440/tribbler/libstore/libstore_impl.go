package libstore

import (
	"errors"
	"github.com/cmu440/tribbler/rpc/librpc"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"net/rpc"
	"sort"
	"strings"
	"sync"
	"time"
	//"net"
	//"fmt"
)

type libstore struct {
	// TODO: implement this!
	mode     LeaseMode
	hostPort string
	clients  []*rpc.Client
	//listener net.Listener
	servers   []storagerpc.Node
	idSeq     []uint32
	serverMap map[uint32]*rpc.Client
	//below are fields needed for cache
	mux               sync.Mutex
	queryCacheSeconds int
	queryCacheThresh  int
	localCache        map[string]*CacheItem
	requestCounters   map[string]*RequestCounter
}

type CacheItem struct {
	data         interface{}
	cacheTime    time.Time
	validSeconds int
}

type RequestCounter struct {
	requestTime time.Time
	count       int
}

// NewLibstore creates a new instance of a TribServer's libstore. masterServerHostPort
// is the master storage server's host:port. myHostPort is this Libstore's host:port
// (i.e. the callback address that the storage servers should use to send back
// notifications when leases are revoked).
//
// The mode argument is a debugging flag that determines how the Libstore should
// request/handle leases. If mode is Never, then the Libstore should never request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to false). If mode is Always, then the Libstore should always request
// leases from the storage server (i.e. the GetArgs.WantLease field should always
// be set to true). If mode is Normal, then the Libstore should make its own
// decisions on whether or not a lease should be requested from the storage server,
// based on the requirements specified in the project PDF handout.  Note that the
// value of the mode flag may also determine whether or not the Libstore should
// register to receive RPCs from the storage servers.
//
// To register the Libstore to receive RPCs from the storage servers, the following
// line of code should suffice:
//
//     rpc.RegisterName("LeaseCallbacks", librpc.Wrap(libstore))
//
// Note that unlike in the NewTribServer and NewStorageServer functions, there is no
// need to create a brand new HTTP handler to serve the requests (the Libstore may
// simply reuse the TribServer's HTTP handler since the two run in the same process).
func NewLibstore(masterServerHostPort, myHostPort string, mode LeaseMode) (Libstore, error) {
	cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
	if err != nil {
		return nil, err
	}
	// try get server for five times
	args := &storagerpc.GetServersArgs{}
	reply := &storagerpc.GetServersReply{}
	ready := false
	for i := 0; i < 5; i++ {
		cli.Call("StorageServer.GetServers", args, reply)
		if reply.Status == storagerpc.OK {
			ready = true
			break
		}
		time.Sleep(1000 * time.Millisecond)
	}
	if !ready {
		return nil, errors.New("Storage Not Ready")
	}

	// fmt.println("debug")
	// cache the client end for each server
	clients := []*rpc.Client{cli}
	for i := 1; i < len(reply.Servers); i++ {
		node := reply.Servers[i]
		cli, err := rpc.DialHTTP("tcp", node.HostPort)
		if err != nil {
			return nil, err
		}
		clients = append(clients, cli)
	}
	// var idSeq []uint32 // a list of all virtual ids
	idSeq := make([]uint32, 0)
	// var serverMap map[uint32]*rpc.Client // a map of virtual ids to client ends
	serverMap := make(map[uint32]*rpc.Client)
	// append all ids and nodes to list and map
	// ** assume all IDs are different
	for i, node := range reply.Servers {
		ids := node.VirtualIDs
		for _, id := range ids {
			idSeq = append(idSeq, id)
			serverMap[id] = clients[i]
		}
	}
	sort.Slice(idSeq, func(i, j int) bool { return idSeq[i] < idSeq[j] })
	ls := &libstore{
		mode:              mode,
		hostPort:          myHostPort,
		clients:           clients,
		servers:           reply.Servers,
		idSeq:             idSeq,
		serverMap:         serverMap,
		queryCacheSeconds: storagerpc.QueryCacheSeconds,
		queryCacheThresh:  storagerpc.QueryCacheThresh,
		localCache:        make(map[string]*CacheItem),
		requestCounters:   make(map[string]*RequestCounter),
	}

	//listener, err := net.Listen("tcp", myHostPort)
	if err != nil {
		return nil, err
	}
	//ls.listener = listener
	err = rpc.RegisterName("LeaseCallbacks", librpc.Wrap(ls))
	if err != nil {
		return nil, err
	}
	go ls.cleanCache()
	return ls, nil
}

func (ls *libstore) Get(key string) (string, error) {
	cacheItem := ls.checkCache(key)
	if cacheItem != nil {
		content, ok := cacheItem.data.(string)
		if !ok {
			panic("stored data type for Get not string")
		}
		return content, nil
	}
	wantLease := ls.needLease(key)
	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}
	reply := &storagerpc.GetReply{}
	cli.Call("StorageServer.Get", args, reply)
	if reply.Status != storagerpc.OK {
		return "", errors.New("Libsotre Get: key not found")
	}
	if reply.Lease.Granted {
		ls.mux.Lock()
		cache := &CacheItem{
			data:         reply.Value,
			cacheTime:    time.Now(),
			validSeconds: reply.Lease.ValidSeconds,
		}
		ls.localCache[key] = cache
		ls.mux.Unlock()
	}
	//fmt.Println("Libsotre Get Success: key: ", key,  "Value: ", reply.Value)
	return reply.Value, nil
}

func (ls *libstore) Put(key, value string) error {
	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: value,
	}
	reply := &storagerpc.PutReply{}
	cli.Call("StorageServer.Put", args, reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Libsotre put failed")
	}
	return nil
}

func (ls *libstore) Delete(key string) error {
	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.DeleteArgs{
		Key: key,
	}
	reply := &storagerpc.DeleteReply{}
	cli.Call("StorageServer.Delete", args, reply)
	if reply.Status != storagerpc.OK {
		return errors.New("Libsotre delete failed")
	}
	return nil
}

func (ls *libstore) GetList(key string) ([]string, error) {
	cacheItem := ls.checkCache(key)
	if cacheItem != nil {
		content, ok := cacheItem.data.([]string)
		if !ok {
			panic("stored data type not []string")
		}
		return content, nil
	}
	wantLease := ls.needLease(key)

	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.GetArgs{
		Key:       key,
		WantLease: wantLease,
		HostPort:  ls.hostPort,
	}
	reply := &storagerpc.GetListReply{}
	cli.Call("StorageServer.GetList", args, reply)
	if reply.Status != storagerpc.OK {
		return []string{}, errors.New("nothing")
	}
	if reply.Lease.Granted {
		ls.mux.Lock()
		cache := &CacheItem{
			data:         reply.Value,
			cacheTime:    time.Now(),
			validSeconds: reply.Lease.ValidSeconds,
		}
		ls.localCache[key] = cache
		ls.mux.Unlock()
	}

	return reply.Value, nil
}

func (ls *libstore) RemoveFromList(key, removeItem string) error {
	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: removeItem,
	}
	reply := &storagerpc.PutReply{}
	cli.Call("StorageServer.RemoveFromList", args, reply)
	if reply.Status != storagerpc.OK {
		return errors.New("removeFromList: sth wrong")
	}
	return nil
}

func (ls *libstore) AppendToList(key, newItem string) error {
	hashVal := hashKey(key)
	id := findNextHighest(ls.idSeq, hashVal)
	cli := ls.serverMap[id]
	args := &storagerpc.PutArgs{
		Key:   key,
		Value: newItem,
	}
	reply := &storagerpc.PutReply{}
	cli.Call("StorageServer.AppendToList", args, reply)
	if reply.Status != storagerpc.OK {
		return errors.New("AppendToList: sth wrong")
	}
	return nil
}

func (ls *libstore) RevokeLease(args *storagerpc.RevokeLeaseArgs, reply *storagerpc.RevokeLeaseReply) error {
	key := args.Key
	ls.mux.Lock()
	defer ls.mux.Unlock()
	_, ok := ls.localCache[key]
	if ok {
		reply.Status = storagerpc.OK
		delete(ls.localCache, key)
		return nil
	}
	reply.Status = storagerpc.KeyNotFound
	return nil
	//return errors.New("not implemented")
}

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//								Custom Functions
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
func hashKey(key string) uint32 {
	//userString := strings.Split(key, ":")[0]
	return StoreHash(key)
}

func (ls *libstore) cleanCache() {
	for {
		ls.mux.Lock()
		for key, cache := range ls.localCache {
			if time.Since(cache.cacheTime).Seconds() >= float64(cache.validSeconds) {
				delete(ls.localCache, key)
			}
		}
		for k, counter := range ls.requestCounters {
			if time.Since(counter.requestTime).Seconds() >= float64(ls.queryCacheSeconds) {
				delete(ls.requestCounters, k)
			}
		}
		ls.mux.Unlock()
		time.Sleep(200 * time.Millisecond)
	}
}

//check if the key is cached already, if so return the item otherwise nil
func (ls *libstore) checkCache(key string) *CacheItem {
	ls.mux.Lock()
	defer ls.mux.Unlock()
	cache, ok := ls.localCache[key]
	if ok {
		if time.Since(cache.cacheTime).Seconds() < float64(cache.validSeconds) { //lease still valid
			return cache
		} else {
			delete(ls.localCache, key) // invalid cache, delete it
			return nil
		}
	}
	return nil

}

//add count for requestm return whether need to request cache
func (ls *libstore) needLease(key string) bool {
	ls.mux.Lock()
	defer ls.mux.Unlock()
	if ls.mode == Always {
		return true
	} else if ls.mode == Never || strings.Compare(ls.hostPort, "") == 0 {
		return false
	}
	requestCounter, ok := ls.requestCounters[key]
	if ok {
		requestCounter.count += 1
		start := requestCounter.requestTime
		passed := time.Since(start).Seconds()
		if passed <= float64(ls.queryCacheSeconds) {

			if requestCounter.count >= ls.queryCacheThresh {
				delete(ls.requestCounters, key)
				return true
			}
			return false
		} else { //queryCacheSeconds passed and at most requestCounter requests called
			delete(ls.requestCounters, key)
			return false
		}
	} else {
		now := time.Now()
		newCounter := RequestCounter{
			requestTime: now,
			count:       1,
		}
		ls.requestCounters[key] = &newCounter
		return false
	}

}

// returns the value at the next highest position of the sorted array
// assume there are no duplicates in the array
func findNextHighest(nums []uint32, target uint32) uint32 {
	var lo int
	var hi int
	lo = 0
	hi = len(nums)
	for lo < hi {
		mid := lo + (hi-lo)/2
		if nums[mid] == target {
			return nums[mid]
		} else if nums[mid] > target {
			hi = mid
		} else {
			lo = mid + 1
		}
	}
	index := lo + (hi-lo)/2
	if index >= len(nums) {
		return nums[0]
	} else {
		return nums[index]
	}
}
