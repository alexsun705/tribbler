package storageserver

import (
	"sync"
	//"errors"
	// "github.com/cmu440/tribbler/util"
	"github.com/cmu440/tribbler/rpc/storagerpc"
	"hash/fnv"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
	//"fmt"
)

type storageServer struct {
	// TODO: implement this!
	// userMap map[string][]string // this is basically a set of users
	listMap map[string]([]string) // user: post_id list or user_id list
	postMap map[string]string     // post_id: post

	numNodes  int                    // this is the expected number of servers in the ring
	servers   []storagerpc.Node      // this is all of servers in the ring now, only master server keeps track of this
	selfIDs   []uint32               // ids of itself
	ring      []uint32               // the whole ring of the system, pass before new server returns
	libCliMap map[string]*rpc.Client // the map from hostport string to client end

	leaseFlagMap map[string]bool // the flag map for lease granting
	//leaseChanMap map[string] chan int //channels for receiving revoke success signals

	cache map[string]([]*cacheItem) // from key to each libstore's info

	dataMux  sync.Mutex // mutex for access of listMap, PostMap, libCliMap
	flagMux  sync.Mutex // mutex for leaseFlagMap
	cacheMux sync.Mutex // mutex for cache

}

type cacheItem struct {
	start   time.Time
	cli     *rpc.Client
	revoked bool
}

// NewStorageServer creates and starts a new StorageServer. masterServerHostPort
// is the master storage server's host:port address. If empty, then this server
// is the master; otherwise, this server is a slave. numNodes is the total number of
// servers in the ring. port is the port number that this server should listen on.
// virtualIDs is a list of random, unsigned 32-bits IDs identifying this server.
//
// This function should return only once all storage servers have joined the ring,
// and should return a non-nil error if the storage server could not be started.
func NewStorageServer(masterServerHostPort string, numNodes, port int, virtualIDs []uint32) (StorageServer, error) {
	ss := new(storageServer)
	ss.listMap = make(map[string]([]string))
	ss.postMap = make(map[string]string)
	ss.numNodes = numNodes
	ss.selfIDs = virtualIDs
	ss.servers = make([]storagerpc.Node, 0)
	ss.cache = make(map[string]([]*cacheItem))
	ss.ring = make([]uint32, 0)
	ss.libCliMap = make(map[string]*rpc.Client)
	ss.leaseFlagMap = make(map[string]bool)
	//ss.leaseChanMap = make(map[string]chan int)
	// ss.mux = make(sync.Mutex)
	hostPort := net.JoinHostPort("localhost", strconv.Itoa(port))
	listener, err := net.Listen("tcp", hostPort)
	// fmt.Printf("Listener err: %v \n", err)
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	err = rpc.RegisterName("StorageServer", storagerpc.Wrap(ss))
	if err != nil {
		return nil, err
	}
	if masterServerHostPort == "" {
		// this is the master server
		// handle rpcs from libstore
		// register itself, no need to call register server, more conveniently
		self := storagerpc.Node{
			HostPort:   hostPort,
			VirtualIDs: virtualIDs,
		}
		ss.servers = append(ss.servers, self)
		// to ensure all servers have joined
		for {
			ss.dataMux.Lock()
			n := len(ss.servers)
			ss.dataMux.Unlock()
			if n == numNodes {
				break
			}
			time.Sleep(1000 * time.Millisecond)
		}
		// fmt.Println("Master: ring completed")
	} else {
		// this is a slave server

		cli, err := rpc.DialHTTP("tcp", masterServerHostPort)
		// if the master server is not set up, then err would be not nil
		for err != nil {
			cli, err = rpc.DialHTTP("tcp", masterServerHostPort)
			time.Sleep(500 * time.Millisecond)
		}
		//construct rpc args
		self := storagerpc.Node{
			HostPort:   hostPort,
			VirtualIDs: virtualIDs,
		}
		args := &storagerpc.RegisterArgs{
			ServerInfo: self,
		}
		reply := &storagerpc.RegisterReply{}
		// make the register rpc call
		cli.Call("StorageServer.RegisterServer", args, reply) // err is gonna be nil
		for reply.Status != storagerpc.OK {
			// sleep for one second
			time.Sleep(1000 * time.Millisecond)
			cli.Call("StorageServer.RegisterServer", args, reply)
		}
		// at this point the reply should contain all the servers
		// fmt.Println("Slave: ring completed")
		ss.servers = reply.Servers
	}
	// set up the consistent hashing ring in RegisterServer
	// set up the consistent hashing ring
	for _, node := range ss.servers {
		ids := node.VirtualIDs
		ss.ring = append(ss.ring, ids...)
	}
	sort.Slice(ss.ring, func(i, j int) bool { return ss.ring[i] < ss.ring[j] })
	//go ss.cleanCache()
	return ss, nil
}

func (ss *storageServer) RegisterServer(args *storagerpc.RegisterArgs, reply *storagerpc.RegisterReply) error {
	ss.dataMux.Lock()
	slave := args.ServerInfo
	id := slave.VirtualIDs
	// see if this server is registered
	registered := false
	for _, server := range ss.servers {
		id_r := server.VirtualIDs
		if compareVirtualIDs(id, id_r) {
			registered = true
			break
		}
	}
	if !registered {
		ss.servers = append(ss.servers, slave)
	}
	if len(ss.servers) == ss.numNodes {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers
		// set up the consistent hashing ring
		// to deal with a weird test case.
		ss.ring = make([]uint32, 0)
		for _, node := range ss.servers {
			ids := node.VirtualIDs
			ss.ring = append(ss.ring, ids...)
		}
		sort.Slice(ss.ring, func(i, j int) bool { return ss.ring[i] < ss.ring[j] })
	} else {
		reply.Status = storagerpc.NotReady
	}
	ss.dataMux.Unlock()
	return nil
}

func (ss *storageServer) GetServers(args *storagerpc.GetServersArgs, reply *storagerpc.GetServersReply) error {
	ss.dataMux.Lock()
	if len(ss.servers) < ss.numNodes {
		reply.Status = storagerpc.NotReady
	} else {
		reply.Status = storagerpc.OK
		reply.Servers = ss.servers // can use alias here because this list will not be changed once finished joining
	}
	ss.dataMux.Unlock()
	return nil
}

func (ss *storageServer) Get(args *storagerpc.GetArgs, reply *storagerpc.GetReply) error {

	key := args.Key
	wantLease := args.WantLease
	hostPort := args.HostPort
	//fmt.Println("Get key: ", key)
	lease := storagerpc.Lease{
		Granted:      false,
		ValidSeconds: storagerpc.LeaseSeconds,
	}
	if wantLease {
		//check if key is under append
		if !ss.keyBeingChanged(key) { //would grant lease
			//fmt.Println("checked key is not beingChanged")
			lease.Granted = true
			cli := ss.checkTCP(hostPort)
			//fmt.Println("checked tcp connection established")
			if cli == nil {
				panic("Dial TCP failed for port")
			}

			newItem := &cacheItem{
				start:   time.Now(),
				cli:     cli,
				revoked: false,
			}
			ss.insertCache(key, newItem)
			//fmt.Println("insertCache done")
		}
	}
	reply.Lease = lease

	ss.dataMux.Lock()

	value, ok := ss.postMap[key] // ***************
	//fmt.Println("In Get: Key: ", key," Value: ", value, " If there: ",ok)
	ss.dataMux.Unlock()

	var err error
	if ok {
		reply.Status = storagerpc.OK
		reply.Value = value
		err = nil
	} else {
		// need to decide between wrongserver and keynotfound
		hashVal := hash(key)
		// fmt.Printf("Inside get: ring %v \n", ss.ring)
		// fmt.Printf("Inside get: servers: %v \n", ss.servers)
		// fmt.Printf("Inside get: selfIDs: %v \n", ss.selfIDs)
		id := ss.findNextHighest(hashVal)
		if valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.WrongServer
		}
		//err = errors.New("keyNotFound")
		err = nil
	}
	// ss.mux.Unlock()
	//fmt.Println("Get returned Key: ", key, " Granted Lease: ", reply.Lease.Granted)
	return err
}

func (ss *storageServer) Delete(args *storagerpc.DeleteArgs, reply *storagerpc.DeleteReply) error {

	key := args.Key
	//fmt.Println("Delete, Key: ",key)
	// need to check if routed to the right server
	hashVal := hash(key)
	id := ss.findNextHighest(hashVal)
	if !valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageRevokeLease(key)

	ss.dataMux.Lock()
	_, ok := ss.postMap[key]
	ss.dataMux.Unlock()
	var err error
	if ok {
		reply.Status = storagerpc.OK
		ss.dataMux.Lock()
		delete(ss.postMap, key)
		ss.dataMux.Unlock()
		err = nil
	} else {
		// need to decide between wrongserver and keynotfound
		hashVal := hash(key)
		id := ss.findNextHighest(hashVal)
		if valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.WrongServer
		}
		//err = errors.New("keyNotFound")
		err = nil
	}

	return err
}

func (ss *storageServer) GetList(args *storagerpc.GetArgs, reply *storagerpc.GetListReply) error {
	key := args.Key
	wantLease := args.WantLease
	hostPort := args.HostPort

	lease := storagerpc.Lease{
		Granted:      false,
		ValidSeconds: storagerpc.LeaseSeconds,
	}
	if wantLease {
		//check if key is under append
		if !ss.keyBeingChanged(key) { //would grant lease
			lease.Granted = true
			cli := ss.checkTCP(hostPort)
			if cli == nil {
				panic("Dial TCP failed:")
			}
			reply.Lease = lease
			newItem := &cacheItem{
				start:   time.Now(),
				cli:     cli,
				revoked: false,
			}
			ss.insertCache(key, newItem)
		}
	}

	ss.dataMux.Lock()
	value, ok := ss.listMap[key] // ***************
	ss.dataMux.Unlock()

	var err error
	if ok {
		reply.Status = storagerpc.OK
		reply.Value = value
		err = nil
	} else {
		// need to decide between wrongserver and keynotfound
		hashVal := hash(key)
		// fmt.Printf("Inside get: ring %v \n", ss.ring)
		// fmt.Printf("Inside get: servers: %v \n", ss.servers)
		// fmt.Printf("Inside get: selfIDs: %v \n", ss.selfIDs)
		id := ss.findNextHighest(hashVal)
		if valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.WrongServer
		}
		//err = errors.New("keyNotFound")
		err = nil
	}
	// ss.mux.Unlock()
	return err
}

// helper function, use inside storageRevokeLease
func (ss *storageServer) sendRevokeLease(key string, item *cacheItem, leaseChan chan int) {
	args := &storagerpc.RevokeLeaseArgs{
		Key: key,
	}
	limit := float64(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds)
	timer := time.NewTimer(time.Duration(limit) * time.Second)

	reply := &storagerpc.RevokeLeaseReply{}
	err := item.cli.Call("LeaseCallbacks.RevokeLease", args, reply)
	for err != nil { // retry for unsuccessful roc
		select {
		case <-timer.C:
			return
		default:
			err = item.cli.Call("LeaseCallbacks.RevokeLease", args, reply)
		}

	}
	item.revoked = true
	leaseChan <- 1
}

// helper function, use with revoke lease requests
func (ss *storageServer) storageRevokeLease(key string) {
	// below is revoke process
	//fmt.Println("storageRevokeLease: before changing flag to true")
	ss.flagMux.Lock()           // lock 2
	ss.leaseFlagMap[key] = true // **************
	ss.flagMux.Unlock()         // unlock 2
	//fmt.Println("storageRevokeLease: before sending revokLeases")
	ss.cacheMux.Lock()         // lock 3
	libs, got := ss.cache[key] // *************
	//fmt.Println("check CacheLease, Key:  ", key, " Value: ", libs, " if Lease: ", got)
	//ss.cacheMux.Unlock()

	if got {
		limit := float64(storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds)
		timeCount := float64(0)
		revokeCount := 0
		// send revoke go routines
		localChan := make(chan int)
		//ss.cacheMux.Lock()
		for _, item := range libs { // *************
			t := time.Since(item.start).Seconds()
			// fmt.Printf("Storage: %v \n", t)
			if t < limit && !item.revoked {
				// only calls when the lease has not expired

				timeCount = max(limit-t, timeCount)
				//fmt.Println("Time is", t, " still have: ", limit - t, "now timeCount: ", timeCount)
				go ss.sendRevokeLease(key, item, localChan)
				revokeCount += 1
			}
		}
		ss.cacheMux.Unlock()

		//totalCount := revokeCount
		// now count the number of replies, until receive all replies or all expired
		if revokeCount != 0 {
			timer := time.NewTimer((time.Duration(limit)) * time.Second) // %%%%%%%%%%%%% slows the system!
			//fmt.Println("storageRevokeLease: finish revokLeases, timer: ",  time.Duration(timeCount) +1)
			ifBreak := false
			for !ifBreak {
				//fmt.Println("enter loop")
				select {
				case <-timer.C:
					//fmt.Println("break timer")
					ifBreak = true
				case <-localChan:
					revokeCount -= 1
					if revokeCount == 0 {
						ifBreak = true
					}
				}
			}
		}

		//fmt.Println("Finished revokingLeases")
		ss.cacheMux.Lock()
	}
	ss.cacheMux.Unlock() // unlock 2
	//fmt.Println("storageRevokeLease: before changing flag to false")
	ss.flagMux.Lock()
	ss.leaseFlagMap[key] = false // ************
	ss.flagMux.Unlock()
}

func (ss *storageServer) Put(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {

	key := args.Key
	// need to check if routed to the right server
	//fmt.Println("Put: ", key, " Value: ", args.Value)
	hashVal := hash(key)
	id := ss.findNextHighest(hashVal)
	if !valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageRevokeLease(key)
	//fmt.Println("Before Putting Key:", key, " Value: ", args.Value)
	ss.dataMux.Lock()
	ss.postMap[key] = args.Value // ************
	ss.dataMux.Unlock()
	//fmt.Println("Done Putting: ", key, " Value: ",args.Value)
	reply.Status = storagerpc.OK
	return nil
}

func (ss *storageServer) AppendToList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	value := args.Value
	// need to check if routed to the right server
	hashVal := hash(key)
	id := ss.findNextHighest(hashVal)
	if !valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
		reply.Status = storagerpc.WrongServer
		return nil
	}

	ss.storageRevokeLease(key)

	ss.dataMux.Lock()
	l, ok := ss.listMap[key] // ************
	if ok {
		exists := false
		for _, val := range l {
			if val == value {
				exists = true
				break
			}
		}
		if !exists {
			reply.Status = storagerpc.OK
			ss.listMap[key] = append(l, value)
		} else {
			reply.Status = storagerpc.ItemExists
		}

	} else {
		// create a new list of the key
		ss.listMap[key] = []string{value}
		reply.Status = storagerpc.OK
	}
	ss.dataMux.Unlock()
	return nil
}

func (ss *storageServer) RemoveFromList(args *storagerpc.PutArgs, reply *storagerpc.PutReply) error {
	key := args.Key
	value := args.Value

	ss.storageRevokeLease(key)

	ss.dataMux.Lock()
	l, ok := ss.listMap[key]
	var err error
	if ok {
		exists := false
		index := -1
		for i, val := range l {
			if val == value {
				index = i
				exists = true
				break
			}
		}
		if exists {
			// create a new list
			new_list := append(l[:index], l[index+1:]...)
			ss.listMap[key] = new_list
			reply.Status = storagerpc.OK

		} else {
			reply.Status = storagerpc.ItemNotFound
		}
		err = nil
	} else {
		// need to decide between wrongserver and keynotfound
		hashVal := hash(key)
		id := ss.findNextHighest(hashVal)
		if valueInArray(id, ss.selfIDs) { // right server, but still does not find the key
			reply.Status = storagerpc.KeyNotFound
		} else {
			reply.Status = storagerpc.WrongServer
		}
		//err = errors.New("keyNotFound")
		err = nil
	}
	ss.dataMux.Unlock()
	return err
}

// func (ss *storageServer) storageRevokeLease(key string) {
//     l, ok := ss.cache[key]
//     if ok {
//         limit := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
//         for _, item := range l {
//             t := time.Since(item.start).Seconds()
//             // fmt.Printf("Storage: %v \n", t)
//             if t < float64(limit) && !item.revoked{
//                 // only calls when the lease has not expired
//                 args := &storagerpc.RevokeLeaseArgs{
//                     Key: key,
//                 }
//                 reply := &storagerpc.RevokeLeaseReply{}
//                 item.cli.Call("LeaseCallbacks.RevokeLease", args, reply)
//                 item.revoked = true
//             }
//         }
//     }
// }

// func (ss *storageServer) cleanCache() {
//     for {
//         ss.mux.Lock()
//         if ss.leaseFlagMap{ // cannot clean while lease is being revoked
//             limit := storagerpc.LeaseSeconds + storagerpc.LeaseGuardSeconds
//             for key, l := range ss.cache {
//                 newL := []*cacheItem{}
//                 for _, item := range l {
//                     start := item.start
//                     if time.Since(start).Seconds() < float64(limit) && !item.revoked {
//                         newL = append(newL, item)
//                     }
//                 }
//                 ss.cache[key] = newL
//             }
//             // to conserve space, delete the key from the cache
//             for key, l := range ss.cache {
//                 if len(l) == 0 {
//                     delete(ss.cache, key)
//                 }
//             }
//         }
//         ss.mux.Unlock()
//         time.Sleep(200 * time.Millisecond)
//     }
// }
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//                              Custom Functions
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

func max(x, y float64) float64 {
	if x < y {
		return y
	}
	return x
}
func (ss *storageServer) insertCache(key string, item *cacheItem) {
	ss.cacheMux.Lock()
	leaseList, ok := ss.cache[key] // ************
	// fmt.Println("Storageserver: OK here")
	if ok { // there is an existing libstore array that requested for the lease
		ss.cache[key] = append(leaseList, item)
	} else { // this is the first time all libstores has ever requested a lease
		ss.cache[key] = []*cacheItem{item}
	}
	ss.cacheMux.Unlock()
}

func (ss *storageServer) checkTCP(hostPort string) *rpc.Client {
	//check if tcp conenction setup already
	ss.dataMux.Lock()
	cli, gotClient := ss.libCliMap[hostPort] // ***************
	ss.dataMux.Unlock()
	if !gotClient {
		//fmt.Println("checkTCP: ", hostPort)
		temp, err_dial := rpc.DialHTTP("tcp", hostPort)
		if err_dial != nil {
			return nil // cannot dial libstore address
		}
		cli = temp
		ss.dataMux.Lock()
		ss.libCliMap[hostPort] = cli // ***************
		ss.dataMux.Unlock()
		return cli
	}
	return cli

}
func (ss *storageServer) keyBeingChanged(key string) bool {
	//fmt.Println("In keyBeingChanged for key: ", key)
	ss.flagMux.Lock()
	ifChanging, ok := ss.leaseFlagMap[key]
	//fmt.Println("In keyBeingChanged: got key: ", key, " flag: ", ifChanging, " ok: ", ok)
	ss.flagMux.Unlock()
	if ok {
		return ifChanging
	}
	return false //not being changed

}

// see if two list of ids are the same
func compareVirtualIDs(id1 []uint32, id2 []uint32) bool {
	if len(id1) != len(id2) {
		return false
	}
	for i := 0; i < len(id1); i++ {
		if id1[i] != id2[i] {
			return false
		}
	}
	return true
}

// returns the value at the next highest position of the sorted array
// assume there are no duplicates in the array
func (ss *storageServer) findNextHighest(target uint32) uint32 {
	var lo int
	var hi int

	nums := ss.ring
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

// returns whether target value is in the array
func valueInArray(target uint32, nums []uint32) bool {
	found := false
	for _, num := range nums {
		if num == target {
			found = true
			break
		}
	}
	return found
}

// hash hashes a string key and returns a 32-bit integer. This function
// is provided here so that all implementations use the same hashing mechanism
// (both the Libstore and StorageServer should use this function to hash keys).
func hash(key string) uint32 {
	prefix := strings.Split(key, ":")[0]
	hasher := fnv.New32()
	hasher.Write([]byte(prefix))
	return hasher.Sum32()
}
