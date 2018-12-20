package tribserver

import (
	//"errors"

	"encoding/json"
	"github.com/cmu440/tribbler/libstore"
	"github.com/cmu440/tribbler/rpc/tribrpc"
	"github.com/cmu440/tribbler/util"
	"net"
	"net/http"
	"net/rpc"
	"sort"
	"strconv"
	"strings"
	"time"
	//"fmt"
)

type tribServer struct {
	// TODO: implement this!
	lib        libstore.Libstore
	masterPort string // ???????
	listener   net.Listener
	timeStart  time.Time
}

// NewTribServer creates, starts and returns a new TribServer. masterServerHostPort
// is the master storage server's host:port and port is this port number on which
// the TribServer should listen. A non-nil error should be returned if the TribServer
// could not be started.
//
// For hints on how to properly setup RPC, see the rpc/tribrpc package.
func NewTribServer(masterServerHostPort, myHostPort string) (TribServer, error) {
	s := new(tribServer)
	ls, err := libstore.NewLibstore(masterServerHostPort, myHostPort, libstore.Normal)
	// if err != nil {
	// 	return nil, errors.New("Unsuccessful Libstore")
	// }
	s.lib = ls
	listener, err := net.Listen("tcp", myHostPort)
	s.listener = listener
	s.timeStart = time.Now()
	if err != nil {
		return nil, err
	}
	err = rpc.RegisterName("TribServer", tribrpc.Wrap(s))
	if err != nil {
		return nil, err
	}
	rpc.HandleHTTP()
	go http.Serve(listener, nil)
	//go s.mainRoutine()
	return s, nil
}

func (ts *tribServer) CreateUser(args *tribrpc.CreateUserArgs, reply *tribrpc.CreateUserReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) { //not created before
		lib.Put(userId, "exists")
		//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>NEED TO CHANGE ?
		//this assumes libstore.GetList and storage server implementation would create new empty list and return
		// if the key isn't found in storage server
		//<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
		//lib.GetList(util.FormatSubListKey(userId)) //this would supposedly initialize an empty list for the userId:subList
		//lig.GetList(util.FormatTribListKey(userId))
		reply.Status = tribrpc.OK
		return nil
	}

	reply.Status = tribrpc.Exists
	return nil
}

func (ts *tribServer) AddSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	lib := ts.lib
	userId := args.UserID
	targetId := args.TargetUserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	if !ts.checkUserExists(targetId) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	err := lib.AppendToList(util.FormatSubListKey(userId), targetId)
	if err != nil {
		reply.Status = tribrpc.Exists
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) RemoveSubscription(args *tribrpc.SubscriptionArgs, reply *tribrpc.SubscriptionReply) error {
	lib := ts.lib
	userId := args.UserID
	targetId := args.TargetUserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}

	if !ts.checkUserExists(targetId) {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	//if target user valid, but user not subscribed to him anymore, what status to return?
	err := lib.RemoveFromList(util.FormatSubListKey(userId), targetId)
	if err != nil {
		reply.Status = tribrpc.NoSuchTargetUser
		return nil
	}
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetFriends(args *tribrpc.GetFriendsArgs, reply *tribrpc.GetFriendsReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	var friends []string
	subs, err := lib.GetList(util.FormatSubListKey(userId))
	if err != nil {
		reply.Status = tribrpc.OK
		reply.UserIDs = friends
		return nil
	}
	for _, sub := range subs {
		subs_of_sub, err := lib.GetList(util.FormatSubListKey(sub))
		if err != nil { //really shouldn't happen since we check both id when add subscription, and no user ids gets deleted
			reply.Status = tribrpc.NoSuchTargetUser
			return nil
		}
		if checkInList(subs_of_sub, userId) {
			friends = append(friends, sub)
		}
	}
	reply.UserIDs = friends
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) PostTribble(args *tribrpc.PostTribbleArgs, reply *tribrpc.PostTribbleReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	timeNow := time.Now()
	timeNano := timeNow.UnixNano() //
	postKey := util.FormatPostKey(userId, timeNano)
	for ts.checkPostExists(postKey) == true { //key not unique
		postKey = util.FormatPostKey(userId, timeNano)
	}

	var trib tribrpc.Tribble
	trib.UserID = userId
	trib.Posted = timeNow
	trib.Contents = args.Contents

	post, err := marshal(&trib)
	if err != nil {
		panic("marshal wrong")
	}

	//put postKey first
	err = lib.Put(postKey, string(post))
	if err == nil {
		err = lib.AppendToList(util.FormatTribListKey(userId), postKey)
		if err == nil {
			reply.PostKey = postKey
			reply.Status = tribrpc.OK
		} else {
			lib.Delete(postKey)
			reply.Status = tribrpc.NoSuchUser
		}

	} else {
		reply.Status = tribrpc.NoSuchPost
	}

	return nil
}

func (ts *tribServer) DeleteTribble(args *tribrpc.DeleteTribbleArgs, reply *tribrpc.DeleteTribbleReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	err := lib.RemoveFromList(util.FormatTribListKey(userId), args.PostKey)
	if err != nil {
		reply.Status = tribrpc.NoSuchPost
		return nil
	}
	lib.Delete(args.PostKey)
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribbles(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//get posts
	tribbles := make([]tribrpc.Tribble, 0)
	posts, err := lib.GetList(util.FormatTribListKey(userId))
	if err != nil {
		reply.Status = tribrpc.OK
		reply.Tribbles = tribbles
	}
	//sort posts by time
	//sort.Slice(posts, func(i,j int) bool {return getTimeNano(posts[i]) > getTimeNano(posts[j])})
	posts = cleanDuplicates(posts)

	//add content one by one
	for _, postKey := range posts {
		marshalled, err := lib.Get(postKey)
		if err == nil {
			var tribble tribrpc.Tribble
			unmarshal([]byte(marshalled), &tribble)
			// for debug below
			// timeFromTribble := tribble.Posted.UnixNano()
			// temp := strings.Split(postKey, ":")[1]
			// timeFromKey, _:= strconv.ParseInt(strings.Split(temp, "_")[1], 16, 64)
			// if (timeFromTribble != timeFromKey){ // something wrong with the tribble
			// 	fmt.Println("##################################")
			// 	fmt.Printf("key, tribble does not agree: %v, %v \n", timeFromKey, timeFromTribble)
			// 	panic("key, tribble does not agree")
			// }
			// for debug above

			if !inTribbles(tribbles, tribble) {

				tribbles = append(tribbles, tribble)
				// if len(tribbles) == 100 {
				// 	break
				// }
			}

		}
	}
	sort.Slice(tribbles, func(i, j int) bool { return tribbles[i].Posted.After(tribbles[j].Posted) })
	length := 0
	if 100 < len(tribbles) {
		length = 100
	} else {
		length = len(tribbles)
	}
	tribbles = tribbles[:length]
	//isSorted := isSortedByTime(tribbles)
	// if !isSorted {
	// 	panic("Tribbles not sorted!!!!!")
	// }
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

func (ts *tribServer) GetTribblesBySubscription(args *tribrpc.GetTribblesArgs, reply *tribrpc.GetTribblesReply) error {
	lib := ts.lib
	userId := args.UserID
	if !ts.checkUserExists(userId) {
		reply.Status = tribrpc.NoSuchUser
		return nil
	}
	//get all users who userID subscribes to
	subs, _ := lib.GetList(util.FormatSubListKey(userId))
	var allPostKeys []string
	//get all the post from all of them
	for _, subID := range subs {
		postKeys, err := lib.GetList(util.FormatTribListKey(subID))
		if err == nil {
			allPostKeys = append(allPostKeys, postKeys...)
		}
		//ignore if err is not nil
	}
	//sort everything
	sort.Slice(allPostKeys, func(i, j int) bool { return getTimeNano(allPostKeys[i]) > getTimeNano(allPostKeys[j]) })

	var tribbles []tribrpc.Tribble
	for _, postKey := range allPostKeys {
		marshalled, err := lib.Get(postKey)
		if err == nil {
			var tribble tribrpc.Tribble
			unmarshal([]byte(marshalled), &tribble)
			//tribble := ts.generateTribble(getUserID(postKey),contents)
			tribbles = append(tribbles, tribble)
			if len(tribbles) == 100 {
				break
			}
		}
	}
	reply.Tribbles = tribbles
	reply.Status = tribrpc.OK
	return nil
}

//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
//								Custom Functions
//>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>><<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<

func marshal(msg *tribrpc.Tribble) ([]byte, error) {
	res, err := json.Marshal(msg)
	return res, err
}

func unmarshal(data []byte, v *tribrpc.Tribble) error {
	err := json.Unmarshal(data, v)
	return err
}

// func (ts *tribServer) generateTribble(postKey,contents string) tribrpc.Tribble {
// 	tribble := Tribble{}
// 	userId := getUserID(postKey)
// 	tribble.UserID = userId
// 	tribble.Contents = contents
// 	timeNano := getTimeNano(postKey)
// 	postTime := ts.timeStart.Add(time.Duration(timeNano) * time.Nanosecond)
// 	tribble.Posted = postTime
// 	return tribble
// }

func getUserID(postKey string) string {
	return strings.Split(postKey, ":")[0]
}

func getTimeNano(postKey string) int64 {
	post_time_rand := strings.Split(postKey, ":")[1] //should always be length 2
	time, err := strconv.ParseInt(strings.Split(post_time_rand, "_")[1], 16, 64)
	if err != nil {
		//fmt.Println("PostKey: ", postKey, "parse1: ", post_time_rand, "parse2: ", strings.Split(post_time_rand, "_")[1])
		panic(err)
	}
	return time
}

func (ts *tribServer) checkPostExists(postKey string) bool {
	lib := ts.lib
	_, err := lib.Get(postKey)
	if err != nil {
		return false
	}
	return true
}

func (ts *tribServer) checkUserExists(ID string) bool {
	lib := ts.lib
	reply, err := lib.Get(ID)
	if err != nil {
		return false
	}
	//if exists
	if strings.Compare(reply, "exists") != 0 { //value should be "exists" to show it was created and not changed
		//fmt.Println("UserID: ", ID, "Reply:" ,reply, "err: ", err)
		panic("Database corrupted")
	}
	return true
}
func inTribbles(listTribbles []tribrpc.Tribble, item tribrpc.Tribble) bool {
	for _, tribble := range listTribbles {
		if tribble.Contents == item.Contents {
			if tribble.Posted == item.Posted && tribble.UserID == item.UserID {
				return true
			}

		}
	}
	return false
}

func isSortedByTime(l []tribrpc.Tribble) bool {
	if len(l) < 2 {
		return true
	}
	for i := 0; i < len(l)-1; i++ {
		t1 := l[i].Posted
		t2 := l[i+1].Posted
		if !t1.After(t2) {
			return false
		}
	}
	return true
}
func checkInList(listStrings []string, item1 string) bool {
	for _, item2 := range listStrings {
		if strings.Compare(item1, item2) == 0 {
			return true
		}
	}
	return false
}
func cleanDuplicates(listStrings []string) []string {
	//n := len(listStrings)
	for i, item := range listStrings {
		ifDup := duplicateIndexInList(listStrings, item)
		if ifDup {
			listStrings = append(listStrings[:i], listStrings[i+1:]...)
		}
	}
	return listStrings
}
func duplicateIndexInList(listStrings []string, item1 string) bool {
	count := 0
	//index := -1
	for _, item2 := range listStrings {
		if item1 == item2 {
			count = count + 1
			//index = i
		}
	}
	return count >= 2 //, index
}
