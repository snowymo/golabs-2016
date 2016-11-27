package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import (
	"sort"
)

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type rpcFunc func(Op, int, *GetReply)

type Op struct {
	// Your definitions here.
	Oper  string
	Key   string
	Value string
	Uid   int64
}

type KeyOpPair struct {
	key string
	op  string
}

type GGPair struct {
	bf int64
	af int64
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64 // my replica group ID

	// Your definitions here.
	config    shardmaster.Config
	lastLogId int
	uidmap    map[int64]int //uidmap = make(map[int64]bool)
	logCache  map[int]Op    // logCache = make(map[int]Op)
	validList map[int]bool  // validList = make(map[int]bool, 0)
	minDone   int
}

func (kv *ShardKV) rpcRoutine(curProposal Op, insid int, reply *GetReply, afterProp rpcFunc) {
	DPrintf("%v RPC me:%d-%d id-%d\tkey-%v\tvalue-%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal.Key, curProposal.Value)

	kv.px.Start(insid, curProposal)

	to := 10 * time.Millisecond
	for {
		status, proposal := kv.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					// kv.CheckMinDone(insid, curProposal)
					// kv.freeMem()
					afterProp(curProposal, insid, reply)
					return
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("%v RPC me:%d-%d id-%d\tkey-%v\tvalue-%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal.Key, curProposal.Value)
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("%v RPC me:%d-%d id-%d\tWrong op type\n", curProposal.Oper, kv.gid, kv.me, insid)
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *ShardKV) rmDuplicate(insid int, curLog Op) {
	// step 1: remove seq with duplicate ids
	to := 10 * time.Millisecond
	for pre_insid := kv.lastLogId + 1; pre_insid <= insid; pre_insid++ {
		//DPrintf("interpretLog %d: call Status\t%d\n", kv.me, pre_insid)
		err, pre_v := kv.px.Status(pre_insid)
		// if it is old seq then we ask it to propose
		if err != 1 {
			kv.px.Start(pre_insid, pre_v)
		}
		for err != 1 {
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
			err, pre_v = kv.px.Status(pre_insid)
		}

		if _, isop := pre_v.(Op); isop {
			//DPrintf("interpretLog: \t%d op-%v\n", pre_insid, kv.printop(pre_v.(Op)))
			if _, uidok := kv.uidmap[pre_v.(Op).Uid]; uidok {
				// already true, discard this entry
				DPrintf("interpretLog: \t%d duplicate:%d\n", pre_insid, pre_v.(Op).Uid)
			} else {
				DPrintf("interpretLog: \t%d notdup:%d\n", pre_insid, pre_v.(Op).Uid)
				kv.logCache[pre_insid] = pre_v.(Op)
				kv.lastLogId = pre_insid
				DPrintf("cur shardkv:%d %v\n", kv.me, kv.logCache)
				kv.uidmap[pre_v.(Op).Uid] = pre_insid
				kv.validList[pre_insid] = true
			}
		} else {
			DPrintf("interpretLog: \t%d not GET/PUT/APP op-%v\n", pre_insid, pre_v)
			kv.validList[pre_insid] = true
		}
	}
}

func (kv *ShardKV) interpretLog(insid int, curLog Op) string {
	// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
	endIdx := insid
	if dupSeqid := kv.uidmap[curLog.Uid]; dupSeqid != insid {
		if kv.logCache[dupSeqid].Value == "" {
			endIdx = dupSeqid
		} else {
			// if it is duplicate, then get the value directly
			return kv.logCache[dupSeqid].Value
		}
	}
	// if it is not duplicate, or did not get older Get before
	// TODO: how to deal with reconfiguration
	startIdx := 0
	for logidx := endIdx; logidx >= 0; logidx-- {
		// if find a log is put then break
		if kv.logCache[logidx].Oper == "Put" && kv.logCache[logidx].Key == curLog.Key {
			startIdx = logidx
			break
		}
	}
	// step 3: figure out all the put/append from start to last same key put
	DPrintf("before access kv.logcache[%d].Value\n", insid)
	// kv.printdb()
	updatedOp := kv.logCache[insid]
	updatedOp.Value = ""
	for logidx := startIdx; logidx < endIdx; logidx++ {
		logentry, logok := kv.logCache[logidx]
		//DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
		if logok && logentry.Key == curLog.Key {
			if logentry.Oper == "Put" {
				updatedOp.Value = logentry.Value
				//DPrintf("interpretLog: put k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			} else if logentry.Oper == "Append" {
				updatedOp.Value += logentry.Value
				//DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			}
		}

	}
	kv.logCache[insid] = updatedOp
	DPrintf("cur shardkv:%d %v\n", kv.me, kv.logCache)
	return updatedOp.Value
}

func (kv *ShardKV) updateMinDone() {
	// Find the smallest true
	myMinValid := len(kv.validList)
	for k, v := range kv.validList {
		if v {
			if myMinValid > k {
				myMinValid = k
			}
		}
	}
	// Find the biggest false
	myMaxUValid := -1
	for k, v := range kv.validList {
		if !v {
			if k > myMaxUValid && k < myMinValid {
				myMaxUValid = k
			}
		}
	}
	if kv.minDone < myMaxUValid {
		kv.minDone = myMaxUValid
		//DPrintf("update minDone:%d\n", kv.minDone)
	}
	//DPrintf("me: %d\tCheckMinDone before call Done:%d\n", kv.me, kv.minDone)
	kv.px.Done(kv.minDone)
}

func (kv *ShardKV) turnAppendtoPut(op string) string {
	if op == "Append" {
		return "Put"
	}
	return op
}

func (kv *ShardKV) CheckMinDone(insid int, curProposal Op) {
	//DPrintf("CheckMinDone seq:%d\t%v\n", insid, kv.printop(curProposal))
	if curProposal.Oper == "Append" {
		return
	}

	keyop := make(map[KeyOpPair]bool)
	curPair := KeyOpPair{curProposal.Key, curProposal.Oper}
	keyop[curPair] = true

	// set all previous op with same key and op to false
	var keyInsids []int
	for keyInsid, _ := range kv.logCache {
		keyInsids = append(keyInsids, keyInsid)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keyInsids)))
	for _, keyInsid := range keyInsids {
		if keyInsid > kv.minDone && keyInsid < insid {
			//DPrintf("CheckMinDone check key:%d\n", keyInsid)
			curPair = KeyOpPair{kv.logCache[keyInsid].Key, kv.turnAppendtoPut(kv.logCache[keyInsid].Oper)}
			curPairOri := KeyOpPair{kv.logCache[keyInsid].Key, kv.logCache[keyInsid].Oper}
			//DPrintf("CheckMinDone curpair:%v\n", curPair)
			if b, ok2 := keyop[curPair]; ok2 && b {
				kv.validList[keyInsid] = false
				//DPrintf("turn to false: seq-%d %t\n", keyInsid, kv.validList[keyInsid])
			} else if b, ok2 = keyop[curPairOri]; !ok2 {
				keyop[curPairOri] = true
			}
		}
	}
	kv.updateMinDone()
}

func (kv *ShardKV) freeMem() {
	kv.px.Min()
	// free own attribute about statusMap
	for i, _ := range kv.logCache {
		if i <= kv.minDone {
			if kv.logCache[i].Oper != "Get" {
				curOp := kv.logCache[i]
				curOp.Value = ""
				kv.logCache[i] = curOp
			}
		}
	}
	//kv.printdb()
}

func (kv *ShardKV) emptyRpc(curProposal Op, insid int, reply *GetReply) {
	// step 1: remove seq with duplicate ids
	kv.rmDuplicate(insid, curProposal)
	if curProposal.Oper == "Get" {
		reply.Value = kv.interpretLog(insid, curProposal)
	}
	kv.CheckMinDone(insid, curProposal)
	kv.freeMem()
	reply.Err = OK
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		return nil
	}

	curProposal := Op{"Get", args.Key, "", args.Id}
	insid := kv.px.Max() + 1

	kv.rpcRoutine(curProposal, insid, reply, kv.emptyRpc)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		return nil
	}

	curProposal := Op{args.Op, args.Key, args.Value, args.Id}
	insid := kv.px.Max() + 1

	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.emptyRpc)
	reply.Err = tmpReply.Err

	return nil
}

func CopyMapII(dstMap map[int64]int, srcMap map[int64]int) {
	for k, v := range srcMap {
		dstMap[k] = v
	}
	DPrintf("after cpy %v\n", dstMap)
}

func CopyMapIO(dstMap map[int]Op, srcMap map[int]Op) {
	for k, v := range srcMap {
		dstMap[k] = v
	}
	DPrintf("after cpy %v\n", dstMap)
}

func CopyMapIB(dstMap map[int]bool, srcMap map[int]bool) {
	for k, v := range srcMap {
		dstMap[k] = v
	}
	DPrintf("after cpy %v\n", dstMap)
}

// func CopyMap(dstMap interface{}, srcMap interface{}) {
// 	DPrintf("before cpy %v\n", dstMap)
// 	s, ok := srcMap.(map[int64]int)
// 	if ok {
// 		for k, v := range s {
// 			dstMap.(map[int64]int)[k] = v
// 		}
// 	}
// 	s2, ok2 := srcMap.(map[int]Op)
// 	if ok2 {
// 		for k, v := range s2 {
// 			dstMap.(map[int]Op)[k] = v
// 		}
// 	}
// 	s3, ok3 := srcMap.(map[int]bool)
// 	if ok3 {
// 		for k, v := range s3 {
// 			dstMap.(map[int]bool)[k] = v
// 		}
// 	}
// 	DPrintf("after cpy %v\n", dstMap)
// }

// RPC handler for client Put and Append requests
func (kv *ShardKV) BK(args *BKArgs, reply *BKReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// do the copy
	DPrintf("BK RPC me:%d-%d\n", kv.gid, kv.me)
	kv.config = args.Config
	kv.lastLogId = args.LastLogId
	CopyMapII(kv.uidmap, args.Uidmap)
	CopyMapIO(kv.logCache, args.LogCache)
	CopyMapIB(kv.validList, args.ValidList)
	kv.minDone = args.MinDone
	//kv.px = *args.PX

	reply.Err = OK

	return nil
}

func (kv *ShardKV) moveState(curCfg shardmaster.Config) {
	if len(kv.config.Groups) > 0 {
		backupGrp := map[GGPair]bool{}
		//DPrintf("me:%d-%d\tchange config from %v to %v\n", kv.gid, kv.me, kv.config, curCfg)
		// check if there is new group take over current group
		//DPrintf("inside our group:%v\ncur group:%v\n", kv.config.Groups, curCfg.Groups)
		for i, gid := range kv.config.Shards {
			curGid := curCfg.Shards[i]
			DPrintf("me:%d-%d shard:%d gid:%d curgid:%d\n", kv.gid, kv.me, i, gid, curGid)
			if b, bok := backupGrp[GGPair{gid, curGid}]; kv.gid == gid && (!bok || (bok && !b)) && gid != curGid {
				// group changed, send all my info to all servers in that group
				backupGrp[GGPair{gid, curGid}] = true
				for _, group := range curCfg.Groups[curGid] { //Groups map[int64][]string // gid -> servers[]
					ok := false
					for !ok {
						// call backup RPC to all
						//						args := &BKArgs{kv.config, kv.lastLogId, kv.uidmap, kv.logCache, kv.validList, kv.minDone, &kv.px}
						args := &BKArgs{kv.config, kv.lastLogId, kv.uidmap, kv.logCache, kv.validList, kv.minDone}
						var reply BKReply
						DPrintf("\nbefore me:%d-%d BK call %v\n", kv.gid, kv.me, group)
						ok = call(group, "ShardKV.BK", args, &reply)
						if ok {
							break
						}
						time.Sleep(100 * time.Millisecond)
					}
				}
			}
		}
	}
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	curCfg := kv.sm.Query(-1)
	if kv.config.Num != curCfg.Num {
		kv.moveState(curCfg)

		// update configuration
		kv.config.Num = curCfg.Num
		kv.config.Shards = curCfg.Shards
		shardmaster.CopyMap(kv.config.Groups, curCfg.Groups)
		DPrintf("me:%d-%d\tafter change config to %v\n", kv.gid, kv.me, kv.config)
	}
}

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	// Your initialization code here.
	kv.config = shardmaster.Config{-1, [shardmaster.NShards]int64{}, map[int64][]string{}}
	kv.lastLogId = -1
	kv.uidmap = make(map[int64]int)
	kv.logCache = make(map[int]Op, 0)
	kv.validList = make(map[int]bool, 0)
	kv.minDone = -1
	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
