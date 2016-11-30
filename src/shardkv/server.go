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

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		fmt.Printf(format, a...)
	}
	return
}

type rpcFunc func(Op, int, *GetReply)

type Op struct {
	// Your definitions here.
	Oper     string
	Key      string
	Value    string
	ConfigNo int // old config no
	//Gid      int64 // for reconfig, to inquire replica from which group
	//Shards   int   // for reconfig, to inquire replica for which shards
	//GSmap map[int]int64 // for reconfig, to inquire replica  [shards]group id
	GSmap map[int64][]int
	Uid   int64
}

type SnapShot struct {
	Uidmap map[int64]int //uidmap = make(map[int64]bool)
	DB     map[string]string
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
	db        map[string]string
	validList map[int]bool // validList = make(map[int]bool, 0)
	minDone   int

	snapshots map[int]SnapShot
	reconfigs map[int]SnapShot
	bReCfg    map[int]bool

	updateMu sync.Mutex
}

func (kv *ShardKV) sameMap(map1 map[int]int64, map2 map[int]int64) bool {
	for k, v := range map1 {
		if v != map2[k] {
			return false
		}
	}
	for k, v := range map2 {
		if v != map2[k] {
			return false
		}
	}
	return true
}

func (kv *ShardKV) sameMap2(map1 map[int64][]int, map2 map[int64][]int) bool {
	for k, v := range map1 {
		for i, _ := range v {
			if v[i] != map2[k][i] {
				return false
			}
		}
	}
	for k, v := range map2 {
		for i, _ := range v {
			if v[i] != map1[k][i] {
				return false
			}
		}
	}
	return true
}

func (kv *ShardKV) sameOp(op1 Op, op2 Op) bool {
	if (op1.ConfigNo == op2.ConfigNo) && (op1.Key == op2.Key) && (op1.Oper == op2.Oper) && (op1.Uid == op2.Uid) && (op1.Value == op2.Value) && kv.sameMap2(op1.GSmap, op2.GSmap) {
		return true
	} else {
		return false
	}
}

func (kv *ShardKV) printLog() {
	// if Debug == 1 {
	// 	kv.mu.Lock()
	// 	defer kv.mu.Unlock()
	DPrintf("log size:%d\n", len(kv.logCache))
	var keyInsids []int
	for keyInsid, _ := range kv.logCache {
		keyInsids = append(keyInsids, keyInsid)
	}
	sort.Sort(sort.IntSlice(keyInsids))
	for _, keyInsid := range keyInsids {
		DPrintf("\tsortlog:%d\t%v\n", keyInsid, kv.logCache[keyInsid])
	}
	//}
}

func (kv *ShardKV) rpcRoutine(curProposal Op, insid int, reply *GetReply, afterProp rpcFunc) {
	DPrintf("%v RPC me:%d-%d id-%d\t%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal)

	kv.px.Start(insid, curProposal)

	to := 10 * time.Millisecond
	for {
		status, proposal := kv.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				// check if it is exact the same propose
				if kv.sameOp(proposal.(Op), curProposal) {
					// kv.CheckMinDone(insid, curProposal)
					// kv.freeMem()
					afterProp(curProposal, insid, reply)
					return
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("%v RPC me:%d-%d wrong prop id-%d\t%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal)
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
			if u, uidok := kv.uidmap[pre_v.(Op).Uid]; uidok && (u != 0) {
				// already true, discard this entry
				DPrintf("rmDuplicate: \t%d duplicate:%d-%d\n", pre_insid, pre_v.(Op).Uid, u)
			} else {
				//DPrintf("rmDuplicate: \t%d notdup:%v\n", pre_insid, pre_v.(Op))
				kv.logCache[pre_insid] = pre_v.(Op)
				//DPrintf("\ncur shardkv:%d-%d %v\n", kv.gid, kv.me, kv.logCache)
				// if pre_v.(Op).Uid != -1 {
				// 	kv.uidmap[pre_v.(Op).Uid] = pre_insid
				// }
				kv.validList[pre_insid] = true
			}
		} else {
			DPrintf("rmDuplicate: \t%d not GET/PUT/APP op-%v\n", pre_insid, pre_v)
			kv.validList[pre_insid] = true
		}
	}
}

func (kv *ShardKV) Update2(args *UpdateArgs, reply *UpdateReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, uidok := kv.uidmap[args.Id]; uidok {
		// already true, discard this entry
		DPrintf("Update me:%d-%d duplicate:%v\n", kv.gid, kv.me, args)
	} else {
		DPrintf("Update me:%d-%d not dup:%v\n", kv.gid, kv.me, args)
		reply.DB = make(map[string]string)
		reply.Uidmap = make(map[int64]int)
		// it is possible that this server has not done snapshot
		if args.SnapNo >= kv.config.Num {
			DPrintf("Update me:%d-%d not snapshot yet and do tick:%v\n", kv.gid, kv.me, args)
			kv.Snapshot()
			kv.updateCfg(kv.sm.Query(-1))
		}
		CopyMapSS(reply.DB, kv.snapshots[args.SnapNo].DB)
		CopyMapII(reply.Uidmap, kv.snapshots[args.SnapNo].Uidmap)
		reply.Err = OK
		DPrintf("Update me:%d-%d\tdb-%v\nlogs:", kv.gid, kv.me, reply.DB)
		kv.printLog()
		DPrintf("\n")
	}
	return nil
}

func (kv *ShardKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	DPrintf("enter Update RPC %d-%d\n", kv.gid, kv.me)
	// if _, uidok := kv.uidmap[args.Id]; uidok {
	// 	// already true, discard this entry
	// 	DPrintf("Update me:%d-%d duplicate:%v\n", kv.gid, kv.me, args)
	// } else {
	//DPrintf("Update me:%d-%d not dup:%v\n", kv.gid, kv.me, args)
	reply.DB = make(map[string]string)
	reply.Uidmap = make(map[int64]int)
	// it is possible that this server has not done snapshot
	// according to atomic
	kv.updateMu.Lock()
	defer kv.updateMu.Unlock()
	if sn, ok := kv.snapshots[args.SnapNo]; ok {

		CopyMapSS(reply.DB, sn.DB)
		CopyMapII(reply.Uidmap, sn.Uidmap)

		reply.Err = OK
		DPrintf("Update me:%d-%d\tdb-%v\nlog", kv.gid, kv.me, reply.DB)
		// kv.printLog()
		// DPrintf("\n")
	} else {
		DPrintf("Update me:%d-%d not snapshot yet:%v\n", kv.gid, kv.me, args)
		reply.Err = ErrNoSnap
	}
	//}
	return nil
}

func contain(item int, arr []int) bool {
	for _, v := range arr {
		if v == item {
			return true
		}
	}
	return false
}

func CopyMapSSSh(dstMap map[string]string, srcMap map[string]string, shards []int) {
	//DPrintf("before cpy dst:%v\tsrc:%v\n", dstMap, srcMap)
	for k, v := range srcMap {
		if contain(key2shard(k), shards) {
			dstMap[k] = v
			DPrintf("assign key-%v to shard-%d with dst-%v\n", k, key2shard(k), dstMap[k])
		}
	}
	//DPrintf("after cpy %v\n", dstMap)
	DPrintf("\n")
}

func (kv *ShardKV) updateState(reply UpdateReply, shards []int) {
	CopyMapSSSh(kv.db, reply.DB, shards)
	CopyMapII(kv.uidmap, reply.Uidmap)
}

func (kv *ShardKV) interpretLog(insid int, curLog Op) string {
	// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
	//endIdx := insid
	if dupSeqid, dok := kv.uidmap[curLog.Uid]; dok && (curLog.Uid != -1) && ((dupSeqid != insid) || (dupSeqid == -1)) {
		DPrintf("interpretLog: duplicate-%d and skip %v v-%v uid-%v\n", dupSeqid, curLog, kv.db[curLog.Key], kv.uidmap)
		if kv.lastLogId > dupSeqid {
			return kv.db[curLog.Key]
		}
	}
	// if it is not duplicate, or did not get older Get before
	// startIdx := 0
	// for logidx := endIdx; logidx >= 0; logidx-- {
	// 	// if find a log is put then break
	// 	if kv.logCache[logidx].Oper == "Put" && kv.logCache[logidx].Key == curLog.Key {
	// 		startIdx = logidx
	// 		break
	// 	}
	// }
	DPrintf("me:%d-%d interpretLog: startIdx-%d insid-%d cur-%v\n", kv.gid, kv.me, kv.lastLogId, insid, curLog)
	DPrintf("whole log:\t")
	kv.printLog()
	// step 3: figure out all the put/append from start to last same key put
	// kv.printdb()
	for logidx := kv.lastLogId + 1; logidx <= insid; logidx++ {
		logentry, logok := kv.logCache[logidx]
		if logok {
			// check if duplicate
			if dupSeqid, dok := kv.uidmap[logentry.Uid]; dok && (dupSeqid != 0) && (logentry.Uid != -1) && ((dupSeqid != logidx) || (dupSeqid == -1)) {
				delete(kv.logCache, logidx)
				DPrintf("interpretLog: duplicate %d with %d and remove %v v-%v uid-%v\n", logidx, dupSeqid, kv.logCache, kv.db[curLog.Key], kv.uidmap)
				kv.printLog()
				continue
			}

			if logentry.Oper == "Put" {
				kv.db[logentry.Key] = logentry.Value
				DPrintf("interpretLog: put k-%v v-%v\n", logentry.Key, logentry.Value)
			} else if logentry.Oper == "Append" {
				kv.db[logentry.Key] += logentry.Value
				DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, logentry.Value, kv.db[logentry.Key])
			} else if logentry.Oper == "Reconfig" {
				DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
				//if v, vok := kv.validList[logidx]; !vok || (vok && v) {
				if _, rcok := kv.reconfigs[logentry.ConfigNo]; !rcok {
					DPrintf("do recfg me:%d-%d WRONG not yet recfg-%d db-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db)
					// stop here until snapshot is ready
					return NOK
				} else if _, brok := kv.bReCfg[logentry.ConfigNo]; brok {
					DPrintf("do recfg me:%d-%d dupReconfig recfg-%d db-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db)
				} else {
					//kv.updateMu.Lock()
					CopyMapSS(kv.db, kv.reconfigs[logentry.ConfigNo].DB)
					CopyMapII(kv.uidmap, kv.reconfigs[logentry.ConfigNo].Uidmap)
					//kv.updateMu.Unlock()

					DPrintf("do recfg me:%d-%d already recfg-%d db-%v uid-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidmap)
					kv.bReCfg[logentry.ConfigNo] = true
				}

				// if groupUp != len(logentry.GSmap) {
				// 	DPrintf("interLog me:%d-%d not update enough %d %d\n", kv.gid, kv.me, groupUp, len(logentry.GSmap))
				// 	return "NotUpdate"
				// }
				DPrintf("interpretLog me:%d-%d %v result-%v\n", kv.gid, kv.me, logentry, kv.db)
			} else if logentry.Oper == "SnapShot" {
				// apply current state to that snapshot
				DPrintf("do snapshot me:%d-%d %v\n", kv.gid, kv.me, logentry)
				if sn, snok := kv.snapshots[logentry.ConfigNo]; !snok {
					DPrintf("do snapshot me:%d-%d for cfg-%d db-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, sn)
					kv.updateMu.Lock()
					kv.snapshots[logentry.ConfigNo] = SnapShot{kv.uidmap, kv.db}
					kv.updateMu.Unlock()
					// update atomic
				} else {
					DPrintf("do snapshot me:%d-%d already cfg-%d db-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, sn)
				}
			}

			kv.lastLogId = logidx
			if logentry.Uid != -1 {
				DPrintf("bf uid assign [%d]%d\t", logentry.Uid, kv.uidmap[logentry.Uid])
				kv.uidmap[logentry.Uid] = logidx
				DPrintf("af uid assign [%d]%d\n", logentry.Uid, kv.uidmap[logentry.Uid])
			}
		}
	}

	//DPrintf("\ncur shardkv:%d-%d %v %v\n", kv.gid, kv.me, kv.logCache, kv.db)
	//DPrintf("\n")
	return kv.db[curLog.Key]
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

	kv.rmDuplicate(insid, curProposal)
	//	if curProposal.Oper == "Get" {
	reply.Value = kv.interpretLog(insid, curProposal)
	//	}

	kv.CheckMinDone(insid, curProposal)
	kv.freeMem()
	if reply.Value == NOK {
		reply.Err = NOK
	} else {
		reply.Err = OK
	}

}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	// check if current cfg is updated
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("enter Get %d-%d %v\n", kv.gid, kv.me, args)
	curCfg := kv.sm.Query(-1)
	if curCfg.Num != kv.config.Num {
		reply.Err = NOK
		DPrintf("in %v %d-%d config:%d not latest:%d\n", args, kv.gid, kv.me, kv.config.Num, curCfg.Num)
		return nil
	}
	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		DPrintf("in Get %d-%d %v\n", kv.gid, kv.me, reply.Err)
		return nil
	}
	curProposal := Op{"Get", args.Key, "", kv.config.Num, nil, args.Id}
	//kv.px.Lab4print()
	insid := kv.px.Max() + 1

	kv.rpcRoutine(curProposal, insid, reply, kv.emptyRpc)

	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	// check if current cfg is updated
	kv.mu.Lock()
	defer kv.mu.Unlock()
	DPrintf("enter PutAppend %d-%d %v\n", kv.gid, kv.me, args)
	curCfg := kv.sm.Query(-1)
	if curCfg.Num != kv.config.Num {
		reply.Err = NOK
		DPrintf("in %v %d-%d config:%d not latest:%d\n", args, kv.gid, kv.me, kv.config.Num, curCfg.Num)
		return nil
	}
	// check if the shard belongs to the group id contain current server
	if kv.gid != kv.config.Shards[args.Shard] {
		reply.Err = ErrWrongGroup
		DPrintf("in Get %d-%d %v\n", kv.gid, kv.me, reply.Err)
		return nil
	}
	curProposal := Op{args.Op, args.Key, args.Value, kv.config.Num, nil, args.Id}
	insid := kv.px.Max() + 1

	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.emptyRpc)
	reply.Err = tmpReply.Err

	return nil
}

func CopyMapII(dstMap map[int64]int, srcMap map[int64]int) {
	for k, _ := range srcMap {
		dstMap[k] = -1
	}
	//DPrintf("after cpy %v\n", dstMap)
}

func CopyMapSS(dstMap map[string]string, srcMap map[string]string) {
	//DPrintf("before cpy dst:%v\tsrc:%v\n", dstMap, srcMap)
	for k, v := range srcMap {
		dstMap[k] = v
	}
	//DPrintf("after cpy %v\n", dstMap)
}

// func CopyMapIB(dstMap map[int]bool, srcMap map[int]bool) {
// 	for k, v := range srcMap {
// 		dstMap[k] = v
// 	}
// 	DPrintf("after cpy %v\n", dstMap)
// }

// func (kv *ShardKV) Reconfig(gs map[int]int64) {
// 	// Your code here.
// 	curProposal := Op{"Reconfig", "", "", kv.config.Num, gs, -1}
// 	insid := kv.px.Max()
// 	if insid == -1 {
// 		insid = 0
// 	}

// 	tmpReply := &GetReply{}
// 	kv.rpcRoutine(curProposal, insid, tmpReply, kv.emptyRpc)
// }
func (kv *ShardKV) rfgProp(curProposal Op, insid int, reply *GetReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// step 1: remove seq with duplicate ids
	curRecfg := SnapShot{make(map[int64]int), make(map[string]string)}
	for groupid, shards := range curProposal.GSmap {
		servers, sok := kv.config.Groups[groupid]
		for sok {
			// try each server in that replication group.
			for _, srv := range servers {
				args := &UpdateArgs{curProposal.ConfigNo, nrand() % MAXUID}
				var reply UpdateReply
				DPrintf("rfgProp me:%d-%d before call %d\n\tresult-%v\n", kv.gid, kv.me, groupid, kv.db)
				ok := call(srv, "ShardKV.Update", args, &reply)
				if ok && (reply.Err == OK) {
					// update from the reply data
					CopyMapSSSh(curRecfg.DB, reply.DB, shards)
					CopyMapII(curRecfg.Uidmap, reply.Uidmap)
					DPrintf("interLog me:%d-%d after update %v\n\tresult-%v\n", kv.gid, kv.me, curProposal, curRecfg.DB)
					sok = false
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	kv.reconfigs[curProposal.ConfigNo] = curRecfg
	//if reply.Value == "NotUpdate" {
	//	reply.Err = NOK
	//} else {
	reply.Err = OK
	//}

}

func (kv *ShardKV) Reconfig2(gs map[int64][]int) {
	// Your code here.
	curProposal := Op{"Reconfig", "", "", kv.config.Num, gs, -1}
	insid := kv.px.Max()
	if insid == -1 {
		insid = 0
	}

	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.rfgProp)
}

func (kv *ShardKV) snapProp(curProposal Op, insid int, reply *GetReply) {
	// snapshot current state
	// deal with all previous logs
	// step 1: remove seq with duplicate ids
	kv.rmDuplicate(insid, curProposal)
	kv.interpretLog(insid, curProposal)
	kv.CheckMinDone(insid, curProposal)
	kv.freeMem()

	reply.Err = OK
}

func (kv *ShardKV) Snapshot() {
	// send snapshot to ours and record cur state to snapshots
	// check if already snapshot current state
	// according to atomic
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, ok := kv.snapshots[kv.config.Num]
	curProposal := Op{"SnapShot", "", "", kv.config.Num, nil, -1}
	insid := kv.px.Max()
	if insid == -1 {
		insid = 0
	}
	for !ok {
		//DPrintf("Snapshot: %v\n", kv.snapshots)

		tmpReply := &GetReply{}
		kv.rpcRoutine(curProposal, insid, tmpReply, kv.snapProp)
		_, ok = kv.snapshots[kv.config.Num]
	}

	// send rpc to other servers in the same group to
}

func (kv *ShardKV) checkGG(curCfg shardmaster.Config) bool {
	//if len(kv.config.Groups) > 0 {
	//backupGrp := map[GGPair]bool{}
	//DPrintf("me:%d-%d\tchange config from %v to %v\n", kv.gid, kv.me, kv.config, curCfg)
	// check if there is new group take over current group
	//shards := make(map[int]int64)
	groups := make(map[int64][]int)
	DPrintf("selfCfg: %d\t%v\n", kv.config.Num, kv.config.Shards)
	DPrintf("nextCfg: %d\t%v\n", curCfg.Num, curCfg.Shards)
	for i, gid := range kv.config.Shards {
		curGid := curCfg.Shards[i]

		//if b, bok := backupGrp[GGPair{gid, curGid}]; (!bok || (bok && !b)) && gid != curGid {
		//DPrintf("me:%d-%d shard:%d gid:%d curgid:%d\n", kv.gid, kv.me, i, gid, curGid)
		//backupGrp[GGPair{gid, curGid}] = true
		if gid != curGid {
			if kv.gid == gid {
				// replica to others, snapshot self
				kv.Snapshot()
				return true
			} else if kv.gid == curGid {
				// others replica to self, reconfig self
				if gid > 0 {
					//shards[i] = gid
					if _, ok := groups[gid]; !ok {
						groups[gid] = make([]int, 0)
					}
					groups[gid] = append(groups[gid], i)
				}
			}
		}
	}
	if len(groups) > 0 {
		kv.Reconfig2(groups)
		return true
	}
	return false
}

func (kv *ShardKV) updateCfg(curCfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// update configuration
	kv.config.Num = curCfg.Num
	kv.config.Shards = curCfg.Shards
	shardmaster.CopyMap(kv.config.Groups, curCfg.Groups)
}

func (kv *ShardKV) sameShard(sh1 [shardmaster.NShards]int64, sh2 [shardmaster.NShards]int64) bool {
	for i, s := range sh1 {
		if s != sh2[i] {
			return false
		}
	}
	return true
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	curCfg := kv.sm.Query(-1)
	for kv.config.Num != curCfg.Num {
		DPrintf("me%d-%d\tlatestCfg: %d\t%v\n", kv.gid, kv.me, curCfg.Num, curCfg.Shards)
		tmpCfg := kv.sm.Query(kv.config.Num + 1)
		for kv.sameShard(tmpCfg.Shards, kv.config.Shards) && (tmpCfg.Num != curCfg.Num) {
			kv.updateCfg(tmpCfg)
			tmpCfg = kv.sm.Query(kv.config.Num + 1)
		}
		isChanged := kv.checkGG(tmpCfg)
		kv.updateCfg(tmpCfg)
		DPrintf("me:%d-%d\tafter change config to %v\n\n", kv.gid, kv.me, kv.config.Shards)
		if isChanged {
			break
		}
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
	kv.db = make(map[string]string)
	kv.snapshots = make(map[int]SnapShot)
	kv.reconfigs = make(map[int]SnapShot)
	kv.bReCfg = make(map[int]bool)
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
