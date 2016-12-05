package diskv

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
import "encoding/base32"
import "math/rand"
import "shardmaster"
import "io/ioutil"
import "strconv"
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
	//Uidmap map[int64]int //uidmap = make(map[int64]bool)
	UidWsh map[int]map[int64]int
	DB     map[string]string
	//U2S    map[int64]int //uidmap = make(map[int64]bool)
}

type KeyOpPair struct {
	key string
	op  string
}

type GGPair struct {
	bf int64
	af int64
}

type DisKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos
	dir        string // each replica has its own data directory

	gid int64 // my replica group ID

	// Your definitions here.
	config    shardmaster.Config
	lastLogId int
	//uidmap    map[int64]int //uidmap = make(map[int64]bool)
	logCache  map[int]Op // logCache = make(map[int]Op)
	db        map[string]string
	validList map[int]bool // validList = make(map[int]bool, 0)
	minDone   int

	snapshots map[int]SnapShot
	reconfigs map[int]SnapShot
	bReCfg    map[int]bool

	//U2S map[int64]int //uidmap = make(map[int64]bool)
	uidWsh map[int]map[int64]int

	updateMu sync.Mutex
	loadMu   sync.Mutex
}

func (kv *DisKV) printLog() {
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

func (kv *DisKV) rpcRoutine(curProposal Op, insid int, reply *GetReply, afterProp rpcFunc) {
	// check if it just come back from crash
	kv.checkCrash()

	if insid <= kv.lastLogId {
		insid = kv.lastLogId + 1
	}

	DPrintf("%v RPC me:%d-%d id-%d\t%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal)
	kv.px.Start(insid, curProposal)
	if (insid - kv.lastLogId) > 5 {
		kv.px.Lab4print()
	}

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

func (kv *DisKV) rmDuplicate(insid int, curLog Op) {
	// step 1: remove seq with duplicate ids
	DPrintf("enter rmDuplicate for %d to %d-%v\n", kv.lastLogId, insid, curLog)
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
			//if u, uidok := kv.uidmap[pre_v.(Op).Uid]; uidok && (u != 0) {
			if u, uidok := kv.uidWsh[key2shard(pre_v.(Op).Key)][pre_v.(Op).Uid]; uidok && (u != 0) {
				// already true, discard this entry
				DPrintf("rmDuplicate: \t%d duplicate:%d-%d\n", pre_insid, pre_v.(Op).Uid, u)
			} else {
				DPrintf("rmDuplicate: \t%d notdup:%d-%d\n", pre_insid, pre_v.(Op).Uid, u)
				kv.logCache[pre_insid] = pre_v.(Op)
				kv.validList[pre_insid] = true
			}
		} else {
			DPrintf("rmDuplicate: \t%d not GET/PUT/APP op-%v\n", pre_insid, pre_v)
			kv.validList[pre_insid] = true
		}
	}
	DPrintf("end rmDuplicate for %d to %d-%v\n", kv.lastLogId, insid, curLog)
}

func (kv *DisKV) Update(args *UpdateArgs, reply *UpdateReply) error {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()
	DPrintf("enter Update RPC %d-%d\n", kv.gid, kv.me)

	reply.UidWsh = make(map[int]map[int64]int)
	reply.DB = make(map[string]string)

	kv.updateMu.Lock()
	defer kv.updateMu.Unlock()
	if sn, ok := kv.snapshots[args.SnapNo]; ok {
		CopyMapSS(reply.DB, sn.DB)
		CopyMapMM(reply.UidWsh, sn.UidWsh)
		reply.Err = OK
		DPrintf("Update me:%d-%d\tdb-%v\nlog", kv.gid, kv.me, reply.DB)
	} else {
		DPrintf("Update me:%d-%d not snapshot yet:%v\n", kv.gid, kv.me, args)
		reply.Err = ErrNoSnap
	}
	return nil
}

func (kv *DisKV) interpretRecfg(logidx int, logentry Op) string {
	DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
	//if v, vok := kv.validList[logidx]; !vok || (vok && v) {
	if _, rcok := kv.reconfigs[logentry.ConfigNo]; !rcok {
		DPrintf("do recfg me:%d-%d WRONG not yet recfg-%d db-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db)
		// stop here until snapshot is ready
		return NOK
	} else if _, brok := kv.bReCfg[logentry.ConfigNo]; brok {
		DPrintf("do recfg me:%d-%d dupReconfig recfg-%d db-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db)
	} else {
		DPrintf("do recfg me:%d-%d before recfg-%d db-%v uid-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidWsh)
		CopyMapSS(kv.db, kv.reconfigs[logentry.ConfigNo].DB)
		CopyMapMM(kv.uidWsh, kv.reconfigs[logentry.ConfigNo].UidWsh)
		DPrintf("do recfg me:%d-%d already recfg-%d db-%v uid-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidWsh)
		kv.bReCfg[logentry.ConfigNo] = true
	}
	return OK
}

func (kv *DisKV) interpretSnap(logentry Op) {
	DPrintf("do snapshot me:%d-%d %v\n", kv.gid, kv.me, logentry)
	if sn, snok := kv.snapshots[logentry.ConfigNo]; !snok {
		DPrintf("do snapshot me:%d-%d for cfg-%d db-%v uid-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidWsh, sn)
		kv.updateMu.Lock()
		//kv.snapshots[logentry.ConfigNo] = SnapShot{kv.uidmap, kv.db, kv.U2S}
		kv.snapshots[logentry.ConfigNo] = SnapShot{kv.uidWsh, kv.db}
		kv.updateMu.Unlock()
		DPrintf("do snapshot me:%d-%d after cfg-%d sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.snapshots[logentry.ConfigNo])
		// update atomic
	} else {
		DPrintf("do snapshot me:%d-%d already cfg-%d db-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, sn)
	}
}

func (kv *DisKV) saveToDisk(logentry Op, value string, logidx int) {
	// save the new k/v

	err := kv.filePut(key2shard(logentry.Key), logentry.Key, value)
	DPrintf("saveToDisk me:%d-%d err-%v\n", kv.gid, kv.me, err)
	// save the latest logidx
	kv.filePut(LOGSHARD, LOGFILE, strconv.Itoa(logidx))
	// save the new uid map
	kv.filePut(UIDSHARD+key2shard(logentry.Key), strconv.FormatInt(logentry.Uid, 10), strconv.Itoa(logidx))
	// save the paxos
	//kv.filePut(PAXOSSHARD, strconv.Itoa(logidx), enc(kv.logCache[logidx]))
}

func (kv *DisKV) interpretLog(insid int, curLog Op) string {
	DPrintf("enter interpretLog %d-%v\n", insid, curLog)
	// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
	if dupSeqid, dok := kv.uidWsh[key2shard(curLog.Key)][curLog.Uid]; dok && (curLog.Uid != -1) && ((dupSeqid != insid) || (dupSeqid == -1)) {
		DPrintf("interpretLog: duplicate-%d and skip %v v-%v uid-%v\n", dupSeqid, curLog, kv.db[curLog.Key], kv.uidWsh)
		if kv.lastLogId > dupSeqid {
			return kv.db[curLog.Key]
		}
	}
	DPrintf("me:%d-%d interpretLog: startIdx-%d insid-%d cur-%v\nwhole log:\t", kv.gid, kv.me, kv.lastLogId, insid, curLog)
	kv.printLog()

	// step 3: figure out all the put/append from start to last same key put
	for logidx := kv.lastLogId + 1; logidx <= insid; logidx++ {
		logentry, logok := kv.logCache[logidx]
		if logok {
			// check if duplicate
			if dupSeqid, dok := kv.uidWsh[key2shard(logentry.Key)][logentry.Uid]; dok && (dupSeqid != 0) && (logentry.Uid != -1) && ((dupSeqid != logidx) || (dupSeqid == -1)) {
				delete(kv.logCache, logidx)
				DPrintf("interpretLog: duplicate %d with %d and remove %v v-%v uid-%v\n", logidx, dupSeqid, kv.logCache, kv.db[curLog.Key], kv.uidWsh)
				kv.printLog()
				continue
			}

			if logentry.Oper == "Put" {
				kv.db[logentry.Key] = logentry.Value
				DPrintf("interpretLog: put k-%v v-%v\n", logentry.Key, logentry.Value)
				// do the persistence
				kv.saveToDisk(logentry, kv.db[logentry.Key], logidx)
			} else if logentry.Oper == "Append" {
				kv.db[logentry.Key] += logentry.Value
				DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, logentry.Value, kv.db[logentry.Key])
				// do the persistence
				kv.saveToDisk(logentry, kv.db[logentry.Key], logidx)
			} else if logentry.Oper == "Reconfig" {
				ret := kv.interpretRecfg(logidx, logentry)
				if ret == NOK {
					return ret
				}
				DPrintf("interpretLog me:%d-%d %v result-%v\n", kv.gid, kv.me, logentry, kv.db)
			} else if logentry.Oper == "SnapShot" {
				// apply current state to that snapshot
				kv.interpretSnap(logentry)
			}

			kv.lastLogId = logidx
			if logentry.Uid != -1 {
				DPrintf("bf uid assign [%d]%d\t", logentry.Uid, kv.uidWsh[key2shard(logentry.Key)][logentry.Uid])
				kv.updateMu.Lock()
				kv.uidWsh[key2shard(logentry.Key)][logentry.Uid] = logidx
				kv.updateMu.Unlock()
				DPrintf("af uid assign [%d]%d\n", logentry.Uid, kv.uidWsh[key2shard(logentry.Key)][logentry.Uid])
			}
		}
	}
	return kv.db[curLog.Key]
}

func (kv *DisKV) updateMinDone() {
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

func (kv *DisKV) CheckMinDone(insid int, curProposal Op) {
	DPrintf("enter CheckMinDone seq:%d\t%v\n", insid, curProposal)
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

func (kv *DisKV) freeMem() {
	DPrintf("enter freeMem\n")
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

func (kv *DisKV) loadFromDisk(reply *LoadDiskReply) int {
	kv.loadMu.Lock()
	defer kv.loadMu.Unlock()
	reply.LastLogId = -1
	reply.DB = make(map[string]string)
	reply.UidWsh = make(map[int]map[int64]int)
	reply.LogCache = make(map[int]Op, 0)
	lens := 0
	for i := 0; i < UIDSHARD+10; i++ {
		m := kv.fileReadShard(i)
		//DPrintf("load from disk shard-%d map-%v\n", i, m)
		lens += len(m)
		if len(m) == 0 {
			continue
		}
		if i < LOGSHARD {
			CopyMapSS(reply.DB, m)
		} else if i == LOGSHARD {
			valu, _ := strconv.ParseInt(m[LOGFILE], 10, 32)
			reply.LastLogId = int(valu)
		} else if i < PAXOSSHARD {
			CopyUidMap(reply.UidWsh, m, i-UIDSHARD)
		}
		// else {
		// 	CopyLogCache(reply.LogCache, m)
		// }
		DPrintf("after loading %v\n", reply)
	}
	return lens
}

func (kv *DisKV) repropose() {
	for k, v := range kv.logCache {
		for k > kv.lastLogId {
			to := 10 * time.Millisecond
			status, proposal := kv.px.Status(k)
			if status == paxos.Decided {
				// check if it is exact the same propose
				if kv.sameOp(proposal.(Op), v) {
					// correct
				} else {
					DPrintf("in repropose wrong %v should be %v\n", proposal, v)
				}
				continue
			} else {
				kv.px.Start(k, v)
				to = 10 * time.Millisecond
			}
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
	}
}

func (kv *DisKV) checkCrash() {
	DPrintf("enter checkCrash %d\n", kv.lastLogId)
	if kv.lastLogId == -1 { // might be just crashed or the first time, which is fine
		// load from disk
		reply := &LoadDiskReply{}
		kv.loadFromDisk(reply)
		CopyMapSS(kv.db, reply.DB)
		kv.lastLogId = reply.LastLogId
		CopyMapMM(kv.uidWsh, reply.UidWsh)
		//CopyMapIO(kv.logCache, reply.LogCache)
		kv.repropose()

		//if lens == 0 {
		// might be just clean the disk or the first time, ask other replicas in the group
		servers, sok := kv.config.Groups[kv.gid]
		if sok {
			// try each server in that replication group.
			for _, srv := range servers {
				//if servers != kv.me {
				args := &LoadDiskArgs{nrand() % MAXUID}
				var reply LoadDiskReply
				ok := call(srv, "DisKV.LoadDisk", args, &reply)
				DPrintf("checkCrash me:%d-%d after call LoadDisk %v lastId-%d\n\tresult-%v\n", kv.gid, kv.me, srv, kv.lastLogId, kv.db)
				//}
				if ok && reply.Err == OK {
					if reply.LastLogId > 1 && ((reply.LastLogId - kv.lastLogId) > 1) {
						CopyMapSS(kv.db, reply.DB)
						kv.lastLogId = reply.LastLogId
						CopyMapMM(kv.uidWsh, reply.UidWsh)
						//CopyMapIO(kv.logCache, reply.LogCache)
					}
					// if reply.LastLogId > -1 {
					// 	break
					// }
				}
			}
		}
		//}
	}
}

func (kv *DisKV) emptyRpc(curProposal Op, insid int, reply *GetReply) {

	DPrintf("insid:%d\n", insid)
	kv.rmDuplicate(insid, curProposal)
	reply.Value = kv.interpretLog(insid, curProposal)
	kv.CheckMinDone(insid, curProposal)
	kv.freeMem()
	if reply.Value == NOK {
		reply.Err = NOK
	} else {
		reply.Err = OK
	}
}

func (kv *DisKV) rfgProp(curProposal Op, insid int, reply *GetReply) {

	kv.mu.Lock()
	defer kv.mu.Unlock()
	// step 1: remove seq with duplicate ids
	curRecfg := SnapShot{make(map[int]map[int64]int), make(map[string]string)}
	for groupid, shards := range curProposal.GSmap {
		servers, sok := kv.config.Groups[groupid]
		for sok {
			// try each server in that replication group.
			for _, srv := range servers {
				args := &UpdateArgs{curProposal.ConfigNo, nrand() % MAXUID}
				var reply UpdateReply
				DPrintf("rfgProp me:%d-%d before call %d\n\tresult-%v\n", kv.gid, kv.me, groupid, kv.db)
				ok := call(srv, "DisKV.Update", args, &reply)
				if ok && (reply.Err == OK) {
					// update from the reply data
					CopyMapSSSh(curRecfg.DB, reply.DB, shards)
					CopyMapMMSh(curRecfg.UidWsh, reply.UidWsh, shards)
					//CopyMapIISh(curRecfg.Uidmap, reply.Uidmap, shards, reply.U2S)
					DPrintf("interLog me:%d-%d after update %v\n\tresult-%v\n", kv.gid, kv.me, curProposal, curRecfg.DB)
					sok = false
					break
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	kv.reconfigs[curProposal.ConfigNo] = curRecfg
	DPrintf("interLog me:%d-%d after update check reconfig %v\n", kv.gid, kv.me, kv.reconfigs)
	reply.Err = OK
}

func (kv *DisKV) Reconfig(gs map[int64][]int) {
	// Your code here.
	curProposal := Op{"Reconfig", "", "", kv.config.Num, gs, -1}
	insid := kv.px.Max()
	if insid == -1 {
		insid = 0
	}

	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.rfgProp)
}

func (kv *DisKV) snapProp(curProposal Op, insid int, reply *GetReply) {
	// snapshot current state
	// deal with all previous logs
	// step 1: remove seq with duplicate ids
	kv.rmDuplicate(insid, curProposal)
	kv.interpretLog(insid, curProposal)
	kv.CheckMinDone(insid, curProposal)
	kv.freeMem()

	reply.Err = OK
}

func (kv *DisKV) Snapshot() {
	// send snapshot to ours and record cur state to snapshots
	// check if already snapshot current state
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
}

func (kv *DisKV) checkGG(curCfg shardmaster.Config) bool {
	// check if there is new group take over current group
	groups := make(map[int64][]int)
	DPrintf("selfCfg: %d\t%v\n", kv.config.Num, kv.config.Shards)
	DPrintf("nextCfg: %d\t%v\n", curCfg.Num, curCfg.Shards)
	for i, gid := range kv.config.Shards {
		curGid := curCfg.Shards[i]
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
		kv.Reconfig(groups)
		return true
	}
	return false
}

func (kv *DisKV) updateCfg(curCfg shardmaster.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	// update configuration
	kv.config.Num = curCfg.Num
	kv.config.Shards = curCfg.Shards
	shardmaster.CopyMap(kv.config.Groups, curCfg.Groups)
}

//
// these are handy functions that might be useful
// for reading and writing key/value files, and
// for reading and writing entire shards.
// puts the key files for each shard in a separate
// directory.
//

func (kv *DisKV) shardDir(shard int) string {
	d := kv.dir + "/shard-" + strconv.Itoa(shard) + "/"
	// create directory if needed.
	_, err := os.Stat(d)
	if err != nil {
		if err := os.Mkdir(d, 0777); err != nil {
			log.Fatalf("Mkdir(%v): %v", d, err)
		}
	}
	return d
}

// cannot use keys in file names directly, since
// they might contain troublesome characters like /.
// base32-encode the key to get a file name.
// base32 rather than base64 b/c Mac has case-insensitive
// file names.
func (kv *DisKV) encodeKey(key string) string {
	return base32.StdEncoding.EncodeToString([]byte(key))
}

func (kv *DisKV) decodeKey(filename string) (string, error) {
	key, err := base32.StdEncoding.DecodeString(filename)
	return string(key), err
}

// read the content of a key's file.
func (kv *DisKV) fileGet(shard int, key string) (string, error) {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	content, err := ioutil.ReadFile(fullname)
	return string(content), err
}

// replace the content of a key's file.
// uses rename() to make the replacement atomic with
// respect to crashes.
func (kv *DisKV) filePut(shard int, key string, content string) error {
	fullname := kv.shardDir(shard) + "/key-" + kv.encodeKey(key)
	tempname := kv.shardDir(shard) + "/temp-" + kv.encodeKey(key)
	//DPrintf("full:%v temp:%v\n", fullname, tempname)
	if err := ioutil.WriteFile(tempname, []byte(content), 0666); err != nil {
		return err
	}
	if err := os.Rename(tempname, fullname); err != nil {
		return err
	}
	return nil
}

// return content of every key file in a given shard.
func (kv *DisKV) fileReadShard(shard int) map[string]string {
	m := map[string]string{}
	d := kv.shardDir(shard)
	files, err := ioutil.ReadDir(d)
	if err != nil {
		log.Fatalf("fileReadShard could not read %v: %v", d, err)
	}
	for _, fi := range files {
		n1 := fi.Name()
		if n1[0:4] == "key-" {
			key, err := kv.decodeKey(n1[4:])
			if err != nil {
				log.Fatalf("fileReadShard bad file name %v: %v", n1, err)
			}
			content, err := kv.fileGet(shard, key)
			if err != nil {
				log.Fatalf("fileReadShard fileGet failed for %v: %v", key, err)
			}
			m[key] = content
		}
	}
	return m
}

// replace an entire shard directory.
func (kv *DisKV) fileReplaceShard(shard int, m map[string]string) {
	d := kv.shardDir(shard)
	os.RemoveAll(d) // remove all existing files from shard.
	for k, v := range m {
		kv.filePut(shard, k, v)
	}
}

func (kv *DisKV) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// check if config is not latest
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
	// propose
	curProposal := Op{"Get", args.Key, "", kv.config.Num, nil, args.Id}
	insid := kv.px.Max() + 1
	DPrintf("after Max insid:%d\n", insid)
	kv.rpcRoutine(curProposal, insid, reply, kv.emptyRpc)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//DPrintf("enter PutAppend %d-%d %v\n", kv.gid, kv.me, args)

	// check if config is not latest
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
	// propose
	curProposal := Op{args.Op, args.Key, args.Value, kv.config.Num, nil, args.Id}
	insid := kv.px.Max() + 1
	DPrintf("after Max insid:%d\n", insid)
	// if insid <= kv.lastLogId {
	// 	insid = kv.lastLogId + 1
	// }
	// DPrintf("insid:%d\n", insid)
	tmpReply := &GetReply{}
	kv.rpcRoutine(curProposal, insid, tmpReply, kv.emptyRpc)
	reply.Err = tmpReply.Err
	return nil
}

// RPC handler for servers asking for disk data
func (kv *DisKV) LoadDisk(args *LoadDiskArgs, reply *LoadDiskReply) error {
	// kv.loadMu.Lock()
	// defer kv.loadMu.Unlock()
	kv.loadFromDisk(reply)

	reply.Err = OK
	return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *DisKV) tick() {
	// Your code here.
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
func (kv *DisKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *DisKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *DisKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *DisKV) isunreliable() bool {
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
// dir is the directory name under which this
//   replica should store all its files.
//   each replica is passed a different directory.
// restart is false the very first time this server
//   is started, and true to indicate a re-start
//   after a crash or after a crash with disk loss.
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int, dir string, restart bool) *DisKV {

	kv := new(DisKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	kv.dir = dir

	// Your initialization code here.
	kv.config = shardmaster.Config{-1, [shardmaster.NShards]int64{}, map[int64][]string{}}
	kv.lastLogId = -1
	//kv.uidmap = make(map[int64]int)
	kv.logCache = make(map[int]Op, 0)
	kv.validList = make(map[int]bool, 0)
	kv.minDone = -1
	kv.db = make(map[string]string)
	kv.snapshots = make(map[int]SnapShot)
	kv.reconfigs = make(map[int]SnapShot)
	kv.bReCfg = make(map[int]bool)
	//kv.U2S = make(map[int64]int)
	kv.uidWsh = make(map[int]map[int64]int)
	for i := 0; i < 10; i++ {
		kv.uidWsh[int(i)] = make(map[int64]int)
	}
	// Don't call Join().

	// log.SetOutput(ioutil.Discard)

	gob.Register(Op{})

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	// log.SetOutput(os.Stdout)

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
				fmt.Printf("DisKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	return kv
}
