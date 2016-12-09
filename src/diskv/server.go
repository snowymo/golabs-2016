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
	Disk   map[int]map[string]string
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

	srv string

	initMu sync.Mutex
	bInit  bool
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
	DPrintf("enter%v RPC me:%d-%d id-%d\n", curProposal.Oper, kv.gid, kv.me, insid)
	kv.initMu.Lock()
	DPrintf("acquire lock me:%d-%d lastLogId:%d kv.bInit:%v\n", kv.gid, kv.me, kv.lastLogId, kv.bInit)
	if !kv.bInit {
		reply.Err = NOK
		kv.initMu.Unlock()
		return
	} else {
		kv.initMu.Unlock()
	}

	fromDisk := &LoadDiskReply{}
	kv.loadFromDisk(fromDisk)
	kv.lastLogId = fromDisk.LastLogId
	kv.loadMu.Lock()
	CopyMapSS(kv.db, fromDisk.DB)
	CopyMapMM(kv.uidWsh, fromDisk.UidWsh)
	kv.loadMu.Unlock()

	if insid <= kv.lastLogId {
		insid = kv.lastLogId + 1
	}

	DPrintf("%v RPC me:%d-%d id-%d\t%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal)
	kv.px.Start(insid, curProposal)
	//if (insid - kv.lastLogId) > 5 {
	//kv.px.Lab4print()
	//}

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
					DPrintf("%v RPC me:%d-%d newdesign wrong prop id-%d\t%v\n", curProposal.Oper, kv.gid, kv.me, insid, curProposal)
					//reply.Err = NOK
					//return

					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("%v RPC me:%d-%d id-%d\tWrong op type\n", curProposal.Oper, kv.gid, kv.me, insid)
			}
		}
		DPrintf("%v RPC me:%d-%d id-%d\tNOT DECIDED\n", curProposal.Oper, kv.gid, kv.me, insid)
		//kv.px.Lab4print()
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
}

func (kv *DisKV) rmDuplicate(insid int, curLog Op) {
	// step 1: remove seq with duplicate ids
	DPrintf("enter rmDuplicate %d-%d for %d to %d-%v\n", kv.gid, kv.me, kv.lastLogId, insid, curLog)
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
				DPrintf("rmDuplicate %d-%d: \t%d duplicate:%d-%d\n", kv.gid, kv.me, pre_insid, pre_v.(Op).Uid, u)
			} else {
				DPrintf("rmDuplicate %d-%d: \t%d notdup:%d-%d\n", kv.gid, kv.me, pre_insid, pre_v.(Op).Uid, u)
				kv.logCache[pre_insid] = pre_v.(Op)
				kv.validList[pre_insid] = true
			}
		} else {
			DPrintf("rmDuplicate %d-%d: \t%d not GET/PUT/APP op-%v\n", kv.gid, kv.me, pre_insid, pre_v)
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
	reply.Disk = make(map[int]map[string]string)

	kv.loadMu.Lock()
	for i := 0; i < UIDSHARD+10; i++ {
		m := kv.fileReadShard(i)
		reply.Disk[i] = make(map[string]string)
		CopyMapSS(reply.Disk[i], m)
	}
	kv.loadMu.Unlock()

	kv.updateMu.Lock()
	if sn, ok := kv.snapshots[args.SnapNo]; ok {
		CopyMapSS(reply.DB, sn.DB)
		CopyMapMM(reply.UidWsh, sn.UidWsh)
		reply.Err = OK
		DPrintf("Update me:%d-%d\tdb-%v\nlog", kv.gid, kv.me, reply.DB)
	} else {
		DPrintf("Update me:%d-%d not snapshot yet:%v\n", kv.gid, kv.me, args)
		reply.Err = ErrNoSnap
	}
	kv.updateMu.Unlock()
	return nil
}

func (kv *DisKV) interpretRecfg(logidx int, logentry Op) string {
	DPrintf("enter interpretRecfg log entry:%d op-%v\n", logidx, logentry.Oper)
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
		kv.loadMu.Lock()
		for k, v := range kv.reconfigs[logentry.ConfigNo].Disk {
			kv.fileReplaceShard(k, v)
		}
		kv.loadMu.Unlock()
		DPrintf("do recfg me:%d-%d already recfg-%d db-%v uid-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidWsh)
		kv.bReCfg[logentry.ConfigNo] = true
	}
	return OK
}

func (kv *DisKV) interpretSnap(logentry Op) {
	DPrintf("do snapshot me:%d-%d %v\n", kv.gid, kv.me, logentry)
	if sn, snok := kv.snapshots[logentry.ConfigNo]; !snok {
		DPrintf("do snapshot me:%d-%d for cfg-%d db-%v uid-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, kv.uidWsh, sn)
		disk := make(map[int]map[string]string)
		kv.loadMu.Lock()
		for i := 0; i < UIDSHARD+10; i++ {
			m := kv.fileReadShard(i)
			disk[i] = make(map[string]string)
			CopyMapSS(disk[i], m)
		}
		kv.loadMu.Unlock()

		kv.updateMu.Lock()
		kv.snapshots[logentry.ConfigNo] = SnapShot{kv.uidWsh, kv.db, disk}
		kv.updateMu.Unlock()

		DPrintf("do snapshot me:%d-%d after cfg-%d sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.snapshots[logentry.ConfigNo])
		// update atomic
	} else {
		DPrintf("do snapshot me:%d-%d already cfg-%d db-%v sn-%v\n", kv.gid, kv.me, logentry.ConfigNo, kv.db, sn)
	}
}

func (kv *DisKV) saveToDisk(logentry Op, value string, logidx int) {
	// save the new k/v
	st, err := kv.fileGet(LOGSHARD, LOGFILE)
	DPrintf("test saveToDisk me:%d-%d dir-%v diskIdx-%v err-%v logidx-%d\n", kv.gid, kv.me, kv.dir, st, err, logidx)

	it, _ := strconv.ParseInt(st, 10, 32)
	if err != nil {
		it = -1
	}
	if int(it) < logidx {
		// send to majority
		kv.saveToMajority(logentry, value, logidx)

		kv.loadMu.Lock()
		kv.filePut(key2shard(logentry.Key), logentry.Key, value)
		// save the latest logidx
		kv.filePut(LOGSHARD, LOGFILE, strconv.Itoa(logidx))

		if st2, err2 := kv.fileGet(LOGSHARD, LOGFILE); err2 == nil {
			DPrintf("saveToDisk me:%d-%d logidx-%v err-%v\n", kv.gid, kv.me, st2, err2)
		}
		// save the new uid map
		kv.filePut(UIDSHARD+key2shard(logentry.Key), strconv.FormatInt(logentry.Uid, 10), strconv.Itoa(logidx))
		kv.loadMu.Unlock()
		// save the paxos
		//kv.filePut(PAXOSSHARD, strconv.Itoa(logidx), enc(kv.logCache[logidx]))

	}
}

func (kv *DisKV) SaveDisk(args *SaveDiskArgs, reply *SaveDiskReply) error {
	if args.Me == kv.me {
		reply.Err = OK
		DPrintf("%d-%d self\n", kv.gid, kv.me)
		return nil
	}
	DPrintf("%d-%d acquire init lock\n", kv.gid, kv.me)
	// save the new k/v
	kv.initMu.Lock()
	defer kv.initMu.Unlock()
	if kv.bInit == false {
		reply.Err = NOK
		return nil
	}

	st, err := kv.fileGet(LOGSHARD, LOGFILE)
	DPrintf("test SaveDisk me:%d-%d dir-%v diskIdx-%v err-%v logidx-%d\n", kv.gid, kv.me, kv.dir, st, err, args.Logidx)

	it, _ := strconv.ParseInt(st, 10, 32)
	if err != nil {
		it = -1
	}
	if int(it) < args.Logidx {
		kv.loadMu.Lock()
		err := kv.filePut(key2shard(args.Logentry.Key), args.Logentry.Key, args.Value)
		DPrintf("SaveDisk me:%d-%d logidx-%d err-%v\n", kv.gid, kv.me, args.Logidx, err)
		// save the latest logidx
		kv.filePut(LOGSHARD, LOGFILE, strconv.Itoa(args.Logidx))
		// save the new uid map
		kv.filePut(UIDSHARD+key2shard(args.Logentry.Key), strconv.FormatInt(args.Logentry.Uid, 10), strconv.Itoa(args.Logidx))
		kv.loadMu.Unlock()
	}
	reply.LastLogId = int(it)
	reply.Err = OK
	return nil
}

func (kv *DisKV) saveReconfig() {
	kv.loadMu.Lock()
	for k, v := range kv.db {
		kv.filePut(key2shard(k), k, v)
	}

	// save the latest logidx
	kv.filePut(LOGSHARD, LOGFILE, strconv.Itoa(kv.lastLogId))

	// if st2, err2 := kv.fileGet(LOGSHARD, LOGFILE); err2 == nil {

	// }
	// save the  uid map
	for k, uidmap := range kv.uidWsh {
		for uid, seq := range uidmap {
			kv.filePut(UIDSHARD+k, strconv.FormatInt(uid, 10), strconv.Itoa(seq))
		}
	}

	kv.loadMu.Unlock()
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
	//DPrintf("me:%d-%d interpretLog: startIdx-%d insid-%d cur-%v\nwhole log:\t", kv.gid, kv.me, kv.lastLogId, insid, curLog)
	//kv.printLog()

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
				// do the persistence
				kv.saveReconfig()
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
	DPrintf("me:%d-%d call Done %d\n", kv.gid, kv.me, kv.minDone)
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
	reply.LastLogId = -1
	reply.DB = make(map[string]string)
	reply.UidWsh = make(map[int]map[int64]int)
	reply.LogCache = make(map[int]Op, 0)
	reply.Disk = make(map[int]map[string]string)

	DPrintf("enter loadFromDisk %d-%d\n", kv.gid, kv.me)
	kv.loadMu.Lock()
	defer kv.loadMu.Unlock()

	lens := 0
	for i := 0; i < UIDSHARD+10; i++ {
		m := kv.fileReadShard(i)
		//DPrintf("load from disk shard-%d map-%v\n", i, m)
		reply.Disk[i] = make(map[string]string)
		CopyMapSS(reply.Disk[i], m)

		lens += len(m)
		if len(m) == 0 {
			continue
		}
		if i < LOGSHARD {
			CopyMapSS(reply.DB, m)
		} else if i == LOGSHARD {
			DPrintf("load from disk %d-%d shard-%d map-%v\n", kv.gid, kv.me, i, m)
			if v, ok := m[LOGFILE]; ok {
				valu, _ := strconv.ParseInt(v, 10, 32)
				reply.LastLogId = int(valu)
			}

		} else if i < PAXOSSHARD {
			CopyUidMap(reply.UidWsh, m, i-UIDSHARD)
		}
		// else {
		// 	CopyLogCache(reply.LogCache, m)
		// }

	}
	DPrintf("loadFromDisk %d-%d af loading logid-%d\n", kv.gid, kv.me, reply.LastLogId)
	//kv.px.Lab4print()
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

//load from majority
func (kv *DisKV) loadFromMajority(loadReply *LoadDiskReply, diskLogId int) int {
	servers, sok := kv.config.Groups[kv.gid]
	DPrintf("enter loadFromMajority me:%d-%d %v mysrv:%v\n", kv.gid, kv.me, servers, kv.srv)
	checkSrv := make(map[int]bool)

	leastIdx := -1

	if sok {
		ret := 0
		for ret <= (len(servers)/2) || leastIdx < diskLogId { // because it is unreliable
			// try each server in that replication group.
			for i, srv := range servers {
				DPrintf("checkCrash me:%d-%d bf call LoadDisk %v mysrv:%v lastId-%d\n", kv.gid, kv.me, srv, kv.srv, kv.lastLogId)
				if _, err := checkSrv[i]; !err && (srv != kv.srv) {
					//if srv != kv.srv {
					args := &LoadDiskArgs{kv.uidWsh, kv.db, kv.lastLogId, kv.me, nil, nrand() % MAXUID}
					reply := &LoadDiskReply{}
					//DPrintf("call in checkCrash:%d\t", loopidx)
					ok := call(srv, "DisKV.LoadDisk", args, &reply)
					if ok && reply.Err == OK {
						ret++
						checkSrv[i] = true
						DPrintf("checkCrash me:%d-%d after call LoadDisk %v lastId-%d err-%v\tresult-%d\n\n", kv.gid, kv.me, srv, kv.lastLogId, ok, reply.LastLogId)
						if reply.Err == OK {
							if reply.LastLogId > leastIdx {
								leastIdx = reply.LastLogId
								CopyMapSS(loadReply.DB, reply.DB)
								loadReply.LastLogId = reply.LastLogId
								CopyMapMM(loadReply.UidWsh, reply.UidWsh)
								// kv.loadMu.Lock()
								for k, v := range reply.Disk {
									loadReply.Disk[k] = v
									//kv.fileReplaceShard(k, v)
								}
								// kv.loadMu.Unlock()
								//CopyMapIO(kv.logCache, reply.LogCache)
							}
						}
					}
					//}

				}

			}
			DPrintf("checkCrash me:%d-%d af assignment lastId-%d\n", kv.gid, kv.me, kv.lastLogId)
		}
	}
	return leastIdx
}

//save to majority
func (kv *DisKV) saveToMajority(logentry Op, value string, logidx int) {
	servers, sok := kv.config.Groups[kv.gid]
	checkSrv := make(map[int]bool)
	if sok {
		ret := 0
		for ret <= (len(servers) / 2) { // because it is unreliable
			// try each server in that replication group.
			for i, srv := range servers {
				if _, err := checkSrv[i]; !err {
					DPrintf("saveToMajority me:%d-%d bf call SaveDisk %v lastId-%d\n", kv.gid, kv.me, srv, kv.lastLogId)
					//if servers != kv.me {
					args := &SaveDiskArgs{logentry, value, logidx, kv.me, nrand() % MAXUID}
					reply := &SaveDiskReply{}
					//DPrintf("call in checkCrash:%d\t", loopidx)
					ok := call(srv, "DisKV.SaveDisk", args, &reply)

					//}
					if ok && (reply.Err == OK) {
						ret++
						checkSrv[i] = true
						DPrintf("saveToMajority me:%d-%d after call SaveDisk %v lastId-%d err-%v\ret-%d\t%v\n", kv.gid, kv.me, srv, kv.lastLogId, ok, ret, checkSrv)
					}
				}

			}
			DPrintf("saveToMajority me:%d-%d af assignment lastId-%d\n", kv.gid, kv.me, kv.lastLogId)
		}
	}

}

func (kv *DisKV) checkCrash() {

	if (kv.lastLogId == -1) && (kv.bInit == false) {
		DPrintf("enter checkCrash %d-%d %d\n", kv.gid, kv.me, kv.lastLogId)

		// might be just crashed or the first time, which is fine
		fromDisk := &LoadDiskReply{}
		// load from disk
		//lens :=
		kv.loadFromDisk(fromDisk)

		reply := &LoadDiskReply{make(map[int]map[int64]int), make(map[string]string), make(map[int]Op), -1, make(map[int]map[string]string), OK}
		kv.loadFromMajority(reply, fromDisk.LastLogId)
		DPrintf("after loadFromMajority %d-%d:%d\n", kv.gid, kv.me, reply.LastLogId)

		if reply.LastLogId > fromDisk.LastLogId {
			// save reply to ours
			kv.mu.Lock()
			kv.lastLogId = reply.LastLogId
			kv.mu.Unlock()
			kv.mu.Lock()
			CopyMapSS(kv.db, reply.DB)
			CopyMapMM(kv.uidWsh, reply.UidWsh)
			kv.mu.Unlock()
			for k, v := range reply.Disk {
				kv.fileReplaceShard(k, v)
			}
		} else {
			kv.mu.Lock()
			kv.lastLogId = fromDisk.LastLogId
			kv.mu.Unlock()
			kv.loadMu.Lock()
			CopyMapSS(kv.db, fromDisk.DB)
			CopyMapMM(kv.uidWsh, fromDisk.UidWsh)
			kv.loadMu.Unlock()
		}

		// send ours to every one, maybe later, after we finish this RPC
		// if fromDisk.LastLogId > reply.LastLogId {
		// 	kv.sendToMajority(fromDisk.Disk)
		// } else {
		// 	kv.sendToMajority(reply.Disk)
		// }
		kv.initMu.Lock()
		kv.bInit = true
		kv.initMu.Unlock()
	}
	// it only fit for RM1, the question is, does it have to be maj bigger than -1 if me is bigger than -1?
	// for (reply.LastLogId == -1) && (kv.lastLogId != -1) {
	// 	// we need more with disk content? test RM1
	// 	reply = &LoadDiskReply{make(map[int]map[int64]int), make(map[string]string), make(map[int]Op), -1, make(map[int]map[string]string), OK}
	// 	kv.loadFromMajority(fromDisk, reply)
	// 	DPrintf("after loadFromMajority:%d\n", reply.LastLogId)
	// 	time.Sleep(20 * time.Millisecond)
	// }

}

func (kv *DisKV) sendToMajority(disk map[int]map[string]string) {
	servers, sok := kv.config.Groups[kv.gid]
	checkSrv := make(map[int]bool)
	if sok {
		ret := 1
		for ret <= (len(servers) / 2) { // because it is unreliable
			// try each server in that replication group.
			for i, srv := range servers {
				if _, err := checkSrv[i]; !err && (srv != kv.srv) {
					DPrintf("sendToMajority me:%d-%d bf call ReplaceDisk %v lastId-%d\n", kv.gid, kv.me, srv, kv.lastLogId)
					//if servers != kv.me {
					args := &ReplaceDiskArgs{kv.lastLogId, disk, nrand() % MAXUID}
					reply := &ReplaceDiskReply{}
					//DPrintf("call in checkCrash:%d\t", loopidx)

					if ok := call(srv, "DisKV.ReplaceDisk", args, &reply); ok {
						ret++
						checkSrv[i] = true
						DPrintf("sendToMajority me:%d-%d after call ReplaceDisk %v lastId-%d err-%v\tresult-%d\n", kv.gid, kv.me, srv, kv.lastLogId, ok, reply.LastLogId)
					}
				}

			}
			DPrintf("sendToMajority me:%d-%d af assignment lastId-%d\n", kv.gid, kv.me, kv.lastLogId)
		}
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
	curRecfg := SnapShot{make(map[int]map[int64]int), make(map[string]string), make(map[int]map[string]string)}
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
					for k, v := range reply.Disk {
						if contains(shards, k) || contains(shards, k-UIDSHARD) {
							curRecfg.Disk[k] = v
						}
					}
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
	DPrintf("enter Reconfig %d-%d\n", kv.gid, kv.me)
	curProposal := Op{"Reconfig", "", "", kv.config.Num, gs, -1}
	insid := kv.px.Max() + 1
	// if insid == -1 {
	// 	insid = 0
	// }

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
	insid := kv.px.Max() + 1
	// if insid == -1 {
	// 	insid = 0
	// }
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
	kv.srv = args.Srv
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
	DPrintf("Get RPC ret-%v\n", reply.Value)
	return nil
}

// RPC handler for client Put and Append requests
func (kv *DisKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	//DPrintf("enter PutAppend %d-%d %v\n", kv.gid, kv.me, args)
	kv.srv = args.Srv
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
	DPrintf("\tenter LoadDisk %d-%d logid-%d \n", kv.gid, kv.me, kv.lastLogId)
	reply.LastLogId = -1
	if args.Me != kv.me {
		// read from disk first
		kv.loadFromDisk(reply)
		if kv.lastLogId > reply.LastLogId {
			DPrintf("\tLoadDisk %d-%d kv.lastLogId-%d > reply.LastLogId-%d\n", kv.gid, kv.me, kv.lastLogId, reply.LastLogId)
		}

		reply.Err = OK
	} else {
		reply.Err = NOK
		DPrintf("\tenter LoadDisk %d-%d self \n", kv.gid, kv.me)
	}

	return nil
}

func (kv *DisKV) ReplaceDisk(args *ReplaceDiskArgs, reply *ReplaceDiskReply) error {
	if args.LastLogId > reply.LastLogId {
		// load args to self
		//kv.mu.Lock()
		// CopyMapSS(kv.db, args.DB)
		// CopyMapMM(kv.uidWsh, args.UidWsh)
		// kv.lastLogId = args.LastLogId
		kv.loadMu.Lock()
		for k, v := range args.Disk {
			kv.fileReplaceShard(k, v)
		}
		kv.loadMu.Unlock()

		//kv.mu.Unlock()
		//CopyMapIO(kv.logCache, reply.LogCache)
	}
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
			//DPrintf("me%d-%d\tneed to updateCfg: %d\t%v\n", kv.gid, kv.me, curCfg.Num, curCfg.Shards)
		}
		isChanged := kv.checkGG(tmpCfg)
		kv.updateCfg(tmpCfg)
		DPrintf("me:%d-%d\tafter change config to %v isChanged-%v\n\n", kv.gid, kv.me, kv.config.Shards, isChanged)
		if isChanged {
			break
		}
	}
	// as initialize
	// check if it just come back from crash
	kv.checkCrash()
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
	kv.bInit = false
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
