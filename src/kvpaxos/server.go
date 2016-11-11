package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "time"
import (
	"sort"
)

// import (
// 	"math"
// )

const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		// log.Printf(format, a...)
		fmt.Printf(format, a...)
	}
	return
}

func (kv *KVPaxos) printdb() {
	//	DPrintf("print db:\t%d\n", kv.me)
	// for key, value := range kv.db {
	// 	DPrintf("Key: %v Value: %v\n", key, ShrinkValue(value))
	// }
	for i, value := range kv.statusMap {
		DPrintf("i: %d\t%v\n", i, kv.printop(value))
	}
	DPrintf("\n")
}

func (kv *KVPaxos) printop(op Op) string {
	return fmt.Sprintf("Op: Key-%v oper:-%v Value-%v", op.Key, op.Oper, ShrinkValue(op.Value))
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Oper  string
	Key   string
	Value string
	Uid   int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	// Your definitions here.
	db            map[string]string
	statusMap     map[int]Op    // statusMap = make(map[int]Op)
	uidmap        map[int64]int //uidmap = make(map[int64]bool)
	validList     map[int]bool  // validList = make([]bool, 0)
	minDone       int
	lastStatusIdx int
}

func (kv *KVPaxos) interpretLog2(insid int, curLog interface{}) string {
	to := 10 * time.Millisecond

	// step 1: remove seq with duplicate ids
	for pre_insid := kv.lastStatusIdx + 1; pre_insid <= insid; pre_insid++ {
		//DPrintf("interpretLog %d: call Status\t%d\n", kv.me, pre_insid)
		err, pre_v := kv.px.Status(pre_insid)
		// if it is old seq then we ask it to propose
		kv.px.Start(pre_insid, pre_v)
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
				//kv.statusMap = append(kv.statusMap, pre_v.(Op))
				kv.statusMap[pre_insid] = pre_v.(Op)
				kv.lastStatusIdx = pre_insid
				kv.printdb()
				kv.uidmap[pre_v.(Op).Uid] = pre_insid
				kv.validList[pre_insid] = true
			}
		} else {
			//DPrintf("interpretLog: \t%d not op-%v\n", pre_insid, pre_v)
		}
	}
	// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
	// if it is duplicate, then get the value directly
	checkLastPut := insid
	if dupSeqid := kv.uidmap[curLog.(Op).Uid]; dupSeqid != insid {
		if kv.statusMap[dupSeqid].Value == "" {
			checkLastPut = dupSeqid
		} else {
			return kv.statusMap[dupSeqid].Value
		}
	}
	// if it is not duplicate, or did not get older Get before
	startIdx := 0
	for logidx := checkLastPut; logidx >= 0; logidx-- {
		// if find a log is put then break
		if kv.statusMap[logidx].Oper == "Put" && kv.statusMap[logidx].Key == curLog.(Op).Key {
			startIdx = logidx
			break
		}
	}
	// step 3: figure out all the put/append from start to last same key put
	DPrintf("before access kv.statusMap[%d].Value\n", insid)
	kv.printdb()
	//kv.statusMap[insid].Value = ""
	updatedOp := kv.statusMap[insid]
	updatedOp.Value = ""
	for logidx := startIdx; logidx < checkLastPut; logidx++ {
		logentry, logok := kv.statusMap[logidx]
		//DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
		if logok && logentry.Key == curLog.(Op).Key {
			if logentry.Oper == "Put" {
				updatedOp.Value = logentry.Value
				//DPrintf("interpretLog: put k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			} else if logentry.Oper == "Append" {
				updatedOp.Value += logentry.Value
				//DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			}
		}

	}
	kv.statusMap[insid] = updatedOp
	kv.printdb()
	return updatedOp.Value

}

// func (kv *KVPaxos) MarkLog(oper Op, insid int) {
// 	if oper.Oper == "Put" || oper.Oper == "Get" {
// 		for pre_id := insid - 1; pre_id >= 0; pre_id-- {
// 			_, pre_op := kv.px.Status(pre_id)
// 			if pre_op.(Op).Oper == oper.Oper && pre_op.(Op).Key == oper.Key {
// 				if kv.valid[pre_id] == false {
// 					break
// 				}
// 				kv.valid[pre_id] = false
// 			}
// 		}
// 	}
// 	// find min valid and call Done and Min
// 	for i, v := range kv.valid {
// 		if v {
// 			kv.px.Done(i - 1)
// 			kv.px.Min()
// 			break
// 		}
// 	}
// }

func (kv *KVPaxos) sameOp(oper1 string, oper2 string) bool {
	DPrintf("oper1:%v\toper2:%v\n", oper1, oper2)
	if oper1 == "Get" && oper2 == "Get" {
		return true
	} else if oper1 == "Put" && oper2 == "Put" {
		return true
	} else if oper1 == "Put" && oper2 == "Append" {
		return true
	}
	return false
}

func (kv *KVPaxos) printValid() {
	for i, v := range kv.validList {
		DPrintf("me:%d\tvalid list:%d\t%t\n", kv.me, i, v)
	}
}

type KeyOpPair struct {
	key string
	op  string
}

func (kv *KVPaxos) turnAppendtoPut(op string) string {
	if op == "Append" {
		return "Put"
	}
	return op
}

func (kv *KVPaxos) CheckMinDone(insid int, curProposal Op) {
	//DPrintf("CheckMinDone seq:%d\t%v\n", insid, kv.printop(curProposal))
	// add itself
	//kv.validList[insid] = true
	//kv.printValid()
	if curProposal.Oper == "Append" {
		return
	}

	keyop := make(map[KeyOpPair]bool)

	curPair := KeyOpPair{curProposal.Key, curProposal.Oper}
	//DPrintf("CheckMinDone curpair:%v\n", curPair)
	keyop[curPair] = true

	// set all previous op with same key and op to false
	var keyInsids []int
	for keyInsid, _ := range kv.statusMap {
		keyInsids = append(keyInsids, keyInsid)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(keyInsids)))
	for _, keyInsid := range keyInsids {
		if keyInsid > kv.minDone && keyInsid < insid {

			//DPrintf("CheckMinDone check key:%d\n", keyInsid)
			curPair = KeyOpPair{kv.statusMap[keyInsid].Key, kv.turnAppendtoPut(kv.statusMap[keyInsid].Oper)}
			curPairOri := KeyOpPair{kv.statusMap[keyInsid].Key, kv.statusMap[keyInsid].Oper}
			//DPrintf("CheckMinDone curpair:%v\n", curPair)
			if b, ok2 := keyop[curPair]; ok2 && b {
				kv.validList[keyInsid] = false
				//DPrintf("turn to false: seq-%d %t\n", keyInsid, kv.validList[keyInsid])
			} else if b, ok2 = keyop[curPairOri]; !ok2 {
				keyop[curPairOri] = true
			}
		}
	}

	// for pre_insid := insid - 1; pre_insid > kv.minDone && pre_insid < len(kv.statusMap); pre_insid-- {
	// 	DPrintf("CheckMinDone check idx:%d\n", pre_insid)
	// 	curPair = KeyOpPair{kv.statusMap[pre_insid].Key, kv.turnAppendtoPut(kv.statusMap[pre_insid].Oper)}
	// 	DPrintf("CheckMinDone curpair:%v\n", curPair)
	// 	if b, ok2 := keyop[curPair]; ok2 && b {
	// 		kv.validList[pre_insid] = false
	// 		DPrintf("turn to false: seq-%d %t\n", pre_insid, kv.validList[pre_insid])
	// 	} else {
	// 		keyop[curPair] = true
	// 	}
	// }
	// Find the smallest true
	myMinValid := len(kv.validList)
	for k, v := range kv.validList {
		if v {
			if myMinValid > k {
				myMinValid = k
				//DPrintf("smallest true:%d\n", myMinValid)
			}
		}
	}
	// Find the biggest false
	myMaxUValid := -1
	for k, v := range kv.validList {
		if !v {
			if k > myMaxUValid && k < myMinValid {
				myMaxUValid = k
				//DPrintf("biggest false:%d\n", myMaxUValid)
			}
		}
	}
	if kv.minDone < myMaxUValid {
		kv.minDone = myMaxUValid
		//DPrintf("update minDone:%d\n", kv.minDone)
	}
	DPrintf("me: %d\tCheckMinDone before call Done:%d\n", kv.me, kv.minDone)
	kv.px.Done(kv.minDone)
}

func (kv *KVPaxos) freeMem() {
	kv.px.Min()
	// free own attribute about statusMap
	for i, _ := range kv.statusMap {
		if i <= kv.minDone {
			if kv.statusMap[i].Oper != "Get" {
				curOp := kv.statusMap[i]
				curOp.Value = ""
				kv.statusMap[i] = curOp
			}
		}
	}
	//kv.printdb()
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	curProposal := Op{"Get", args.Key, "", args.Id}
	insid := kv.px.Max() + 1
	DPrintf("Get RPC %d: id-%d\tkey-%v\n", kv.me, insid, args.Key)
	kv.px.Start(insid, curProposal)

	// wait for decided
	to := 10 * time.Millisecond
	for {
		status, proposal := kv.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				//DPrintf("Get seq-%d\tcurLog-%v\tstatusLop-%v\n", insid, kv.printop(curProposal), kv.printop(proposal.(Op)))
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					reply.Value = kv.interpretLog2(insid, proposal)
					reply.Err = OK
					kv.CheckMinDone(insid, curProposal)
					//kv.mu.Unlock()
					kv.freeMem()
					//kv.mu.Lock()
					return nil
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("Get RPC %d: id-%d\tkey-%v\n", kv.me, insid, args.Key)
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				//DPrintf("Get seq-%d not op-%v\n", insid, proposal)
			}
		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	//kv.CheckMinDone(insid, curProposal)
	return nil
}

func (kv *KVPaxos) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	curProposal := Op{args.Op, args.Key, args.Value, args.Id}
	// use paxos to allocate a ins, whose value includes k&v and other kvpaxoses know about put() and append()
	insid := kv.px.Max() + 1

	DPrintf("PutAppend RPC %d: id-%d\tkey-%v\tvalue-%v\n", kv.me, insid, args.Key, ShrinkValue(args.Value))

	kv.px.Start(insid, curProposal)

	to := 10 * time.Millisecond
	for {
		status, proposal := kv.px.Status(insid)
		if status == paxos.Decided {
			if _, isop := proposal.(Op); isop {
				//DPrintf("PutAppend seq-%d\tcurLog-%v\tstatusLop-%v\n", insid, kv.printop(curProposal), kv.printop(proposal.(Op)))
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					//kv.interpretLog2(insid)
					reply.Err = OK
					//kv.MarkLog(Op{"Get", args.Key, ""}, insid)
					kv.CheckMinDone(insid, curProposal)
					//kv.mu.Unlock()
					kv.freeMem()
					//kv.mu.Lock()
					return nil
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("PutAppend RPC %d: id-%d\tkey-%v\tvalue-%v\n", kv.me, insid, args.Key, ShrinkValue(args.Value))
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				//DPrintf("PutAppend seq-%d not op-%v\n", insid, proposal)
			}

		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	//kv.CheckMinDone(insid, curProposal)
	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

// call this to find out if the server is dead.
func (kv *KVPaxos) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *KVPaxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *KVPaxos) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	//kv.db = make(map[string]string)
	kv.statusMap = make(map[int]Op, 0)
	kv.uidmap = make(map[int64]int)
	kv.validList = make(map[int]bool, 0)
	kv.minDone = -1
	kv.lastStatusIdx = -1

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
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
