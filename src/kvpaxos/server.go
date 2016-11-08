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
	DPrintf("print db:\t%d\n", kv.me)
	for key, value := range kv.db {
		DPrintf("Key: %v Value: %v\n", key, ShrinkValue(value))
	}
	DPrintf("\n")
}

func (kv *KVPaxos) printop(op Op) string {
	return fmt.Sprintf("Op: Key: %v oper: %v Value: %v", op.Key, op.Oper, ShrinkValue(op.Value))
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
	db        map[string]string
	statusMap []Op           // loglist = make([]Op, 0)
	uidmap    map[int64]bool //uidmap = make(map[int64]bool)
}

func (kv *KVPaxos) interpretLog(insid int) {
	var loglist []Op
	loglist = make([]Op, 0)
	_, curLog := kv.px.Status(insid)
	to := 10 * time.Millisecond
	// interpret the log before that point to make sure its key/value db reflects all recent put()s
	for pre_insid := insid - 1; pre_insid >= 0; pre_insid-- {
		DPrintf("interpretLog: call Status\t%d\n", pre_insid)
		err := paxos.Pending
		var pre_v interface{}
		//err, pre_v := kv.px.Status(pre_insid)
		for err != 1 {
			err, pre_v = kv.px.Status(pre_insid)
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}
		//		DPrintf("interpretLog: call status\t%d\n", err)
		// if find a log is put then break
		if _, isop := pre_v.(Op); isop {
			DPrintf("interpretLog: \t%d key-%v op-%v\n", pre_insid, pre_v.(Op).Key, pre_v.(Op).Oper)
			loglist = append(loglist, pre_v.(Op))
			if pre_v.(Op).Oper == "Put" && pre_v.(Op).Key == curLog.(Op).Key {
				DPrintf("interpretLog:\t break size-%d key-%v\n", len(loglist), curLog.(Op).Key)
				break
			}
		} else {
			DPrintf("interpretLog: \t%d not op-%v\n", pre_insid, pre_v)
		}

	}
	// figure out all the put/append from start to last same key put
	DPrintf("interpretLog:\t size-%d key-%v\n", len(loglist), curLog.(Op).Key)
	kv.db[curLog.(Op).Key] = ""
	for logidx := len(loglist) - 1; logidx >= 0; logidx-- {
		logentry := loglist[logidx]
		DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
		if logentry.Key == curLog.(Op).Key {
			if logentry.Oper == "Put" {
				kv.db[logentry.Key] = logentry.Value

				DPrintf("interpretLog: put k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			} else if logentry.Oper == "Append" {
				kv.db[logentry.Key] += logentry.Value
				DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			}
		}

	}
	kv.printdb()
}

func (kv *KVPaxos) interpretLog2(insid int) {
	_, curLog := kv.px.Status(insid)

	to := 10 * time.Millisecond

	// step 1: remove seq with duplicate ids
	for pre_insid := len(kv.statusMap); pre_insid < insid; pre_insid++ {
		DPrintf("interpretLog %d: call Status\t%d\n", kv.me, pre_insid)
		err, pre_v := kv.px.Status(pre_insid)

		for err != 1 {
			err, pre_v = kv.px.Status(pre_insid)
			time.Sleep(to)
			if to < 10*time.Second {
				to *= 2
			}
		}

		if _, isop := pre_v.(Op); isop {
			DPrintf("interpretLog: \t%d key-%v op-%v\n", pre_insid, pre_v.(Op).Key, pre_v.(Op).Oper)
			if kv.uidmap[pre_v.(Op).Uid] {
				// already true, discard this entry
				DPrintf("interpretLog: \t%d duplicate:%d\n", pre_insid, pre_v.(Op).Uid)
			} else {
				DPrintf("interpretLog: \t%d notduplicate:%d\n", pre_insid, pre_v.(Op).Uid)
				kv.statusMap = append(kv.statusMap, pre_v.(Op))
				kv.uidmap[pre_v.(Op).Uid] = true
			}
		} else {
			DPrintf("interpretLog: \t%d not op-%v\n", pre_insid, pre_v)
		}
	}
	// step 2: interpret the log before that point to make sure its key/value db reflects all recent put()s
	startIdx := 0
	for logidx := len(kv.statusMap) - 1; logidx >= 0; logidx-- {
		// if find a log is put then break
		DPrintf("interpretLog: \tkey-%v op-%v\n", kv.statusMap[logidx].Key, kv.statusMap[logidx].Oper)
		if kv.statusMap[logidx].Oper == "Put" && kv.statusMap[logidx].Key == curLog.(Op).Key {
			startIdx = logidx
			DPrintf("interpretLog:\t start idx-%d key-%v\n", startIdx, curLog.(Op).Key)
			break
		}

	}
	// step 3: figure out all the put/append from start to last same key put
	DPrintf("interpretLog:\t start idx-%d key-%v\n", startIdx, curLog.(Op).Key)
	kv.db[curLog.(Op).Key] = ""
	for logidx := startIdx; logidx < len(kv.statusMap); logidx++ {
		logentry := kv.statusMap[logidx]
		DPrintf("log entry:%d op-%v\n", logidx, logentry.Oper)
		if logentry.Key == curLog.(Op).Key {
			if logentry.Oper == "Put" {
				kv.db[logentry.Key] = logentry.Value
				DPrintf("interpretLog: put k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			} else if logentry.Oper == "Append" {
				kv.db[logentry.Key] += logentry.Value
				DPrintf("interpretLog: app k-%v v-%v equals-%v\n", logentry.Key, ShrinkValue(logentry.Value), ShrinkValue(kv.db[logentry.Key]))
			}
		}

	}
	kv.printdb()
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
				DPrintf("Get seq-%d\tcurLog-%v\tstatusLop-%v\n", insid, kv.printop(curProposal), kv.printop(proposal.(Op)))
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					kv.interpretLog2(insid)
					reply.Err = OK
					reply.Value = kv.db[args.Key]
					//kv.MarkLog(Op{"Get", args.Key, ""}, insid)
					return nil
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("Get RPC %d: id-%d\tkey-%v\n", kv.me, insid, args.Key)
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("Get seq-%d not op-%v\n", insid, proposal)
			}

		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

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
				DPrintf("PutAppend seq-%d\tcurLog-%v\tstatusLop-%v\n", insid, kv.printop(curProposal), kv.printop(proposal.(Op)))
				// check if it is exact the same propose
				if proposal.(Op) == curProposal {
					kv.interpretLog2(insid)
					reply.Err = OK
					//kv.MarkLog(Op{"Get", args.Key, ""}, insid)
					return nil
				} else {
					// wrong proposal
					insid = kv.px.Max() + 1
					DPrintf("PutAppend RPC %d: id-%d\tkey-%v\tvalue-%v\n", kv.me, insid, args.Key, ShrinkValue(args.Value))
					kv.px.Start(insid, curProposal)
					to = 10 * time.Millisecond
				}
			} else {
				DPrintf("PutAppend seq-%d not op-%v\n", insid, proposal)
			}

		}
		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}

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
	kv.db = make(map[string]string)
	kv.statusMap = make([]Op, 0)
	kv.uidmap = make(map[int64]bool)

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
