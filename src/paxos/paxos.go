package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (Fate, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"

import "os"
import "syscall"
import "sync"
import "sync/atomic"
import "fmt"
import "math/rand"

import "time"
import (
	"reflect"
)

// px.Status() return values, indicating
// whether an agreement has been decided,
// or Paxos has not yet reached agreement,
// or it was agreed but forgotten (i.e. < Min()).
type Fate int

var DEBUG = false
var TESTFOR = false

const (
	Decided   Fate = iota + 1
	Pending        // not yet decided.
	Forgotten      // decided but forgotten.
)

func (px *Paxos) explainStatus(s Fate) string {
	if s == 1 {
		return "Decided"
	} else if s == 2 {
		return "Pending"
	} else {
		return "Forgotten"
	}
}

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	rpcCount   int32 // for testing
	peers      []string
	me         int // index into peers[]

	// Your data here.
	agreem     map[int]*Agreement
	doneSeq    int
	proposalNo int
	propWseq   map[int]int
	minSeq     int
}

func (px *Paxos) print() {
	if DEBUG {
		//px.mu.Lock()
		//fmt.Printf("me:\t%d\n", px.me)
		// fmt.Printf("propose no:%d\n", px.proposalNo)
		// for k, v := range px.propWseq {
		// 	fmt.Printf("seq-%d: prop-%v\n", k, v<<8|px.me)
		// }
		//fmt.Println("decision:")
		for k, v := range px.agreem {

			if TESTFOR {
				//value = ""
				fmt.Printf("me:%d\tdecision:\tseq-%d: vsize-%d sta-%d\thighp-%d\thighA-%d\n", px.me, k, reflect.TypeOf(v.value).Size(), v.status, v.highP, v.highA)
			} else {
				fmt.Printf("me:%d\tdecision:\tseq-%d: v-%v\tsta-%d\thighp-%d\thighA-%d\n", px.me, k, v.value, v.status, v.highP, v.highA)
			}

		}

		fmt.Println("\n")
		//px.mu.Unlock()
	}

}

func (px *Paxos) debugPrintln(a ...interface{}) {
	if DEBUG {
		fmt.Println(a...)
	}
}

func (px *Paxos) debugPrintf(format string, a ...interface{}) {
	if DEBUG {
		fmt.Printf(format, a...)
	}
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	//	px.agreem[seq] = Agreement
	px.mu.Lock()

	if seq >= px.doneSeq {
		_, ok := px.agreem[seq]
		if !ok {
			px.debugPrintf("Start %d: %d NOT FOUND and clear\n", px.me, seq)
			px.agreem[seq] = &Agreement{v, Pending, -1, -1}
		}
	}

	if TESTFOR {
		px.debugPrintf("Start: before propose %d\t\n", seq)
	} else {
		px.debugPrintf("Start: before propose %d\t%v\n", seq, v)
	}
	px.mu.Unlock()

	go px.Propose(seq, v)
	return
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	px.debugPrintf("Done %d:%d\n", px.me, seq)
	px.doneSeq = seq
	// discard
	for k, v := range px.agreem {
		if k < seq && v.status != Forgotten {
			v.status = Forgotten
		}
	}
	//px.forgetFree()
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	maxseq := -1
	for k, _ := range px.agreem {
		if k > maxseq {
			maxseq = k
		}
	}
	return maxseq
}

func (px *Paxos) forgetFree(minseq int) {
	px.print()

	for k, v := range px.agreem {
		if (v.status == Forgotten) && (k <= minseq) {
			px.agreem[k].value = ""
		}
	}
	px.print()
}

func (px *Paxos) GetDone(args *GetDoneArgs, reply *GetDoneReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.debugPrintf("GetDone RPC %d:%d\n", px.me, px.doneSeq)
	reply.DoneSeq = px.doneSeq
	return nil
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
	// You code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	// if px.doneSeq == -1 {
	// 	return -1
	// }
	args := &GetDoneArgs{}
	var reply GetDoneReply

	minseq := px.doneSeq
	for i, p := range px.peers {
		if i != px.me {
			//px.debugPrintf("Min:%v\n", p)
			px.mu.Unlock()
			ok := call(p, "Paxos.GetDone", args, &reply)
			px.mu.Lock()
			if ok {
				if minseq > reply.DoneSeq {
					minseq = reply.DoneSeq
				}
			} else {
				px.debugPrintf("Min: cannot contact some and ret %d\n", px.minSeq)
				return px.minSeq
			}
		}
	}
	//px.minSeq = minseq + 1
	for k, v := range px.agreem {
		if k <= minseq {
			v.status = Forgotten
		}
	}
	px.debugPrintf("Min:%d\n", minseq+1)
	px.forgetFree(minseq)
	return minseq + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (Fate, interface{}) {
	// Your code here.
	px.mu.Lock()
	defer px.mu.Unlock()
	agr, ok := px.agreem[seq]
	if ok {

		if TESTFOR {
			px.debugPrintf("Status %d:\t%d\n", seq, agr.status)
		} else {
			px.debugPrintf("Status %d:\t%v\t%v\n", seq, px.explainStatus(agr.status), agr.value)
		}
		//px.print()

		return agr.status, agr.value
	}
	px.debugPrintf("Status %d:\tnot exist\n", seq)
	return Pending, nil
}

func (px *Paxos) cmpProposalNo(p int) bool {
	if (px.proposalNo<<8 | px.me) < p {
		return true
	}
	return false
}

func (px *Paxos) updateProposalNo(p int) {
	px.proposalNo = p >> 8
}

func (px *Paxos) prepareHelp(args *PrepareArgs, reply *PrepareReply) error {
	//px.debugPrintf("Prepare RPC %d: seq-%v with prop-%d\n", px.me, args.Seq, args.N)
	//px.print()
	seq := args.Seq
	agr, ok := px.agreem[seq]
	// if ok && px.agreem[seq].status != Pending {
	// 	px.debugPrintf("Prepare RPC %d: NOT PENDING for seq-%v with prop-%d\n", px.me, args.Seq, args.N)
	// 	reply.OK = DECIDED
	// 	reply.NHigh = px.agreem[seq].highP
	// 	reply.NAccept = px.agreem[seq].highA
	// 	reply.Value = px.agreem[seq].value
	// 	return nil
	// }

	if !ok {
		//px.debugPrintf("Prepare RPC %d: new for this seq-%d with prop-%d\n", px.me, args.Seq, args.N)
		px.agreem[seq] = &Agreement{"", Pending, args.N, -1}
		//agr = px.agreem[seq]

		if px.cmpProposalNo(args.N) {
			px.updateProposalNo(args.N)
		}

		reply.OK = OK
		reply.NHigh = args.N
	} else if args.N > agr.highP {
		//px.debugPrintf("Prepare RPC %d: seq-%d incoming is bigger prop-%v > high-%d\n", px.me, args.Seq, args.N, agr.highP)

		if px.cmpProposalNo(args.N) {
			px.updateProposalNo(args.N)
		}

		px.agreem[seq].highP = args.N

		reply.OK = OK
		reply.NHigh = args.N
		reply.NAccept = px.agreem[args.Seq].highA
		reply.Value = px.agreem[args.Seq].value
	} else {
		//px.debugPrintf("Prepare RPC %d: seq-%d mine is bigger prop-%d <= high-%d\n", px.me, args.Seq, args.N, agr.highP)
		reply.OK = NOK
		reply.NHigh = px.agreem[seq].highP
		//reply.NAccept = px.agreem[seq].highA
		//reply.Value = px.agreem[seq].value
	}
	//px.debugPrintf("Prepare RPC %d: cur high prop-%d\n", px.me, px.agreem[args.seq].highP)
	px.debugPrintln("prepareHelp")
	px.print()
	return nil
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.prepareHelp(args, reply)
}

func (px *Paxos) prepareNo(seq int) int {
	// px.mu.Lock()
	// defer px.mu.Unlock()
	px.debugPrintf("prepareNo %d: seq-%d N-%d\n", px.me, seq, px.propWseq[seq]<<8|px.me)
	return px.propWseq[seq]<<8 | px.me
}

// phase prepare
func (px *Paxos) preparePhase(seq int, value interface{}) int {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.generateNo(seq)
	_, ok := px.agreem[seq]
	if !ok {
		px.debugPrintf("Propose %d: NOT FOUND and clear\n", px.me, seq)
		px.agreem[seq] = &Agreement{value, Pending, -1, -1}

		//		px.debugPrintf("preparePhase %d: not found in agreement\n", seq)
		//		return 0
		//px.agreem[seq] = &Agreement{"", Pending, -1, -1}
	}

	if px.agreem[seq].status != Pending {
		return -1
	}

	args := &PrepareArgs{seq, px.prepareNo(seq)}
	//args.N =
	//args.seq =
	var reply PrepareReply
	reply.OK = NOK
	if TESTFOR {
		//value = ""
		//px.debugPrintf("preparePhase: prepare args seq-%v\tval_sz-%d\tprop-%d\n", args.Seq, reflect.TypeOf(value).Size(), args.N)
	} else {
		//px.debugPrintf("preparePhase: prepare args seq-%v\tval-%v\tprop-%d\n", args.Seq, value, args.N)
	}

	prepareCnt := 0
	//highA := seq
	//highV := value

	// send to itself
	//.debugPrintln("preparePhase: prepare itself")
	//px.mu.Unlock()

	px.prepareHelp(args, &reply)
	//px.mu.Lock()

	if reply.OK == OK {
		prepareCnt++
	}

	//px.mu.Lock()
	// send prepare rpc to other peers
	//px.debugPrintf("preparePhase: prepare peers seq-%d\tval-%v\tprop-%d len:%d\n", args.seq, value, args.N, len(px.peers))
	for i, p := range px.peers {
		reply.OK = NOK
		if i != px.me {
			//px.debugPrintf("preparePhase: in range peers %d:%v before call\n", i, p)
			//args := &PrepareArgs{}
			//var reply PrepareReply

			if TESTFOR {
				//value = ""
				//px.debugPrintf("preparePhase: prepare peers seq-%d\tval_sz-%d\tprop-%d idx:%d\n", args.Seq, reflect.TypeOf(value).Size(), args.N, i)
			} else {
				//px.debugPrintf("preparePhase: prepare peers seq-%d\tval-%v\tprop-%d idx:%d\n", args.Seq, value, args.N, i)
			}
			px.mu.Unlock()
			ok := call(p, "Paxos.Prepare", args, &reply)
			px.mu.Lock()
			px.debugPrintf("reply:%v %d\n", reply.OK, reply.NHigh)
			if ok && reply.OK == OK {
				prepareCnt++
				if reply.NAccept > px.agreem[seq].highA {
					px.agreem[seq].highA = reply.NAccept
					px.debugPrintf("Prepare reply OK seq-%d update value-%v with value-%v\n", seq, px.agreem[seq].value, reply.Value)
					px.agreem[seq].value = reply.Value
				}
			} else if ok && reply.OK != OK {
				// out-of-date peer need to catch up
				// when propose and meet a higher propose no. we do not need to record that, we only need to improve our round
				if px.proposalNo < reply.NHigh>>8 {
					px.proposalNo = reply.NHigh >> 8
				}
			}
		}
	}
	//px.mu.Unlock()
	//px.debugPrintf("preparePhase %d: seq-%d\tfinish prepare peers maj:%d\n", px.me, seq, prepareCnt)
	//px.print()
	return prepareCnt
}

func (px *Paxos) acceptHelp(args *AcceptArgs, reply *AcceptReply) error {
	_, ok := px.agreem[args.Seq]
	if !ok {
		px.agreem[args.Seq] = &Agreement{"", Pending, args.N, -1}
	}
	if args.N >= px.agreem[args.Seq].highP {
		px.debugPrintf("Accept RPC %d: seq-%d incoming is bigger %d >= %d\n", px.me, args.Seq, args.N, px.agreem[args.Seq].highP)
		px.agreem[args.Seq].highP = args.N
		px.agreem[args.Seq].highA = args.N
		px.agreem[args.Seq].value = args.Value
		reply.OK = true
	} else {
		//px.debugPrintf("Accept RPC %d: seq-%d mine is bigger %d < %d\n", px.me, args.Seq, args.N, px.agreem[args.Seq].highP)
		reply.OK = false
		reply.NHigh = px.agreem[args.Seq].highP
	}
	px.debugPrintln("acceptHelp")
	px.print()
	return nil
}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.acceptHelp(args, reply)
}

func (px *Paxos) acceptPhase(seq int, value interface{}) int {
	px.mu.Lock()
	defer px.mu.Unlock()

	agr, ok := px.agreem[seq]
	if agr.value == "" {
		px.agreem[seq].value = value
		agr = px.agreem[seq]
		px.debugPrintf("acceptPhase seq-%d nil before now-\t%v\n", seq, px.agreem[seq].value)
	} else {
		px.debugPrintf("acceptPhase seq-%d value-\t%v\n", seq, px.agreem[seq].value)
	}
	if !ok {
		px.debugPrintf("acceptPhase %d: not found in agreement\n", seq)
		return 0
	}
	if agr.status != Pending {
		return -1
	}

	args := &AcceptArgs{seq, px.prepareNo(seq), px.agreem[seq].value}

	if TESTFOR {
		// value = ""
		//px.debugPrintf("acceptPhase: accept args seq-%v\tval_sz-%d\tprop-%d\n", args.Seq, reflect.TypeOf(args.Value).Size(), args.N)
	} else {
		//px.debugPrintf("acceptPhase: accept args seq-%v\tval-%v\tprop-%d\n", args.Seq, args.Value, args.N)
	}

	var reply AcceptReply
	reply.OK = false
	acceptCnt := 0

	// send to itself
	px.acceptHelp(args, &reply)
	if reply.OK {
		acceptCnt++
		//px.debugPrintf("accept cnt:%d\n", acceptCnt)
	}

	// px.mu.Lock()
	// defer px.mu.Unlock()
	// send accept rpc to other peers
	for i, p := range px.peers {
		reply.OK = false
		if i != px.me {
			px.mu.Unlock()
			ok := call(p, "Paxos.Accept", args, &reply)
			px.mu.Lock()
			if ok && reply.OK {
				acceptCnt++
				//px.debugPrintf("accept cnt:%d\n", acceptCnt)
			}
			// else if ok && !reply.OK {
			// 	if px.proposalNo < reply.NHigh>>8 {
			// 		px.proposalNo = reply.NHigh >> 8
			// 		px.agreem[seq].highP = reply.NHigh
			// 	}
			// }
		}
	}
	//px.print()
	//px.debugPrintf("acceptPhase %d: seq-%d\t prop-%d\tfinish Accept peers maj:%d\n", px.me, seq, args.N, acceptCnt)
	return acceptCnt
}

func (px *Paxos) decideHelp(args *DecideArgs, reply *DecideReply) error {
	px.debugPrintf("Decide RPC %d: seq-%d\t%v\n", px.me, args.Seq, args.Value)
	//px.debugPrintf("Decide RPC %d: seq-%d\n", px.me, args.Seq)
	_, ok := px.agreem[args.Seq]
	if ok {
		px.agreem[args.Seq].status = Decided
		px.agreem[args.Seq].highA = args.N
		px.agreem[args.Seq].highP = args.N
		px.agreem[args.Seq].value = args.Value
	} else {
		px.agreem[args.Seq] = &Agreement{args.Value, Decided, args.N, args.N}
	}
	px.debugPrintln("decideHelp")
	px.print()

	reply.OK = true
	return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	return px.decideHelp(args, reply)
}

func (px *Paxos) decidePhase(seq int) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	agr, ok := px.agreem[seq]
	if !ok {
		px.debugPrintf("decidePhase %d: not found in agreement\n", seq)
		// px.agreem[args.Seq] = &Agreement{"", Pending, args.N, -1}
		return true
	}
	if agr.status != Pending {
		return false
	}

	args := &DecideArgs{}
	args.Seq = seq
	args.N = px.prepareNo(seq)
	args.Value = px.agreem[seq].value
	var reply DecideReply

	// send to itself
	px.decideHelp(args, &reply)
	// if reply.OK {
	// 	acceptCnt++
	// }
	// px.mu.Lock()
	// defer px.mu.Unlock()
	// send accept rpc to other peers

	for i, p := range px.peers {
		if i != px.me {
			px.mu.Unlock()
			call(p, "Paxos.Decide", args, &reply)
			px.mu.Lock()
			// if ok && reply.OK {
			// 	acceptCnt++
			// }
		}
	}

	return false
	//return acceptCnt
}

func (px *Paxos) generateNo(seq int) int {
	// px.mu.Lock()
	// defer px.mu.Unlock()
	px.proposalNo = px.proposalNo + 1 //rand.Int()%3 + 1
	//px.agreem[seq].highP = px.proposalNo
	//px.debugPrintf("Propose ProposalNo\t%d\n", px.proposalNo)
	px.propWseq[seq] = px.proposalNo
	px.debugPrintf("generateNo %d: ProposalNo\t%d seq-%d N-%d\n", px.me, px.proposalNo, seq, px.propWseq[seq]<<8|px.me)
	return px.proposalNo
}

// do propose
func (px *Paxos) Propose(seq int, value interface{}) {
	//px.mu.Lock()
	// px.mu.Lock()
	// defer px.mu.Unlock()

	ok := true
	prepareCnt := 0
	acceptCnt := 0
	// for ok && px.agreem[seq].status == Pending {
	for ok {
		// choose n, unique and higher than any n seen so far

		//px.agreem[seq].prono = px.proposalNo
		//px.debugPrintf("Propose %d: seq-%d before prepare phase\n", px.me, seq)
		//px.mu.Unlock()
		px.debugPrintf("previous prepare cnt:%d accept cnt:%d seq-%d\n", prepareCnt, acceptCnt, seq)

		prepareCnt = px.preparePhase(seq, value)
		// not pending
		if prepareCnt == -1 {
			break
		}
		acceptCnt = 0
		// send accept rpc to other peers
		if prepareCnt > len(px.peers)/2.0 {
			//px.debugPrintln("Propose: before accept phase")
			acceptCnt = px.acceptPhase(seq, value)
		}
		// if acceptCnt == -1 {
		// 	break
		// }
		if acceptCnt > len(px.peers)/2.0 {
			// send decide to all
			ok = px.decidePhase(seq)
			px.debugPrintf("Propose: after decide phase continue-%t\n", ok)
		}
		time.Sleep(time.Millisecond * 5 * time.Duration(px.me))

		// _, ok = px.agreem[seq]
		// if !ok {
		// 	px.agreem[seq] = &Agreement{"", Pending, -1, -1}
		// }
		//px.mu.Lock()
		//_, ok = px.agreem[seq]
	}
	// seq is already decided, try send proposal to undecided ins
	// for k, v := range px.agreem {
	// 	if v.status == Pending {
	// 		px.debugPrintf("FOR LEARN %d: seq-%d", px.me, k)
	// 		go px.Propose(k, v.value)
	// 	}
	// }
	//px.mu.Unlock()
	//px.Done(seq)
}

//
// tell the peer to shut itself down.
// for testing.
// please do not change these two functions.
//
func (px *Paxos) Kill() {
	atomic.StoreInt32(&px.dead, 1)
	if px.l != nil {
		px.l.Close()
	}
}

//
// has this peer been asked to shut down?
//
func (px *Paxos) isdead() bool {
	return atomic.LoadInt32(&px.dead) != 0
}

// please do not change these two functions.
func (px *Paxos) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&px.unreliable, 1)
	} else {
		atomic.StoreInt32(&px.unreliable, 0)
	}
}

func (px *Paxos) isunreliable() bool {
	return atomic.LoadInt32(&px.unreliable) != 0
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{}
	px.peers = peers
	px.me = me

	// Your initialization code here.
	px.agreem = make(map[int]*Agreement)
	px.doneSeq = -1
	px.proposalNo = -1
	px.propWseq = make(map[int]int)
	px.minSeq = -1

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.isdead() == false {
				conn, err := px.l.Accept()
				if err == nil && px.isdead() == false {
					if px.isunreliable() && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.isunreliable() && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					} else {
						atomic.AddInt32(&px.rpcCount, 1)
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.isdead() == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
