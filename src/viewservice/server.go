package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"
import (
//"container/list"
//"strconv"
)

type ViewServer struct {
	mu       sync.Mutex // for nView and curView
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	nView        uint                 // current view number
	bAcked       bool                 // primary server acked
	mapPingTrack map[string]time.Time // time for recent ping from servers
	curView      View                 // current view
	//idleServer   map[string]int       // server, neither primary nor back, pinged, might turn to array if we have more than two servers at all
	//	muIdle       sync.Mutex           // for idleserver map
	//muPing     sync.Mutex // for ping track map
	viewChange bool
}

func (vs *ViewServer) addPrimary(name string) View {
	//	vs.mu.Lock()

	//vs.nView++
	vs.viewChange = true
	vs.curView = View{vs.nView, name, ""}
	vs.mapPingTrack[name] = time.Now()

	//	vs.mu.Unlock()
	return vs.curView
}

func (vs *ViewServer) print() {
	if DEBUG {
		// print current view
		fmt.Printf("current view:%d p:%s b:%s ack:%t\n", vs.curView.Viewnum, vs.curView.Primary, vs.curView.Backup, vs.bAcked)
		// print idleservers
		//fmt.Printf("idle servers %d\n", len(vs.idleServer))
		// for k, _ := range vs.idleServer {
		// 	fmt.Printf("idle server %s\n", k)
		// }
		//print ping log
		for k, v := range vs.mapPingTrack {
			fmt.Printf("server %s:time %s\n", k, v.String())
		}
	}

}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	// Your code here.
	ckname := args.Me
	viewno := args.Viewnum

	vs.mu.Lock()
	if DEBUG {
		fmt.Printf("ping:\t%v\t%d\n", ckname, viewno)
	}

	vs.mapPingTrack[ckname] = time.Now()
	vs.viewChange = false

	// check current view number
	if vs.nView == 0 {
		// does not have a view now
		reply.View = vs.addPrimary(ckname)
	} else {
		// already has views check the name to see if it is primary/backup/first show
		switch ckname {
		case vs.curView.Primary:
			if viewno == 0 {
				// todo:primary broke and restarted
				//vs.addIdle(ckname)
				// destroy current primary
				//vs.curView.Primary = ""
				vs.promoteToPrimary()
			} else {
				// ack it
				if vs.curView.Viewnum == viewno {
					vs.bAcked = true
				}
			}
		case vs.curView.Backup:
			if viewno == 0 {
				// todo:backup broke and restarted
				//vs.addIdle(ckname)
				//vs.curView.Backup = ""
				vs.promoteToBackup()
			}
		default:
			// new server
			//vs.addIdle(ckname)
		}
	}
	if vs.viewChange {
		vs.nView++
		vs.curView = View{vs.nView, vs.curView.Primary, vs.curView.Backup}
	}
	reply.View = vs.curView
	if DEBUG {
		fmt.Println("in Ping")
	}
	vs.print()
	vs.mu.Unlock()
	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

	// Your code here.
	vs.mu.Lock()
	reply.View = vs.curView
	if DEBUG {
		fmt.Println("in Get:\t" + vs.me)
	}
	vs.print()
	vs.mu.Unlock()
	return nil
}

func (vs *ViewServer) addBackup(name string) {
	//vs.mu.Lock()
	if vs.bAcked {
		//vs.nView++
		vs.viewChange = true
		vs.curView = View{vs.nView, vs.curView.Primary, name}
		vs.bAcked = false
		vs.mapPingTrack[name] = time.Now()
		if DEBUG {
			fmt.Println("in addBackup")
		}
		vs.print()
	}
	//vs.mu.Unlock()
}

func (vs *ViewServer) isPingInTime() bool {
	// vs.muPing.Lock()
	// defer vs.muPing.Unlock()

	for _, v := range vs.mapPingTrack {
		if v.Sub(time.Now()) < DeadPings*PingInterval {
			return true
		}
	}
	return false
}

func (vs *ViewServer) promoteToPrimary() {
	//vs.mu.Lock()
	if vs.bAcked {
		//vs.nView++
		vs.viewChange = true

		if vs.curView.Backup != "" {
			//if len(vs.idleServer) > 0 {
			//	vs.curView = View{vs.nView, vs.curView.Backup, vs.getIdleServer()}
			//} else {
			vs.curView = View{vs.nView, vs.curView.Backup, vs.getIdleServer()}
			//}
		} else {
			fmt.Println("FAIL should not happen")
			vs.removePrimary()
		}
		vs.bAcked = false
		//vs.mapPingTrack[name] = time.Now()
	}
	//vs.mu.Unlock()
}

func (vs *ViewServer) promoteToBackup() {
	//vs.mu.Lock()
	if vs.bAcked {
		//vs.nView++

		newbackup := vs.getIdleServer()
		if newbackup != "" {
			vs.curView = View{vs.nView, vs.curView.Primary, newbackup}
			vs.viewChange = true
			vs.bAcked = false
		} else {
			// no idle for backup
		}
	}
	//vs.mu.Unlock()
}

func (vs *ViewServer) getIdleServer() string {
	// vs.muIdle.Lock()
	// defer vs.muIdle.Unlock()
	for k, _ := range vs.mapPingTrack {
		if k != vs.curView.Primary && k != vs.curView.Backup {
			return k
		}
	}
	return ""
}

func (vs *ViewServer) removeBackup() {
	//vs.mu.Lock()
	// if DEBUG {
	// 	fmt.Println("remove backup")
	// }
	if vs.bAcked {
		//vs.nView++
		vs.viewChange = true
		vs.curView = View{vs.nView, vs.curView.Primary, ""}
	}
	//vs.mu.Unlock()
}

func (vs *ViewServer) removePrimary() {
	//vs.mu.Lock()
	if vs.bAcked {
		//vs.nView++
		vs.viewChange = true
		vs.curView = View{vs.nView, "", ""}
	}
	//vs.mu.Unlock()
}

// func (vs *ViewServer) removeIdle() {
// 	l := list.New()
// 	for k, _ := range vs.idleServer {
// 		t, ok := vs.mapPingTrack[k]
// 		if ok && time.Now().Sub(t) > DeadPings*PingInterval {
// 			l.PushBack(k)
// 		}
// 	}
// 	for e := l.Front(); e != nil; e = e.Next() {
// 		delete(vs.idleServer, e.Value.(string))
// 	}

// }

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	// Your code here.
	// no ping from both servers
	vs.mu.Lock()
	// remove idle which is out of date
	//vs.removeIdle()
	// either server crashed
	vs.viewChange = false
	if vs.bAcked {
		for k, t := range vs.mapPingTrack {
			if time.Now().Sub(t) > DeadPings*PingInterval {
				delete(vs.mapPingTrack, k)
				if k == vs.curView.Primary {
					vs.curView.Primary = ""
					vs.promoteToPrimary()
				} else if k == vs.curView.Backup {
					vs.curView.Backup = ""
					vs.promoteToBackup()
				}
			}
		}

		// no backup and there is idle
		if vs.curView.Primary != "" && vs.curView.Backup == "" {
			vs.promoteToBackup()
		}
		if vs.curView.Primary == "" && vs.curView.Backup != "" {
			vs.promoteToPrimary()
		}
		//vs.muIdle.Lock()
		// remove duplicate idle servers if they are still in use

		if vs.viewChange {
			vs.nView++
			vs.curView = View{vs.nView, vs.curView.Primary, vs.curView.Backup}
		}
		vs.print()
		if DEBUG {
			fmt.Println("\nend of tick")
		}
	}

	//vs.muIdle.Unlock()
	vs.mu.Unlock()
}

//
// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
//
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

//
// has this server been asked to shut down?
//
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.mapPingTrack = make(map[string]time.Time)
	vs.bAcked = false
	vs.nView = 0
	vs.curView = View{0, "", ""}
	//vs.idleServer = make(map[string]int)
	vs.viewChange = false
	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
