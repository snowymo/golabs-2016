package diskv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
	ErrNoSnap     = "ErrNoSnap"
	NOK           = "NOK"
	ErrDupR       = "ErrDupReCfg"
)

type Err string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id    int64 // unique id
	Shard int
	Srv   string
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id    int64 // unique id
	Shard int
	Srv   string
}

type GetReply struct {
	Err   Err
	Value string
}

type UpdateArgs struct {
	SnapNo int
	Id     int64 // unique id
}

type UpdateReply struct {
	//Uidmap map[int64]int //uidmap = make(map[int64]bool)
	UidWsh map[int]map[int64]int
	DB     map[string]string
	BReCfg map[int]bool
	//U2S    map[int64]int
	Disk map[int]map[string]string
	Err  Err
}

type LoadDiskArgs struct {
	UidWsh    map[int]map[int64]int
	DB        map[string]string
	BReCfg    map[int]bool
	LastLogId int
	Me        int
	Disk      map[int]map[string]string
	Id        int64 // unique id
}

type LoadDiskReply struct {
	//Uidmap map[int64]int //uidmap = make(map[int64]bool)
	UidWsh    map[int]map[int64]int
	DB        map[string]string
	BReCfg    map[int]bool
	LogCache  map[int]Op
	LastLogId int
	Disk      map[int]map[string]string
	Err       Err
}

type SaveDiskArgs struct {
	Logentry Op
	Value    string
	Logidx   int
	Me       int
	Id       int64 // unique id
}

type SaveDiskReply struct {
	LastLogId int
	Err       Err
}

type ReplaceDiskArgs struct {
	LastLogId int
	Disk      map[int]map[string]string
	Me        int
	Id        int64 // unique id
}

type ReplaceDiskReply struct {
	LastLogId int
	Err       Err
}

const MAXUID = 4294967296

const LOGSHARD = 11
const UIDSHARD = 12
const LOGFILE = "logidx"
const PAXOSSHARD = 22
const RECFG = 10
const RECFGFILE = "bRecfg"
