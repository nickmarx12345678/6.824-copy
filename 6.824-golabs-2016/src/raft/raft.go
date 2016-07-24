package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

func debug() bool {
	return false
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh       chan ApplyMsg
	currentTerm   int
	votedFor      bool
	log           []*ApplyMsg
	commitedIndex int
	lastApplied   int
	isLeader      bool
	nextIndex     []int
	matchIndex    []int
	// DEBUG
	receivedHeartbeat bool
}

type AppendEntries struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	ApplyMsg     *ApplyMsg
	Entries      []*ApplyMsg
	LeaderCommit []int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
	// DEBUG
	FollowerID int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	reply.Term = rf.currentTerm
	if args.Term > rf.currentTerm {
		rf.increaseTerm(args.Term)
		rf.isLeader = false
	}
	reply.FollowerID = rf.me
	if !rf.votedFor {
		reply.VoteGranted = true
		rf.votedFor = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) ProcessAppendEntries(args AppendEntries, reply *AppendEntriesReply) {
	//fmt.Println("Received heartbeat", args.LeaderID, "to", rf.me)
	rf.receivedHeartbeat = true
	reply.Term = args.Term
	reply.Success = true
	if args.ApplyMsg.Index > -1 {
		applyMsg := *args.ApplyMsg
		go func() {
			rf.log = append(rf.log, &applyMsg)
			rf.applyCh <- applyMsg
		}()
		//fmt.Println("Server", rf.me, "accepted cmd", (applyMsg.Command).(int), "at index", len(rf.log))
	}
	if args.Term > rf.currentTerm {
		rf.increaseTerm(args.Term)
		rf.isLeader = false
	}
	if args.Term >= rf.currentTerm {
		rf.isLeader = false
	}
}

func (rf *Raft) sendAppendEntries(
	server int,
	args AppendEntries,
	reply *AppendEntriesReply) bool {
	//fmt.Println(args)
	ok := rf.peers[server].Call("Raft.ProcessAppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true
	if !rf.isLeader {
		return -1, -1, false
	}
	index = len(rf.log) + 1
	term = rf.currentTerm
	applyMsg := ApplyMsg{
		Index:   index,
		Command: command,
	}
	//fmt.Println("Leader", rf.me, "accepted cmd", (applyMsg.Command).(int), "at index", len(rf.log))
	go func() {
		rf.log = append(rf.log, &applyMsg)
		rf.applyCh <- applyMsg
	}()
	//rf.log[len(rf.log)] = &applyMsg
	go rf.sendHeartbeat(&applyMsg)
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	// Your initialization code here.
	// Initialize timer for recognizing heartbeats.
	go rf.listenForHeartbeat()
	// Handle heartbeat by processing AppendEntries from leader.
	// If no AppendEntries received, start election.
	// Call sendRequestVote() to each of the other peers.
	// If a majority of other peers respond with confirmation of vote, then is leader.
	// If leader, update the current term and start issuing AppendEntry heartbeat.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) increaseTerm(term int) {
	rf.currentTerm = term
	rf.votedFor = false
}

func (rf *Raft) changeToFollower() {
	if debug() {
		fmt.Println("Changing to follower", rf.me)
	}
	rf.isLeader = false
}

func (rf *Raft) changeToLeader() {
	if debug() {
		fmt.Println("Changing to leader", rf.me)
	}
	rf.isLeader = true
}

func (rf *Raft) listenForHeartbeat() {
	rf.receivedHeartbeat = false
	timeout := time.Duration(rand.Intn(100) + 100)
	timeout = time.Duration(100)
	time.Sleep(timeout * time.Millisecond)
	if !rf.isLeader && !rf.receivedHeartbeat {
		timeout = time.Duration(rand.Intn(200) + 150)
		time.Sleep(timeout * time.Millisecond)
		if debug() {
			fmt.Println("Leader election", rf.me)
		}
		go rf.startElection()
	}
	go rf.listenForHeartbeat()
}

func (rf *Raft) sendHeartbeat(applyMsg *ApplyMsg) {
	if !rf.isLeader {
		return
	}
	appendEntries := AppendEntries{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		ApplyMsg:     applyMsg,
	}
	reply := AppendEntriesReply{}
	for x := 0; x < len(rf.peers); x++ {
		if x == rf.me {
			continue
		}
		//fmt.Println("Sending heartbeat", rf.me, "to", x)
		rf.sendAppendEntries(x, appendEntries, &reply)
	}
	time.Sleep(50 * time.Millisecond)
	go rf.sendHeartbeat(&ApplyMsg{Index: -1})
}

func (rf *Raft) startElection() {
	rf.increaseTerm(rf.currentTerm + 1)
	if debug() {
		fmt.Println("Server", rf.me, "moved to term", rf.currentTerm)
	}
	rf.votedFor = true
	numVotes := 1
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: 0,
		LastLogTerm:  0,
	}
	reply := RequestVoteReply{}
	for x := 0; x < len(rf.peers); x++ {
		if x == rf.me {
			continue
		}
		rf.sendRequestVote(x, args, &reply)
		if reply.Term >= rf.currentTerm {
			if debug() {
				fmt.Println("Candidate", rf.me, "standing down from", reply.FollowerID)
			}
			rf.isLeader = false
			go rf.listenForHeartbeat()
			return
		}
		if reply.VoteGranted {
			numVotes++
			if debug() {
				fmt.Println("Vote granted to", rf.me, "from", reply.FollowerID)
			}
		}
	}
	if numVotes > len(rf.peers)/2 {
		rf.isLeader = true
		if debug() {
			fmt.Println("Server", rf.me, "elected as leader.")
		}
		go rf.sendHeartbeat(&ApplyMsg{Index: -1})
	} else {
		rf.isLeader = false
		go rf.listenForHeartbeat()
	}
}
