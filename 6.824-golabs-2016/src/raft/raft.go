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

var States = struct {
	Follower  string
	Candidate string
	Leader    string
}{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
}

// import "bytes"
// import "encoding/gob"

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

type LogEntry struct {
	Term    int
	Command interface{}
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
	currentTerm int
	votedFor    int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	heartBeatInterval time.Duration
	electionTimeout   time.Duration
	heartBeatTimeout  time.Duration
	lastHeartBeat     time.Time
	lastRequestVote   time.Time
	state             string

	//TODO: do this better
	lastLeaderHeartBeatSent time.Time
	leaderHeartBeatInterval time.Duration

	sigKill bool
}

func (rf *Raft) State() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) SetState(state string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//TODO: assert state one of three valid options
	rf.state = state
}

func (rf *Raft) HeartBeatInterval() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.heartBeatInterval
}

func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.electionTimeout
}

func (rf *Raft) MatchIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex
}

func (rf *Raft) NextIndex() []int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex
}

func (rf *Raft) LastApplied() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastApplied
}

func (rf *Raft) CommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) Log() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.log
}

func (rf *Raft) VotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//TODO: assert peer valid
	rf.votedFor = peer
}

func (rf *Raft) CurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//TODO: assert non-decreasing
	rf.currentTerm = term
}

func (rf *Raft) Me() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.me
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.peers
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var isleader bool

	isleader = (rf.State() == States.Leader)
	return rf.CurrentTerm(), isleader
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int

	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {

	if args.Term < rf.CurrentTerm() {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm()
		return
	}

	rf.SetCurrentTerm(args.Term)

	if rf.VotedFor() == -1 {
		reply.VoteGranted = true
		reply.Term = args.Term //todo should this be +1?

		rf.SetVotedFor(args.CandidateId)
	}
	return
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Printf("server %v received appendEntries from server %v stating term %v\n", rf.me, args.LeaderId, args.Term)
	//reset heartBeatTimeout2
	rf.mu.Lock()
	rf.lastHeartBeat = time.Now()
	rf.mu.Unlock()

	rf.SetVotedFor(-1) //TODO: is this the right place to clear this

	if args.Term < rf.CurrentTerm() {
		fmt.Printf("append entries success == false, server %v with current term %v higher than term %v sent from server %v\n", rf.Me(), rf.CurrentTerm(), args.Term, args.LeaderId)
		reply.Success = false
		reply.Term = rf.CurrentTerm() //todo when do we set this to me.currentTerm?
		return
	}
	//another leader has been elected, follow it
	rf.SetState(States.Follower)
	rf.SetCurrentTerm(args.Term)

	return

	// if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
	// 	reply.Success = false
	// 	// reply.Term = rf.currentTerm //todo ???
	// 	return
	// }
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

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
	fmt.Println("#Start: command", command)

	index := -1
	term := -1
	_, isLeader := rf.GetState()

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	//currently just kills heartbeat process
	rf.mu.Lock()
	rf.sigKill = true
	rf.mu.Unlock()
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
	rf.commitIndex = 0
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.log = make([]LogEntry, 0)
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.votedFor = -1 //start as nil equivalent

	rf.heartBeatTimeout = time.Duration(random(150, 500)) * time.Millisecond
	rf.lastHeartBeat = time.Now()

	rf.electionTimeout = time.Duration(random(150, 300)) * time.Millisecond //TODO: does this need to be random? if so what range
	rf.state = States.Follower
	rf.heartBeatInterval = time.Duration(15) * time.Millisecond

	//start heartbeat monitor
	go func() {
		rf.startHeartBeatTimeout()
	}()

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) startHeartBeatTimeout() {
	fmt.Println("startHeartBeatTimeout")
	for {
		rf.mu.Lock()
		kill := rf.sigKill
		rf.mu.Unlock()
		if kill {
			fmt.Printf("recieved sigKill for server %v, killing heartbeat loop\n", rf.Me())
			break
		}
		//todo should sleep this so it's not going crazy in the background
		//todo need to timeout the election process as well
		rf.mu.Lock()
		heartBeatTimeoutTime := rf.lastHeartBeat.Add(rf.heartBeatTimeout)

		//leader must send heartbeat to followers every X amount of time
		sendHeartBeat := rf.state == States.Leader && time.Now().After(rf.lastLeaderHeartBeatSent.Add(rf.heartBeatInterval))
		rf.mu.Unlock()

		if sendHeartBeat {
			rf.sendHeartBeat()
			continue
		}

		//TODO second half relies on short circuiting, should be better
		heartBeatTimedOut := (rf.State() == States.Follower && time.Now().After(heartBeatTimeoutTime)) || (rf.State() == States.Candidate && time.Now().After(rf.getRequestVoteTimeout()))
		if heartBeatTimedOut {

			if rf.State() == States.Follower && time.Now().After(heartBeatTimeoutTime) {
				fmt.Printf("heartbeat timed out for server %v\n", rf.Me())
			}

			if rf.state == States.Candidate && time.Now().After(rf.getRequestVoteTimeout()) {
				fmt.Printf("requestVote timed out for server %v\n", rf.Me())
			}

			//increment term and transition to candidate state per 5.2
			rf.SetCurrentTerm(rf.CurrentTerm() + 1)
			rf.SetState(States.Candidate)
			//initiate election
			// lastLogIndex := len(rf.log) - 1 //todo what if this is empty?
			// lastLog := rf.log[lastLogIndex]

			//TODO should these be constant for each request or should we recheck pre each-rpc, probably former
			requestVoteArgs := RequestVoteArgs{
				Term:        rf.CurrentTerm(),
				CandidateId: rf.Me(),
				// LastLogIndex: lastLogIndex,
				LastLogTerm: rf.CurrentTerm(), //todo is this the right value?//todo should be lastLog.term
			}

			rf.mu.Lock()
			//start timeout for requestvote
			rf.lastRequestVote = time.Now()
			rf.mu.Unlock()

			fmt.Printf("server %v initiating request vote\n", rf.Me())

			voteCount := 0
			for serverIndex := 0; serverIndex < len(rf.Peers()); serverIndex++ {
				requestVoteReply := &RequestVoteReply{}
				if serverIndex != rf.Me() {
					//todo should these be in parallel? probably, and probably need to cancel this loop and stuff if vote threshold is hit, or maybe check at timeout?
					ok := rf.sendRequestVote(serverIndex, requestVoteArgs, requestVoteReply)
					if !ok {
						fmt.Printf("not ok sending request vote to server %v from server %v, appears to be down\n", serverIndex, rf.me)
					}

					//TODO: should ensure late rpc's dont mess with state once leader is already decided
					if requestVoteReply.VoteGranted {
						voteCount += 1
						fmt.Printf("server %v received vote, vote count: %v\n", rf.Me(), voteCount)
					}
					if voteCount > len(rf.peers)/2 {
						fmt.Printf("server %v elected leader\n", rf.Me())
						fmt.Printf("server %v setting new term to %v\n", rf.Me(), rf.CurrentTerm())
						rf.SetState(States.Leader)
						rf.sendHeartBeat()
					}
				}
			}
		}
	}
}

func (rf *Raft) getRequestVoteTimeout() time.Time {
	return rf.lastRequestVote.Add(rf.ElectionTimeout())
}

func (rf *Raft) sendHeartBeat() {
	fmt.Printf("sending heart beat from server %v\n", rf.Me())
	//send empty hearthbeat to all other servers to indicate leader has been elected
	appendEntriesArgs := AppendEntriesArgs{
		Term:     rf.CurrentTerm(),
		LeaderId: rf.Me(),
	}
	rf.mu.Lock()
	rf.lastLeaderHeartBeatSent = time.Now()
	rf.mu.Unlock()
	for rvServerIndex := 0; rvServerIndex < len(rf.Peers()); rvServerIndex++ {
		if rvServerIndex != rf.Me() {
			appendEntriesReply := &AppendEntriesReply{}
			rf.sendAppendEntries(rvServerIndex, appendEntriesArgs, appendEntriesReply)
		}
	}
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}
