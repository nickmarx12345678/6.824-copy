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
	Stopped   string
}{
	Follower:  "Follower",
	Candidate: "Candidate",
	Leader:    "Leader",
	Stopped:   "Stopped",
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
	peers     []*Peer
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    int
	leader      int
	log         []LogEntry
	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	heartBeatInterval time.Duration
	electionTimeout   time.Duration
	heartBeatTimeout  time.Duration
	state             string

	//TODO: do this better
	lastLeaderHeartBeatSent time.Time
	leaderHeartBeatInterval time.Duration

	eventChan   chan ev
	stoppedChan chan bool
}

// An internal event to be processed by the server's event loop.
type ev struct {
	target      interface{}
	returnValue interface{}
	c           chan interface{}
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
	respChan := make(chan interface{})
	event := ev{
		target: args,
		c:      respChan,
	}
	fmt.Printf("server %v received request vote request from server %v\n", rf.me, args.CandidateId)
	rf.eventChan <- event

	back := <-event.c //response from event processing in a given state

	rep := back.(RequestVoteReply)

	reply.Term = rep.Term
	reply.VoteGranted = rep.VoteGranted

	return
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	respChan := make(chan interface{})
	event := ev{
		target: args,
		c:      respChan,
	}

	rf.eventChan <- event
	back := <-event.c //response from event processing
	rep := back.(AppendEntriesReply)

	reply.Success = rep.Success
	reply.Term = rep.Term

	return
}

func (rf *Raft) processAppendEntriesRequest(args AppendEntriesArgs) AppendEntriesReply {
	reply := AppendEntriesReply{
		Success: true,
		Term:    args.Term,
	}

	fmt.Printf("server %v received appendEntries from server %v stating term %v\n", rf.me, args.LeaderId, args.Term)
	//reset heartBeatTimeout2
	rf.peers[args.LeaderId].setLastActivity(time.Now())
	rf.SetVotedFor(-1) //TODO: is this the right place to clear this

	if args.Term < rf.CurrentTerm() {
		fmt.Printf("append entries success == false, server %v with current term %v higher than term %v sent from server %v\n", rf.Me(), rf.CurrentTerm(), args.Term, args.LeaderId)
		reply.Success = false
		reply.Term = rf.CurrentTerm() //todo when do we set this to me.currentTerm?
		return reply
	}

	//another leader has been elected, follow it
	rf.SetState(States.Follower)
	rf.SetCurrentTerm(args.Term)
	return reply
}

func (rf *Raft) processRequestVoteRequest(args RequestVoteArgs) RequestVoteReply {
	reply := RequestVoteReply{}

	if args.Term < rf.CurrentTerm() {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm()
		return reply
	}

	// If the term of the request peer is larger than this node, update the term
	// If the term is equal and we've already voted for a different candidate then
	// don't vote for this candidate.
	if args.Term > rf.CurrentTerm() {
		rf.SetCurrentTerm(args.Term)
	} else if rf.VotedFor() != -1 && rf.VotedFor() != args.CandidateId {
		reply.VoteGranted = false
		reply.Term = rf.CurrentTerm()
		return reply
	}

	reply.VoteGranted = true
	reply.Term = rf.CurrentTerm()
	rf.SetVotedFor(args.CandidateId)
	return reply
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
func (rf *Raft) sendRequestVote(server int, responseChan chan *RequestVoteReply) bool {
	fmt.Printf("server %v sending request vote to server %v with term %v\n", rf.me, server, rf.CurrentTerm())
	requestVoteArgs := RequestVoteArgs{
		Term:        rf.CurrentTerm(),
		CandidateId: rf.Me(),
		// LastLogIndex: lastLogIndex,
		LastLogTerm: rf.CurrentTerm(), //todo is this the right value?//todo should be lastLog.term
	}
	reply := &RequestVoteReply{}

	ok := rf.peers[server].Rpc.Call("Raft.RequestVote", requestVoteArgs, reply)
	if ok {
		responseChan <- reply
	}
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Rpc.Call("Raft.AppendEntries", args, reply)
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

func (rf *Raft) Kill() {
	fmt.Printf("recieved sig kill for server %v, in state %v\n", rf.me, rf.State())
	rf.stoppedChan <- true
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

	rf.heartBeatInterval = time.Duration(15) * time.Millisecond

	rf.peers = []*Peer{}
	for i, peer := range peers {
		p := &Peer{
			Rpc:               peer,
			mu:                sync.Mutex{},
			me:                i,
			raft:              rf,
			heartBeatInterval: rf.heartBeatInterval,
		}
		fmt.Printf("creating peer for raft id: %v with peer_id: %v\n", me, i)
		rf.peers = append(rf.peers, p)
	}

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

	rf.electionTimeout = time.Duration(random(150, 300)) * time.Millisecond //TODO: does this need to be random? if so what range
	rf.state = States.Follower

	rf.eventChan = make(chan ev)
	rf.stoppedChan = make(chan bool)

	//start heartbeat monitor
	go func() {
		rf.loop()
	}()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

// Waits for a random time between two durations and sends the current time on
// the returned channel.
func afterBetween(min time.Duration, max time.Duration) <-chan time.Time {
	rand := rand.New(rand.NewSource(time.Now().UnixNano()))
	d, delta := min, (max - min)
	if delta > 0 {
		d += time.Duration(rand.Int63n(int64(delta)))
	}
	return time.After(d)
}

func random(min, max int) int {
	rand.Seed(time.Now().Unix())
	return rand.Intn(max-min) + min
}

func (rf *Raft) loop() {
	defer fmt.Printf("server %v exiting main loop\n", rf.me)

	state := rf.State()

	for state != States.Stopped {
		switch state {
		case States.Follower:
			fmt.Printf("server %v entering follower loop\n", rf.me)
			rf.followerLoop()
		case States.Candidate:
			fmt.Printf("server %v entering candidate loop\n", rf.me)
			rf.candidateLoop()
		case States.Leader:
			fmt.Printf("server %v entering leader loop\n", rf.me)
			rf.leaderLoop()
		case "Snapshotting":
			//TODO
		}
		state = rf.State()
	}
}

func (rf *Raft) followerLoop() {
	electionTimeout := rf.ElectionTimeout()

	timeoutChan := afterBetween(electionTimeout, electionTimeout*2)

	for rf.State() == States.Follower {

		select {
		case <-rf.stoppedChan:
			rf.SetState(States.Stopped)
			fmt.Printf("server %v setting state to stopping in follower loop\n", rf.me)
			return
		case event := <-rf.eventChan:
			switch args := event.target.(type) {
			case RequestVoteArgs:
				event.c <- rf.processRequestVoteRequest(args)
			case AppendEntriesArgs:
				event.c <- rf.processAppendEntriesRequest(args)
			}
		case <-timeoutChan:
			rf.SetState(States.Candidate)
		}

		timeoutChan = afterBetween(electionTimeout, electionTimeout*2)
	}
}

func (rf *Raft) candidateLoop() {
	electionTimeout := rf.ElectionTimeout()

	var timeoutChan <-chan time.Time
	var responseChan chan *RequestVoteReply

	doVote := true
	voteCount := 0

	for rf.State() == States.Candidate {
		if doVote {
			timeoutChan = afterBetween(electionTimeout, electionTimeout*2) //TODO this is probably the wrong constant
			responseChan = make(chan *RequestVoteReply)

			rf.SetCurrentTerm(rf.CurrentTerm() + 1)
			rf.SetVotedFor(rf.me)

			//TODO: should wait group this
			for peerIndex, _ := range rf.Peers() {
				if peerIndex != rf.Me() {
					//TODO:  should these be in parallel? probably, and probably need to cancel this loop and stuff if vote threshold is hit, or maybe check at timeout?
					go func(index int) {
						rf.sendRequestVote(index, responseChan)
					}(peerIndex)
				}
			}
			voteCount = 1
			doVote = false
		}

		if voteCount > len(rf.peers)/2 {
			rf.SetState(States.Leader)
			return
		}

		select {
		case reply := <-responseChan:
			if reply.VoteGranted {
				fmt.Printf("server %v got vote\n", rf.me)
				voteCount++
				//TODO: handle updating terms and stuff
			}
		case <-rf.stoppedChan:
			rf.SetState(States.Stopped)
			fmt.Printf("server %v setting state to stopping in candidate loop\n", rf.me)
			return
		case event := <-rf.eventChan:
			switch args := event.target.(type) {
			case RequestVoteArgs:
				event.c <- rf.processRequestVoteRequest(args)
			case AppendEntriesArgs:
				event.c <- rf.processAppendEntriesRequest(args)
			}
		case <-timeoutChan:
			//timed out, start new election
			doVote = true
		}
	}
}

func (rf *Raft) leaderLoop() {

	//TODO: send initial empty append entries, leader has just been elected

	for _, peer := range rf.peers {
		//dont send self heart beat
		if peer.me != rf.me {
			fmt.Printf("server %v starting heartbeat for server %v\n", rf.Me(), peer.me)
			peer.startHeartBeat()
		}
	}

	for rf.State() == States.Leader {
		fmt.Printf("server %v in state == leader loop\n", rf.me)
		select {
		case <-rf.stoppedChan:
			fmt.Printf("server %v setting state to stopping in leader loop\n", rf.me)
			rf.SetState(States.Stopped)
			break
		case event := <-rf.eventChan:
			switch args := event.target.(type) {
			// case RequestVoteArgs:
			// 	event.c <- rf.processRequestVoteRequest(args)
			case AppendEntriesArgs:
				if rf.CurrentTerm() < args.Term {
					rf.SetState(States.Follower)
				}
				event.c <- rf.processAppendEntriesRequest(args)
			}
		}
	}
	//cleanup, stop all peer heartbeats, we're no longer leader
	for _, peer := range rf.peers {
		//dont send self heart beat
		if peer.me != rf.me {
			peer.stopHeartBeat()
		}
	}
	fmt.Printf("server %v completed state == leader loop\n", rf.me)
}

func (rf *Raft) sendHeartBeat(serverIndex int) {
	// fmt.Printf("sending heart beat from server %v to server %v\n", rf.Me(), serverIndex)
	//send empty hearthbeat to all other servers to indicate leader has been elected
	appendEntriesArgs := AppendEntriesArgs{
		Term:     rf.CurrentTerm(),
		LeaderId: rf.Me(),
	}

	appendEntriesReply := &AppendEntriesReply{}
	rf.sendAppendEntries(serverIndex, appendEntriesArgs, appendEntriesReply)

	// //TODO is this the right place for this?
	// if appendEntriesReply.Term > rf.CurrentTerm() {
	// 	rf.SetCurrentTerm(appendEntriesReply.Term)
	// 	rf.SetState(States.Follower)
	// }
}

func (rf *Raft) Leader() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leader
}

func (rf *Raft) SetLeader(leader int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.leader = leader
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

func (rf *Raft) Peers() []*Peer {
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
