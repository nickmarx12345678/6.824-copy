package raft

import (
	"fmt"
	"labrpc"
	"sync"
	"time"
)

type Peer struct {
	Rpc      *labrpc.ClientEnd
	stopChan chan bool
	raft     *Raft
	me       int

	lastActivity      time.Time
	heartBeatInterval time.Duration
	mu                sync.Mutex
}

func (peer *Peer) getLastActivity() time.Time {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	return peer.lastActivity
}

func (peer *Peer) setLastActivity(t time.Time) {
	peer.mu.Lock()
	defer peer.mu.Unlock()
	peer.lastActivity = t
}

func (peer *Peer) startHeartBeat() {
	peer.stopChan = make(chan bool)
	// c := make(chan bool) TODO: use this in heartBeat(c), and understand why...probably to make sure stop chan doesn't change

	peer.setLastActivity(time.Now())

	go func() {
		peer.heartBeat()
	}()
	// <-c TODO
}

func (peer *Peer) stopHeartBeat() {
	peer.stopChan <- true
}

func (peer *Peer) heartBeat() {
	//c <- true TODO

	ticker := time.Tick(peer.heartBeatInterval)

	for {
		select {
		case <-peer.stopChan:
			fmt.Println("stop heart beat from stopChan")
			return
		case <-ticker:
			peer.raft.sendHeartBeat2(peer.me)
		}
	}
}
