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
	"context"
	"labrpc"
	"log"
	"math/rand"
	"sync"
	"time"
)

func init() {
	log.SetFlags(log.Lshortfile | log.LstdFlags)
}

const (
	HeartbeatInterval = time.Millisecond * 50
	ElectionTimeout   = 10 * HeartbeatInterval
)

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

//
// A Go object implementing a single Raft peer.
//

type RaftState int

const (
	StateFollower  RaftState = iota + 1000
	StateCandidate RaftState = iota + 1000
	StateLeader    RaftState = iota + 1000
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	term          int // term
	state         RaftState
	votedFor      int
	lastHeartBeat time.Time
}

func genElectionTimeout() time.Duration {
	return ElectionTimeout*time.Duration(rand.Intn(200))/100 + ElectionTimeout
}

func getHeartbeatInterval() time.Duration {
	return HeartbeatInterval
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	isLeader := (rf.state == StateLeader)
	return rf.term, isLeader
}

func (rf *Raft) getState() RaftState {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state

}

func (rf *Raft) transitionState(newState RaftState) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = newState
}

func (rf *Raft) getLastHeartbeat() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartBeat
}

func (rf *Raft) setLastHeartbeat(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = t
}

func (rf *Raft) incrTerm() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term++
	rf.votedFor = -1
}

func (rf *Raft) setTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.term = term
	rf.votedFor = -1
}

func (rf *Raft) getTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.term
}

func (rf *Raft) voteFor(peer int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = peer
}

func (rf *Raft) getVotedFor() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) getMe() int {
	return rf.me
}

func (rf *Raft) getPeers() []*labrpc.ClientEnd {
	return rf.peers
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
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
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	term := rf.getTerm()
	reply.Term = term
	if args == nil {
		reply.VoteGranted = false
		return
	}

	if args.Term < term {
		reply.VoteGranted = false
		return
	} else if args.Term > term {
		rf.setTerm(args.Term)
		rf.transitionState(StateFollower)
	}

	votedFor := rf.getVotedFor()
	rf.setLastHeartbeat(time.Now())

	if votedFor == args.CandidateID || votedFor == -1 {
		reply.VoteGranted = true
		rf.voteFor(args.CandidateID)
		return
	}
	reply.VoteGranted = false
	return
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
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	//log.Printf("RequestVote, %d -> %d, reply: %v, ok: %v\n", rf.me, server, reply.VoteGranted, ok)
	return ok
}

type AppendEntriesArgs struct {
	Term     int
	LeaderID int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.getTerm()
	reply.Term = term
	if args == nil {
		reply.Success = true
	}

	if args.Term < term {
		reply.Success = false
		return
	} else if args.Term > term {
		rf.setTerm(args.Term)
		rf.transitionState(StateFollower)
	}

	rf.setLastHeartbeat(time.Now())

	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	log.Printf("AppendEntries, %d -> %d, reply: %v, ok: %v\n", rf.me, server, reply.Success, ok)
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

	// Your code here (2B).

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

func (rf *Raft) followerLoop() {
	if rf.getState() != StateFollower {
		return
	}
	log.Printf("server %d in follower loop, term: %d", rf.getMe(), rf.getTerm())
	timeout := genElectionTimeout()
	tick := time.Tick(timeout)
	for range tick {
		if rf.getState() != StateFollower {
			return
		}
		lastHB := rf.getLastHeartbeat()
		if time.Since(lastHB) > timeout {
			log.Println("lastHB: ", lastHB)
			log.Println("cost: ", time.Since(lastHB))
			rf.transitionState(StateCandidate)
			return
		}
	}
}

func (rf *Raft) candidateLoop() {
	if rf.getState() != StateCandidate {
		return
	}
	me := rf.getMe()
	peerCnt := len(rf.getPeers())
	timeout := genElectionTimeout()
	for {
		if rf.getState() != StateCandidate {
			return
		}
		rf.incrTerm()
		log.Printf("server %d in candidate, term: %d", me, rf.getTerm())
		var wg sync.WaitGroup

		ch := make(chan *RequestVoteReply, peerCnt)
		wg.Add(peerCnt - 1)
		term := rf.getTerm()

		for i := 0; i < peerCnt; i++ {
			if me == i {
				continue
			}
			go func(server int) {
				args := &RequestVoteArgs{
					Term:        term,
					CandidateID: me,
				}
				reply := &RequestVoteReply{}
				if !rf.sendRequestVote(server, args, reply) {
					reply.VoteGranted = false
					reply.Term = term
					log.Printf("server %d not reply RequestVote request from %d\n", server, me)
				}
				ch <- reply
				wg.Done()
			}(i)
		}

		wg.Wait()
		votesWin := 1
		for i := 0; i < peerCnt-1; i++ {
			reply := <-ch
			if reply.VoteGranted == true {
				votesWin++
			} else if reply.Term > term {
				rf.setTerm(reply.Term)
				rf.transitionState(StateFollower)
				return
			}
		}

		if votesWin > peerCnt/2 {
			log.Printf("server %d get %d votes\n", me, votesWin)
			rf.transitionState(StateLeader)
			return
		}
		time.Sleep(timeout)
	}
}

func (rf *Raft) leaderLoop() {
	if rf.getState() != StateLeader {
		return
	}
	me := rf.getMe()
	term := rf.getTerm()
	interval := getHeartbeatInterval()
	peerCnt := len(rf.getPeers())
	ctx, cancel := context.WithCancel(context.Background())
	log.Printf("server %d in leader loop, term: %d", me, term)
	for i := 0; i < peerCnt; i++ {
		if i == me {
			continue
		}
		go func(ctx context.Context, server int) {
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(interval):
					args := &AppendEntriesArgs{
						Term:     term,
						LeaderID: me,
					}
					reply := &AppendEntriesReply{}
					if !rf.sendAppendEntries(server, args, reply) {
						log.Printf("server %d not response Heartbeat from %d\n", server, me)
						continue
					}
					if !reply.Success {
						rf.setTerm(reply.Term)
						rf.transitionState(StateFollower)
					}
				}
			}
		}(ctx, i)
	}
	for range time.Tick(interval) {
		if rf.getState() != StateLeader {
			cancel()
			return
		}
	}
}

func (rf *Raft) Loop() {
	for {
		rf.leaderLoop()
		rf.candidateLoop()
		rf.followerLoop()
	}
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

	// Your initialization code here (2A, 2B, 2C).

	rf.term = 0
	rf.state = StateFollower
	rf.votedFor = -1
	rf.lastHeartBeat = time.Now()

	go rf.Loop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
