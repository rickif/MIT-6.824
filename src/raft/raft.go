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
	"bytes"
	"context"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

const (
	HeartbeatInterval    = time.Millisecond * 100
	HeartbeatTimeout     = time.Millisecond * 100
	ElectionTimeoutBase  = time.Millisecond * 300
	AppendEntriesTimeout = time.Millisecond * 300

	ReplyChanSize = 16
)

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

type RaftStatus int

const (
	RaftStatus_Follower  RaftStatus = iota + 1
	RaftStatus_Candidate RaftStatus = iota + 1
	RaftStatus_Leader    RaftStatus = iota + 1
)

type LogEntry struct {
	Index int
	Term  int
	Cmd   interface{}
}

type Raft struct {
	// PersistentState
	currentTerm int
	votedFor    int
	logs        []LogEntry
	state       interface{}

	//VolatileState
	commitIndex int
	lastApplied int
	lastIndex   int
	applyCh     chan ApplyMsg
	appendEvent chan struct{}

	//LeaderState
	nextIndex  map[int]int
	matchIndex map[int]int

	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	status RaftStatus

	electionDeadline time.Time
	electionTimeout  time.Duration

	cancel context.CancelFunc
	closed chan struct{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (2A).
	return rf.Term(), rf.IsLeader()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//

type PersistState struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Logs              []LogEntry
	State             interface{}
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)

	lastAppliedIndex := rf.lastApplied
	lastAppliedLog, _ := rf.log(lastAppliedIndex)
	lastAppliedTerm := lastAppliedLog.Term
	logs := rf.logAfter(lastAppliedIndex + 1)

	state := PersistState{
		LastIncludedIndex: lastAppliedIndex,
		LastIncludedTerm:  lastAppliedTerm,
		Logs:              logs,
		State:             lastAppliedLog.Cmd,
	}

	enc.Encode(&state)
	DPrintf("%v new state: %v", rf.Me(), state)

	data := buf.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here (2C).
	if data == nil || len(data) < 1 { // bootstrap without any state?
		DPrintf("%v no persist data", rf.Me())
		return
	}

	state := PersistState{}
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	dec.Decode(&state)
	DPrintf("%v load state: %v", rf.Me(), state)

	var lastLog LogEntry
	if len(state.Logs) > 0 {
		rf.logs = state.Logs
		lastLog = rf.lastLog()
		rf.state = state.State
	} else {
		lastLog = LogEntry{
			Index: state.LastIncludedIndex,
			Term:  state.LastIncludedTerm,
		}
		rf.logs = []LogEntry{lastLog}
	}

	rf.currentTerm = lastLog.Term
	rf.commitIndex = state.LastIncludedIndex
	rf.lastApplied = state.LastIncludedIndex

	DPrintf("%v load persiter state, term: %v, commiteIndex: %v, logs: %v", rf.me, rf.currentTerm, rf.commitIndex, rf.logs)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
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
/*
	1. 如果term < currentTerm返回 false （5.2 节）
    2. 如果 votedFor 为空或者为 candidateId，并且候选人的日志至少和自己一样新，那么就投票给他（5.2 节，5.4 节）
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	term := rf.currentTerm
	reply.Term = term
	if args.Term < term {
		return
	} else if args.Term > term || rf.isCandidate() {
		if !rf.isFollower() {
			rf.becomeFollower(args.Term, -1)
			rf.cancel()
		}
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateID && args.Term == term {
		DPrintf("%d request vote, term: %d\n", args.CandidateID, args.Term)
		DPrintf("%d has voted for %d, term: %d\n", rf.me, rf.votedFor, rf.currentTerm)
		return
	}

	//index较大的，日志更新；同样index下，最后一条term较大的，日志更新；同样index和同样最后一条term情况下，日志更长的更新
	lastLog := rf.lastLog()
	if args.LastLogTerm < lastLog.Term {
		return
	} else if args.LastLogTerm == lastLog.Term {
		if args.LastLogIndex < lastLog.Index {
			return
		}
	}

	rf.resetElectionDeadline()
	rf.becomeFollower(args.Term, args.CandidateID)
	rf.cancel()
	reply.VoteGranted = true
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
func (rf *Raft) sendRequestVote(ctx context.Context, server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ch := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		ch <- ok
	}()
	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	}
}

//
// example AppendEntries RPC arguments structure.
// field names must start with capital letters!
//
type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

//
// example AppendEntries RPC reply structure.
// field names must start with capital letters!
//
type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}

//
// example AppendEntries RPC handler.
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	reply.Term = term
	if args.Term < term {
		reply.Success = false
		return
	} else if args.Term > term {
		// remote term is bigger than local
		rf.becomeFollower(args.Term, -1)
		if !rf.isFollower() {
			rf.cancel()
		}
	}
	rf.resetElectionDeadline()

	prevLog, ok := rf.log(args.PrevLogIndex)
	if !ok {
		reply.Success = false
		return
	}

	if prevLog.Term != args.PrevLogTerm {
		reply.Success = false
		return
	}

	// truncate log when the pervious log'term and index is matched
	rf.maybeTruncateLog(args.PrevLogIndex)

	if len(args.Entries) > 0 {
		rf.followerAppendLogEntries(args.Entries)
	}

	rf.maybeFollowerCommit(args.LeaderCommit)
	rf.maybeApplyEntry()
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(ctx context.Context, server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ch := make(chan bool)
	go func() {
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		ch <- ok
	}()
	select {
	case ok := <-ch:
		return ok
	case <-ctx.Done():
		return false
	}
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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.isLeader()
	if !isLeader {
		return -1, term, isLeader
	}

	entry := LogEntry{
		Term: term,
		Cmd:  command,
	}

	lastLogID := rf.leaderAppendLogEntry(entry)
	DPrintf("%d append entry, term: %d , cmd: %v, will be commited :%v, logs: %v", rf.me, rf.currentTerm, command, lastLogID, rf.logs)

	return lastLogID, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	close(rf.closed)
	rf.cancel()
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

func (rf *Raft) newAppend() {
	close(rf.appendEvent)
	rf.appendEvent = make(chan struct{})
}

func (rf *Raft) NewAppend() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.newAppend()
}

func (rf *Raft) newAppended() <-chan struct{} {
	return rf.appendEvent
}

func (rf *Raft) NewAppended() <-chan struct{} {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.newAppended()
}

func (rf *Raft) ElectionTimeout() time.Duration {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionTimeout
}

func (rf *Raft) ResetElectionTimedout() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rand.Seed(time.Now().UnixNano())
	r := rand.Intn(300)
	rf.electionTimeout = ElectionTimeoutBase + time.Duration(r)*time.Millisecond
}

func (rf *Raft) ResetElectionDeadline() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.resetElectionDeadline()
}

func (rf *Raft) resetElectionDeadline() {
	rf.electionDeadline = time.Now().Add(rf.electionTimeout)
}

func (rf *Raft) ElectionDeadline() time.Time {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.electionDeadline
}

func (rf *Raft) Wait4ElectionTimeout(ctx context.Context) bool {
	ticker := time.NewTicker(rf.ElectionTimeout())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false
		case <-rf.closed:
			return false
		case <-ticker.C:
			if rf.ElectionDeadline().Before(time.Now()) {
				// election timeout fire
				return true
			}
		}
	}
}

func (rf *Raft) CommitIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) SetCommitIndex(ci int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = ci
}

func (rf *Raft) maybeFollowerCommit(leaderCommit int) {
	if rf.commitIndex < leaderCommit {
		// leaderCommit is bigger than commitIndex,
		// the local commitIndex equal
		lastLog := rf.lastLog()
		if leaderCommit > lastLog.Index {
			rf.commitIndex = lastLog.Index
			DPrintf("follower %v new commit index: %v", rf.me, rf.commitIndex)
		} else {
			rf.commitIndex = leaderCommit
			DPrintf("follower %v new commit index: %v", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) MaybeApplyEntry() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.maybeApplyEntry()
}

func (rf *Raft) maybeApplyEntry() {
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied++
		entry, ok := rf.log(rf.lastApplied)
		if !ok {
			return
		}
		applyMsg := ApplyMsg{
			Index:   rf.lastApplied,
			Command: entry.Cmd,
		}

		rf.applyCh <- applyMsg
		rf.state = entry.Cmd
		rf.persist()
		DPrintf("%v new applied %v, logs: %v", rf.me, rf.lastApplied, rf.logs)
	}
}

func (rf *Raft) Me() int {
	return rf.me
}

func (rf *Raft) term() int {
	return rf.currentTerm
}

func (rf *Raft) Term() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.term()
}

func (rf *Raft) setTerm(term int) {
	rf.currentTerm = term
}

func (rf *Raft) SetTerm(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.setTerm(term)
}

func (rf *Raft) LastLog() LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.lastLog()
}

func (rf *Raft) lastLog() LogEntry {
	if len(rf.logs) == 0 {
		return LogEntry{
			Index: 0,
			Term:  0,
		}
	}
	return rf.logs[len(rf.logs)-1]
}

func (rf *Raft) MaybeBecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status == RaftStatus_Candidate {
		rf.status = RaftStatus_Leader
	} else {
		rf.status = RaftStatus_Follower
	}
}

func (rf *Raft) BecomeLeader() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeLeader()
}

func (rf *Raft) becomeLeader() {
	rf.status = RaftStatus_Leader
}

func (rf *Raft) BecomeCandidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeCandidate()
}

func (rf *Raft) becomeCandidate() {
	rf.status = RaftStatus_Candidate
}

func (rf *Raft) BecomeFollower(term, leaderID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.becomeFollower(term, leaderID)
}

func (rf *Raft) becomeFollower(term, leaderID int) {
	if term != -1 {
		rf.currentTerm = term
	}
	rf.votedFor = leaderID
	rf.status = RaftStatus_Follower
}

func (rf *Raft) Peers() []*labrpc.ClientEnd {
	return rf.peers
}

func (rf *Raft) VotedFor() int {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.votedFor
}
func (rf *Raft) VoteFor(candidateID int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = candidateID
}

func (rf *Raft) IsLeader() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.status == RaftStatus_Leader
}

func (rf *Raft) IsFollower() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isFollower()
}

func (rf *Raft) isFollower() bool {
	return rf.status == RaftStatus_Follower
}

func (rf *Raft) IsCandidate() bool {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.isCandidate()
}

func (rf *Raft) isCandidate() bool {
	return rf.status == RaftStatus_Candidate
}

func (rf *Raft) FollowerAppendLogEntries(entries []LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.followerAppendLogEntries(entries)
}

func (rf *Raft) followerAppendLogEntries(entries []LogEntry) int {
	rf.logs = append(rf.logs, entries...)

	rf.newAppend()

	lastLog := rf.lastLog()
	rf.lastIndex = lastLog.Index
	return rf.lastIndex
}

func (rf *Raft) LeaderAppendLogEntry(entry LogEntry) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.leaderAppendLogEntry(entry)
}

func (rf *Raft) leaderAppendLogEntry(entry LogEntry) int {
	rf.lastIndex++
	entry.Index = rf.lastIndex

	rf.logs = append(rf.logs, entry)

	rf.newAppend()

	return entry.Index
}

func (rf *Raft) logAfter(id int) []LogEntry {
	index := sort.Search(len(rf.logs), func(i int) bool {
		return rf.logs[i].Index >= id
	})
	if index == len(rf.logs) {
		return nil
	}
	return rf.logs[index:]
}

func (rf *Raft) LogAfter(id int) []LogEntry {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.logAfter(id)
}

func (rf *Raft) maybeTruncateLog(id int) {
	index := sort.Search(len(rf.logs), func(i int) bool {
		return rf.logs[i].Index >= id
	})

	if index == len(rf.logs) {
		return
	}

	rf.logs = rf.logs[:index+1]
	rf.lastIndex = rf.lastLog().Index
}

func (rf *Raft) Log(idx int) (LogEntry, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.log(idx)
}

func (rf *Raft) log(id int) (LogEntry, bool) {
	if id <= 0 || len(rf.logs) == 0 {
		return LogEntry{
			Index: 0,
			Term:  0,
		}, true
	}

	index := sort.Search(len(rf.logs), func(i int) bool {
		return rf.logs[i].Index >= id
	})

	if index == len(rf.logs) {
		return LogEntry{
			Index: 0,
			Term:  0,
		}, false
	}

	return rf.logs[index], true
}

func (rf *Raft) InitNextIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	me := rf.Me()
	count := len(rf.Peers())
	rf.nextIndex = make(map[int]int)
	for i := 0; i < count; i++ {
		if i == me {
			continue
		}
		lastLog := rf.lastLog()
		rf.nextIndex[i] = lastLog.Index + 1
	}
}

func (rf *Raft) NextIndex(sever int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.nextIndex[sever]
}

func (rf *Raft) SetNextIndex(sever, index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.nextIndex[sever] = index
}

func (rf *Raft) SetMatchIndex(server, idx int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.matchIndex == nil {
		rf.matchIndex = make(map[int]int)
	}
	rf.matchIndex[server] = idx
}

func (rf *Raft) MatchIndex(server int) int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.matchIndex[server]
}

func (rf *Raft) FollowerLoop(ctx context.Context) {
	rf.ResetElectionDeadline()
	DPrintf("follower %v...", rf.Me())

	for rf.IsFollower() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !rf.Wait4ElectionTimeout(ctx) {
			return
		}
		rf.BecomeCandidate()
		return
	}
}

func (rf *Raft) CandidateLoop(ctx context.Context) {
	me := rf.Me()
	count := len(rf.Peers())
	for rf.IsCandidate() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rf.ResetElectionTimedout()

		rf.mu.Lock()
		lastLog := rf.lastLog()
		rf.currentTerm++
		term := rf.currentTerm
		rf.resetElectionDeadline()
		rf.votedFor = me
		rf.mu.Unlock()
		DPrintf("%v candidate... term: %v", rf.me, rf.currentTerm)

		resCh := make(chan *RequestVoteReply, count-1)
		args := &RequestVoteArgs{
			Term:         term,
			CandidateID:  me,
			LastLogIndex: lastLog.Index,
			LastLogTerm:  lastLog.Term,
		}

		for peer := 0; peer < count; peer++ {
			if peer == me {
				// skip requesting vote to myself
				continue
			}
			go func(peer int) {
				ctx, cancel := context.WithTimeout(ctx, HeartbeatInterval)
				defer cancel()
				reply := &RequestVoteReply{}
				if !rf.sendRequestVote(ctx, peer, args, reply) {
					return
				}
				resCh <- reply
			}(peer)
		}

		votes := 1
		timer := time.NewTimer(rf.ElectionTimeout())
	Loop:
		for {
			select {
			case <-ctx.Done():
				return
			case res := <-resCh:
				if res.Term > rf.Term() {
					rf.BecomeFollower(res.Term, -1)
					rf.cancel()
					return
				}

				if res.VoteGranted {
					votes++
					if votes > count/2 {
						rf.MaybeBecomeLeader()
						rf.cancel()
						return
					}
				}
			case <-timer.C:
				break Loop
			}
		}
		timer.Stop()
	}
}

func (rf *Raft) LeaderLoop(ctx context.Context) {

	count := len(rf.Peers())
	me := rf.Me()
	newMatched := make(chan struct{}, count-1)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	DPrintf("leader... %d", me)

	rf.InitNextIndex()

	for peer := 0; peer < count; peer++ {

		if peer == me {
			//skip myself
			continue
		}

		go func(peer int) {
			na := rf.NewAppended()
			rf.NewAppend()

			ticker := time.NewTicker(HeartbeatInterval)
			defer ticker.Stop()

			for rf.IsLeader() {
				select {
				case <-ctx.Done():
					return
				default:
				}

				index := rf.NextIndex(peer)
				var entries []LogEntry

				select {
				case <-ctx.Done():
					return
				case <-na:
					na = rf.NewAppended()
				case <-ticker.C:
				}
				entries = rf.LogAfter(index)
				prevEntry, ok := rf.Log(index - 1)
				if !ok {
					lastlog := rf.LastLog()
					rf.SetNextIndex(peer, lastlog.Index+1)
					continue
				}

				ci := rf.CommitIndex()
				term := rf.Term()
				args := &AppendEntriesArgs{
					Term:         term,
					LeaderID:     me,
					Entries:      entries,
					LeaderCommit: ci,
					PrevLogIndex: index - 1,
					PrevLogTerm:  prevEntry.Term,
				}

				reply := &AppendEntriesReply{}
				ctx, cancel := context.WithTimeout(ctx, AppendEntriesTimeout)
				if !rf.sendAppendEntries(ctx, peer, args, reply) {
					cancel()
					continue
				}
				cancel()

				if len(args.Entries) != 0 {
					DPrintf("send entries, args :%v, reply: %v", args, reply)
				}

				if !reply.Success {
					if reply.Term > rf.Term() {
						rf.BecomeFollower(reply.Term, -1)
						rf.cancel()
						return
					}
					rf.SetNextIndex(peer, index-1)
				} else {
					rf.SetNextIndex(peer, index+len(entries))
					rf.SetMatchIndex(peer, index+len(entries)-1)
					if len(entries) > 0 {
						newMatched <- struct{}{}
					}
				}
			}
		}(peer)
	}

	for rf.IsLeader() {
		select {
		case <-ctx.Done():
			return
		case <-newMatched:
		}

		lastLog := rf.LastLog()
		commitIndex := rf.CommitIndex()
		for i := lastLog.Index; i > commitIndex; i-- {

			select {
			case <-ctx.Done():
				return
			default:
			}

			entry, ok := rf.Log(i)
			if !ok {
				break
			}

			// leader only commit log in current term
			if entry.Term != rf.Term() {
				// only commit the entries in current term
				break
			}

			matched := 1
			for ii := 0; ii < count; ii++ {
				if ii == me {
					continue
				}
				if rf.MatchIndex(ii) >= i {
					matched++
				}
			}
			if matched > count/2 {
				rf.SetCommitIndex(i)
				DPrintf("leader %v new commit index: %v", rf.me, i)
				rf.MaybeApplyEntry()
				break
			}
		}
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:     peers,
		me:        me,
		persister: persister,

		currentTerm: 0,
		votedFor:    -1,
		commitIndex: 0,
		lastApplied: 0,
		lastIndex:   0,
		logs:        nil,

		nextIndex:  nil,
		matchIndex: nil,

		applyCh: applyCh,

		appendEvent: make(chan struct{}),

		closed: make(chan struct{}),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// startup as follower
	rf.BecomeFollower(-1, -1)
	rf.ResetElectionTimedout()
	rf.ResetElectionDeadline()

	go func() {
		for {
			select {
			case <-rf.closed:
				DPrintf("server %v closed...", rf.Me())
				return
			default:
			}
			ctx, cancel := context.WithCancel(context.Background())
			rf.cancel = cancel

			switch rf.status {
			case RaftStatus_Leader:
				rf.LeaderLoop(ctx)
			case RaftStatus_Follower:
				rf.FollowerLoop(ctx)
			case RaftStatus_Candidate:
				rf.CandidateLoop(ctx)
			}
			cancel()
		}
	}()

	return rf
}
