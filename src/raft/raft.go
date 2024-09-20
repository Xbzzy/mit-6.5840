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
	"6.5840/labgob"
	"bytes"
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

/*
Raft rules for servers:
All Servers:
	• If commitIndex > lastApplied: increment lastApplied, apply
	log[lastApplied] to state machine (§5.3)
	• If RPC request or response contains term T > currentTerm:
	set currentTerm = T, convert to follower (§5.1)
Followers (§5.2):
	• Respond to RPCs from candidates and leaders
	• If election timeout elapses without receiving AppendEntries
	RPC from current leader or granting vote to candidate: convert to candidate
Candidates (§5.2):
	• On conversion to candidate, start election:
	• Increment currentTerm
	• Vote for self
	• Reset election timer
	• Send RequestVote RPCs to all other servers
	• If votes received from majority of servers: become leader
	• If AppendEntries RPC received from new leader: convert to
	follower
	• If election timeout elapses: start new election
Leaders:
	• Upon election: send initial empty AppendEntries RPCs
	(heartbeat) to each server; repeat during idle periods to
	prevent election timeouts (§5.2)
	• If command received from client: append entry to local log,
	respond after entry applied to state machine (§5.3)
	• If last log index ≥ nextIndex for a follower: send
	AppendEntries RPC with log entries starting at nextIndex
	• If successful: update nextIndex and matchIndex for
	follower (§5.3)
	• If AppendEntries fails because of log inconsistency: decrement nextIndex and retry (§5.3)
	• If there exists an N such that N > commitIndex, a majority
	of matchIndex[i] ≥ N, and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
*/

const (
	Follower            = 1
	Candidate           = 2
	Leader              = 3
	CommitApplyDuration = 20 * time.Millisecond // check if raft need apply log to state machine
)

type LogEntry struct {
	LogIndex int
	Term     int
	Command  interface{}
}

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	applyCh       chan ApplyMsg // apply command to state machine
	identity      int           // server identity(follower/candidate/leader)
	lastHeartbeat int64         // last time for receive leader's heartbeat(follower state)

	snapShot          []byte // snapshot
	lastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	lastIncludedTerm  int    // lastIncludedIndex's term
	hasSend           bool   // has sent snapshot

	// persistent state
	currentTerm int         // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int         // candidateId that received vote in current term (or null if none)
	logs        []*LogEntry // log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)

	// volatile state
	commitIndex int // index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile state on leaders (reinitialized after election)
	nextIndex   []int // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex  []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	leaderTimer *time.Timer
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	return rf.currentTerm, rf.identity == Leader
}

func (rf *Raft) getStartLogIndex() int {
	if len(rf.logs) == 0 {
		return 0
	}

	return rf.logs[0].LogIndex
}

func (rf *Raft) getLastLogIndex() int {
	if len(rf.logs) == 0 {
		return rf.lastIncludedIndex
	}

	return rf.logs[len(rf.logs)-1].LogIndex
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	raftState := w.Bytes()
	rf.persister.Save(raftState, rf.snapShot)
	return
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte, snapshot []byte) {
	if len(data) > 0 {
		r := bytes.NewBuffer(data)
		d := labgob.NewDecoder(r)
		var currentTerm int
		var votedFor int
		var logs []*LogEntry
		if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
			DPrintf("readPersist raft decode err, me:%d", rf.me)
			return
		}

		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
	}

	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var lastIncludedIndex int
		if d.Decode(&lastIncludedIndex) != nil {
			DPrintf("readPersist snapshot decode err, me:%d", rf.me)
			return
		}

		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastApplied = lastIncludedIndex
		rf.snapShot = snapshot
		rf.hasSend = true
	}
	return
}

// Snapshot the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if index < 0 {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Snapshot start, index:%d, logLen:%d, hasSnap:%v, me:%d", index, len(rf.logs), len(rf.snapShot) > 0, rf.me)

	if len(rf.logs) == 0 {
		return
	}

	var trimLen int
	if len(snapshot) == 0 {
		trimLen = index
	} else {
		firstIndex := rf.getStartLogIndex()
		trimLen = index - firstIndex + 1
	}

	rf.snapShot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.logs[trimLen-1].Term
	rf.hasSend = false

	rf.logs = rf.logs[trimLen:]
	rf.persist()

	DPrintf("Snapshot end, index:%d, logLen:%d, hasSnap:%v, me:%d", index, len(rf.logs), len(rf.snapShot) > 0, rf.me)
	return
}

/*
InstallSnapshot Receiver rules:
1. Reply immediately if term < currentTerm
2. Create new snapshot file if first chunk (offset is 0)
3. Write data into snapshot file at given offset
4. Reply and wait for more data chunks if done is false
5. Save snapshot file, discard any existing or partial snapshot with a smaller index
6. If existing log entry has same index and term as snapshot’s last included entry, retain log entries following it and reply
7. Discard the entire log
8. Reset state machine using snapshot contents (and load snapshot’s cluster configuration)
*/

type InstallSnapshotArgs struct {
	Term              int // leader's term
	LeaderId          int // so follower can redirect clients
	LastIncludedIndex int // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int // term of lastIncludedIndex
	// offset            int    // byte offset where chunk is positioned in the snapshot file
	Data []byte // raw bytes of the snapshot chunk, starting at offset
	//done bool   // true if this is the last chunk
}

type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
	Me   int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("InstallSnapshot start, lastIndex:%d, lastTerm:%d, me:%d", args.LastIncludedIndex, args.LastIncludedTerm, rf.me)

	reply.Term = rf.currentTerm
	reply.Me = rf.me
	if args.Term < rf.currentTerm {
		return
	}

	rf.lastHeartbeat = time.Now().UnixMilli()

	if len(rf.snapShot) > 0 && rf.lastIncludedIndex >= args.LastIncludedIndex {
		// has newer snapshot
		return
	}

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
	}

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.snapShot = args.Data

	if len(rf.logs) > 0 {
		startIndex := rf.getStartLogIndex()
		lastIndex := rf.getLastLogIndex()
		if args.LastIncludedIndex < lastIndex && rf.logs[args.LastIncludedIndex-startIndex].Term == args.LastIncludedTerm {
			rf.logs = rf.logs[args.LastIncludedIndex-startIndex+1:]
		} else {
			rf.logs = nil
		}
	}

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapShot,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.persist()
	return
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	DPrintf("sendInstallSnapshot to server:%d me:%d", server, rf.me)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

/*
RequestVote rules:
1. Reply false if term < currentTerm (§5.1)
2. If votedFor is null or candidateId, and candidate’s log is at
least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
*/

type RequestVoteArgs struct {
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	Me          int  // receive server index
	VoteGranted bool // true means candidate received vote
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("RequestVote start args:%v me:%d", args, rf.me)

	reply.Term = rf.currentTerm
	reply.Me = rf.me

	var hasChg bool
	defer func() {
		if !hasChg {
			return
		}
		rf.persist()
	}()

	switch rf.identity {
	case Leader, Candidate:
		if args.Term > rf.currentTerm {
			// receive bigger term, back to follower
			rf.currentTerm = args.Term
			rf.identity = Follower
			rf.votedFor = -1
			hasChg = true
		}
	case Follower:
		if rf.currentTerm > args.Term {
			// args term less than current term
			return
		}

		if rf.currentTerm == args.Term && rf.votedFor >= 0 {
			// similar term but has voted
			return
		}

		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			hasChg = true
		}

		if len(rf.logs) == 0 && len(rf.snapShot) == 0 {
			// not have logs, grant vote directly
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			hasChg = true
			return
		}

		if args.LastLogIndex <= 0 {
			// have logs, but args log index 0
			return
		}

		var myLastLog *LogEntry
		if len(rf.logs) > 0 {
			myLastLog = rf.logs[len(rf.logs)-1]
		} else {
			myLastLog = &LogEntry{
				LogIndex: rf.lastIncludedIndex,
				Term:     rf.lastIncludedTerm,
			}
		}

		if args.LastLogTerm < myLastLog.Term {
			// last log term less than me
			return
		}

		if args.LastLogTerm == myLastLog.Term && args.LastLogIndex < myLastLog.LogIndex {
			// term same, log index len less than me
			return
		}

		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		hasChg = true
		return
	}

	return
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	DPrintf("sendRequestVote to server:%d me:%d", server, rf.me)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

/*
AppendEntries receiver rules:
1. Reply false if term < currentTerm (§5.1)
2. Reply false if log doesn't contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
3. If an existing entry conflicts with a new one (same index
but different terms), delete the existing entry and all that
follow it (§5.3)
4. Append any new entries not already in the log
5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
*/

type AppendEntriesArgs struct {
	Term         int         // leader's term
	LeaderId     int         // so follower can redirect clients
	PrevLogIndex int         // index of log entry immediately preceding new ones
	PrevLogTerm  int         // term of prevLogIndex entry
	Entries      []*LogEntry // log entries to store (empty for heartbeat may send more than one for efficiency)
	LeaderCommit int         // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term     int  // currentTerm, for leader to update itself
	Me       int  // receive server index
	ErrTerm  int  // term in the conflicting entry
	ErrIndex int  // index of first entry with that term
	Len      int  // log length
	Success  bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("AppendEntries start args:%v, me:%d", args, rf.me)

	reply.Term = rf.currentTerm
	reply.Me = rf.me

	if rf.currentTerm > args.Term {
		return
	}

	switch rf.identity {
	case Leader, Candidate:

		// as leader or candidate, but receive bigger term leader append-entries msg
		rf.currentTerm = args.Term
		rf.lastHeartbeat = time.Now().UnixMilli()
		rf.identity = Follower
		rf.votedFor = -1

		rf.updateEntries(args, reply)
		rf.persist()
	case Follower:
		rf.currentTerm = args.Term
		rf.lastHeartbeat = time.Now().UnixMilli()

		chgLogs := rf.updateEntries(args, reply)
		if chgLogs || rf.currentTerm < args.Term {
			rf.persist()
		}
	}
	return
}

func (rf *Raft) updateEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) (chgLogs bool) {
	lastLogIndex := rf.getLastLogIndex()

	reply.Len = lastLogIndex
	if lastLogIndex < args.PrevLogIndex {
		return
	}

	startIndex := rf.getStartLogIndex()
	errIndex, errTerm, hasErr := rf.checkErrTerm(startIndex, args.PrevLogIndex, args.PrevLogTerm)
	if hasErr {
		reply.ErrIndex = errIndex
		reply.ErrTerm = errTerm
		return
	}

	// update log entries
	reply.Success = true

	chgLogs = rf.appendLogAfterLastIndex(args.Entries, startIndex, args.PrevLogIndex)

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.getLastLogIndex())
	}
	DPrintf("updateEntries success,logs:%v me:%d", args.Entries, rf.me)
	return
}

func (rf *Raft) checkErrTerm(startIndex, prevLogIndex int, prevLogTerm int) (errIndex, errTerm int, hasErr bool) {
	if len(rf.logs) == 0 || prevLogIndex == 0 || prevLogIndex < startIndex {
		return
	}

	if prevLogIndex == rf.lastIncludedIndex {
		return
	}

	var myPreTerm int
	if len(rf.snapShot) > 0 {
		myPreTerm = rf.logs[prevLogIndex-startIndex].Term
	} else {
		myPreTerm = rf.logs[prevLogIndex-1].Term
	}

	if prevLogTerm == myPreTerm {
		return
	}

	hasErr = true
	errTerm = myPreTerm
	for index, log := range rf.logs {
		if log.Term == myPreTerm {
			errIndex = index + 1
			break
		}
	}
	return
}

func (rf *Raft) appendLogAfterLastIndex(entries []*LogEntry, startIndex, preLogIndex int) (chgLogs bool) {
	if len(entries) == 0 {
		return
	}

	chgLogs = true
	if len(rf.logs) == 0 || preLogIndex == 0 {
		rf.logs = entries
		return
	}

	rf.logs = append(rf.logs[:preLogIndex-startIndex+1], entries...)
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (index int, term int, leader bool) {
	if command == nil {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = -1
	term = -1
	if rf.killed() {
		return
	}

	if rf.identity != Leader {
		// not leader, do nothing
		return
	}

	log := LogEntry{
		LogIndex: rf.getLastLogIndex() + 1,
		Term:     rf.currentTerm,
		Command:  command,
	}
	rf.logs = append(rf.logs, &log)

	DPrintf("Start leader receive log:%v, curLogLen:%d, me:%d", log, len(rf.logs), rf.me)

	rf.nextIndex[rf.me] = log.LogIndex + 1
	rf.matchIndex[rf.me] = log.LogIndex
	for idx := range rf.nextIndex {
		if rf.nextIndex[idx] == log.LogIndex-1 {
			rf.nextIndex[idx] = log.LogIndex
		}
	}

	if rf.leaderTimer != nil {
		rf.leaderTimer.Reset(0)
	}

	index = log.LogIndex
	term = rf.currentTerm
	leader = true

	rf.persist()
	return
}

// Kill the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Check if a leader election should be started.
		DPrintf("election ticker start check, me:%d", rf.me)

		rf.mu.Lock()
		switch rf.identity {
		case Leader, Candidate:
			rf.mu.Unlock()
		case Follower:
			if time.Now().UnixMilli() >= rf.lastHeartbeat+getElectionTimeOut() {
				// long time not receive leader's heartbeat, starting election
				DPrintf("election ticker wait timeout, start election, term:%d me:%d", rf.currentTerm+1, rf.me)
				rf.startElection()
			}

			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) commitOrApplyTicker() {
	for rf.killed() == false {
		rf.mu.Lock()

		// find the mid-matchIndex and update commitIndex
		if rf.identity == Leader {
			tmpSlice := make([]int, 0, len(rf.matchIndex))
			for _, index := range rf.matchIndex {
				tmpSlice = append(tmpSlice, index)
			}

			sort.Ints(tmpSlice)
			mid := tmpSlice[len(tmpSlice)/2]

			if mid > 0 && len(rf.logs) > 0 {
				startIndex := rf.getStartLogIndex()
				if mid >= startIndex && rf.logs[mid-startIndex].Term == rf.currentTerm && mid > rf.commitIndex {
					DPrintf("commitOrApplyTicker leader update commit:%d, me:%d", mid, rf.me)
					rf.commitIndex = mid
				}
			}
		}

		if len(rf.snapShot) > 0 && !rf.hasSend {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapShot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.hasSend = true
		}

		if rf.commitIndex > rf.lastApplied {
			DPrintf("commitOrApplyTicker apply log, lastApplied:%d, me:%d", rf.lastApplied, rf.me)

			if len(rf.snapShot) > 0 {
				startIndex := rf.getStartLogIndex()
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied-startIndex+1].Command,
					CommandIndex: rf.logs[rf.lastApplied-startIndex+1].LogIndex,
				}
			} else {
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      rf.logs[rf.lastApplied].Command,
					CommandIndex: rf.logs[rf.lastApplied].LogIndex,
				}
			}

			rf.lastApplied++
		}
		rf.mu.Unlock()

		time.Sleep(CommitApplyDuration)
	}
}

const (
	Append   = 1
	Snapshot = 2
)

type EntriesReply struct {
	replyType int
	data      interface{}
}

func (rf *Raft) startLeaderTicker() {
	for range rf.leaderTimer.C {
		rf.mu.Lock()

		if rf.killed() {
			// already killed
			DPrintf("startLeaderTicker has been killed, me:%d", rf.me)
			rf.leaderTimer.Stop()
			rf.mu.Unlock()
			return
		}

		if rf.identity != Leader {
			// not already leader
			DPrintf("startLeaderTicker not leader return, me:%d", rf.me)
			rf.leaderTimer.Stop()
			rf.mu.Unlock()
			return
		}

		DPrintf("startLeaderTicker start send heartbeat, me:%d", rf.me)

		var (
			returnSign   int32
			receiveCount int32
			replyCh      = make(chan *EntriesReply, len(rf.peers)-1)
		)
		for index := range rf.peers {
			if index == rf.me {
				continue
			}

			rf.checkAndSendEntriesArgs(index, &receiveCount, &returnSign, replyCh)
		}

		var retry bool
		cancelTimer := time.NewTimer(getWaitDuration())

	innerTimer:
		for {
			select {
			case <-cancelTimer.C:
				cancelTimer.Stop()
				DPrintf("startLeaderTicker timeout break, me:%d", rf.me)
				break innerTimer
			case replyData := <-replyCh:
				if replyData == nil {
					break innerTimer
				}

				switch replyData.replyType {
				case Append:
					reply := replyData.data.(*AppendEntriesReply)
					DPrintf("startLeaderTicker recevied append entries reply:%v, me:%d", reply, rf.me)

					if reply.Term > rf.currentTerm {
						// receive bigger term, become to follower
						DPrintf("startLeaderTicker reply term:%d big than %d !ok, me:%d", reply.Term, rf.currentTerm, rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identity = Follower
						rf.lastHeartbeat = time.Now().UnixMilli()
						atomic.StoreInt32(&returnSign, 1)
						rf.leaderTimer.Stop()

						rf.persist()
						rf.mu.Unlock()
						return
					}

					if !reply.Success {
						// decrement next index and retry
						rf.updateNextIndex(reply)
						retry = true
						continue
					}

					// update log index state for each server
					lastLogIndex := rf.getLastLogIndex()
					rf.nextIndex[reply.Me] = lastLogIndex + 1
					rf.matchIndex[reply.Me] = lastLogIndex
				case Snapshot:
					reply := replyData.data.(*InstallSnapshotReply)
					DPrintf("startLeaderTicker recevied install snapshot reply:%v, me:%d", reply, rf.me)

					if reply.Term > rf.currentTerm {
						// receive bigger term, become to follower
						DPrintf("startLeaderTicker reply term:%d big than %d !ok, me:%d", reply.Term, rf.currentTerm, rf.me)
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.identity = Follower
						rf.lastHeartbeat = time.Now().UnixMilli()
						atomic.StoreInt32(&returnSign, 1)
						rf.leaderTimer.Stop()

						rf.persist()
						rf.mu.Unlock()
						return
					}

					// update log index state for each server
					rf.nextIndex[reply.Me] = rf.lastIncludedIndex + 1
					rf.matchIndex[reply.Me] = rf.lastIncludedIndex
				}

			}
		}

		DPrintf("startLeaderTicker receive reply end,curCommit:%d, nextIndex:%v,matchIndex:%v me:%d", rf.commitIndex, rf.nextIndex, rf.matchIndex, rf.me)

		if retry {
			rf.leaderTimer.Reset(0)
		} else {
			rf.leaderTimer.Reset(getHeartbeatDuration())
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) checkAndSendEntriesArgs(server int, receiveCount, returnSign *int32, replyCh chan *EntriesReply) {
	peerNextIndex := rf.nextIndex[server]
	if len(rf.snapShot) > 0 && peerNextIndex <= rf.lastIncludedIndex {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapShot,
		}

		go func() {
			reply := InstallSnapshotReply{}
			ok := rf.sendInstallSnapshot(server, &args, &reply)
			if !ok {
				DPrintf("startLeaderTicker AppendEntries to peer:%d !ok, me:%d", server, rf.me)
				return
			}

			if atomic.LoadInt32(returnSign) > 0 {
				return
			}

			replyCh <- &EntriesReply{
				replyType: Snapshot,
				data:      &reply,
			}
			if atomic.AddInt32(receiveCount, 1) == int32(len(rf.peers)-1) {
				close(replyCh)
			}
		}()
		return
	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
	}
	args.PrevLogIndex, args.PrevLogTerm, args.Entries = rf.getAppendPeerEntries(peerNextIndex)

	go func() {
		reply := AppendEntriesReply{}
		ok := rf.sendAppendEntries(server, &args, &reply)
		if !ok {
			DPrintf("startLeaderTicker AppendEntries to peer:%d !ok, me:%d", server, rf.me)
			return
		}

		if atomic.LoadInt32(returnSign) > 0 {
			return
		}

		replyCh <- &EntriesReply{
			replyType: Append,
			data:      &reply,
		}
		if atomic.AddInt32(receiveCount, 1) == int32(len(rf.peers)-1) {
			close(replyCh)
		}
	}()
	return
}

func (rf *Raft) updateNextIndex(reply *AppendEntriesReply) {
	/*
		Case 1: leader doesn't have ErrTerm:
		    nextIndex = ErrIndex
		Case 2: leader has ErrTerm:
		    nextIndex = leader's last entry for ErrTerm
		Case 3: follower's log is too short:
		    nextIndex = Len
	*/
	if reply.ErrTerm > 0 {
		var hasTerm bool
		for index, log := range rf.logs {
			if log.Term == reply.ErrTerm {
				rf.nextIndex[reply.Me] = index + 1
				hasTerm = true
				break
			}
		}

		if !hasTerm {
			rf.nextIndex[reply.Me] = reply.ErrIndex
		}
	} else {
		rf.nextIndex[reply.Me] = reply.Len
	}
}

func (rf *Raft) getAppendPeerEntries(peerNextIndex int) (preIndex, preTerm int, entries []*LogEntry) {
	if len(rf.logs) == 0 {
		preIndex = rf.lastIncludedIndex
		preTerm = rf.lastIncludedTerm
		return
	}

	startIndex := rf.getStartLogIndex()
	curIndex := rf.getLastLogIndex()
	if curIndex < peerNextIndex {
		// cur index less than next index, check cur index and term
		preIndex = curIndex
		if preIndex == rf.lastIncludedIndex {
			preTerm = rf.lastIncludedTerm
		} else if preIndex > 0 {
			preTerm = rf.logs[preIndex-startIndex].Term
		}
		return
	}

	if peerNextIndex <= 1 {
		// next index less than 2, not have pre log
		entries = rf.logs
		return
	}

	if peerNextIndex == rf.lastIncludedIndex {
		preIndex = rf.lastIncludedIndex
		preTerm = rf.lastIncludedTerm
		entries = rf.logs
		return
	}

	preIndex = peerNextIndex - 1
	if preIndex == rf.lastIncludedIndex {
		preTerm = rf.lastIncludedTerm
	} else {
		preTerm = rf.logs[preIndex-startIndex].Term
	}
	entries = rf.logs[peerNextIndex-startIndex:]
	return
}

func (rf *Raft) startElection() (newElection bool) {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.identity = Candidate
	defer rf.persist()

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if len(rf.logs) > 0 {
		lastLog := rf.logs[len(rf.logs)-1]
		args.LastLogIndex = lastLog.LogIndex
		args.LastLogTerm = lastLog.Term
	} else {
		args.LastLogIndex = rf.lastIncludedIndex
		args.LastLogTerm = rf.lastIncludedTerm
	}

	var returnSign int32
	replyCh := rf.broadcastToPeers(&returnSign, "RequestVote", &args, func(rq interface{}, server int) (interface{}, bool) {
		reply := &RequestVoteReply{}
		ok := rf.sendRequestVote(server, rq.(*RequestVoteArgs), reply)
		return reply, ok
	})

	var (
		hasVote     = 1
		cancelTimer = time.NewTimer(getWaitDuration())
	)
	for {
		select {
		case <-cancelTimer.C:
			// election timeout, back to follower for new election
			cancelTimer.Stop()
			rf.identity = Follower
			rf.votedFor = -1
			atomic.StoreInt32(&returnSign, 1)
			DPrintf("startElection timeout cancel, me:%d", rf.me)
			return
		case data := <-replyCh:
			if data == nil {
				// reply end, but vote not enough
				rf.identity = Follower
				rf.votedFor = -1
				DPrintf("startElection vote not enough, hasVote:%d, me:%d", hasVote, rf.me)
				return
			}
			reply := data.(*RequestVoteReply)

			DPrintf("startElection receive reply:%v, me:%d", reply, rf.me)

			if reply.Term > rf.currentTerm {
				// receive bigger term, become follower
				DPrintf("startElection vote term more than me, replyTerm:%d, current:%d, me:%d", reply.Term, rf.currentTerm, rf.me)
				rf.currentTerm = reply.Term
				rf.identity = Follower
				rf.votedFor = -1
				atomic.StoreInt32(&returnSign, 1)
				return
			}

			if !reply.VoteGranted {
				DPrintf("startElection vote not granted,server:%d me:%d", reply.Me, rf.me)
				continue
			}

			hasVote++
			DPrintf("startElection vote granted,hasVote:%d server:%d me:%d", hasVote, reply.Me, rf.me)
			if hasVote <= len(rf.peers)/2 {
				continue
			}

			// wins the election
			rf.identity = Leader
			// init next index to leader last log index + 1
			rf.nextIndex = make([]int, len(rf.peers))

			initIndex := rf.getLastLogIndex() + 1
			for index := range rf.nextIndex {
				rf.nextIndex[index] = initIndex
			}

			rf.matchIndex = make([]int, len(rf.peers))
			DPrintf("startElection win the election, next:%v, match:%v, logs:%v, me:%d", rf.nextIndex, rf.matchIndex, rf.logs, rf.me)

			rf.leaderTimer = time.NewTimer(0) // first heartbeat send soon
			go rf.startLeaderTicker()
			return
		}
	}

}

func (rf *Raft) broadcastToPeers(returnSign *int32, name string, args interface{},
	callFunc func(interface{}, int) (interface{}, bool)) (replyCh chan interface{}) {

	var (
		receiveCount int32
	)
	replyCh = make(chan interface{}, len(rf.peers)-1)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// send request vote rpc in parallel to each the other servers in the cluster
		go func(server int) {
			reply, ok := callFunc(args, server)
			if !ok {
				DPrintf("broadcastToPeers %s to peer:%d !ok, args:%v me:%d", name, server, args, rf.me)
				return
			}

			if atomic.LoadInt32(returnSign) > 0 {
				return
			}

			replyCh <- reply
			if atomic.AddInt32(&receiveCount, 1) == int32(len(rf.peers)-1) {
				close(replyCh)
			}
		}(index)
	}
	return
}

// Make the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:            sync.RWMutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		applyCh:       applyCh,
		identity:      Follower,
		lastHeartbeat: 0,
		currentTerm:   0,
		votedFor:      -1, // default -1
		logs:          nil,
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		leaderTimer:   nil,
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	DPrintf("me:%d start working", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.commitOrApplyTicker()

	return rf
}

func getElectionTimeOut() int64 {
	return rand.Int63n(200) + 300 // 300-500ms
}

func getWaitDuration() time.Duration {
	return time.Duration(rand.Int63n(25)+5) * time.Millisecond // 5-30ms
}

func getHeartbeatDuration() time.Duration {
	return time.Duration(rand.Int63n(50)+150) * time.Millisecond // 150-200ms
}
