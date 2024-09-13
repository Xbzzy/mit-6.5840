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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

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
	Follower  = 1
	Candidate = 2
	Leader    = 3
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// Raft A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	identity      int   // server identity(follower/candidate/leader)
	leaderId      int   // index for leader into peers[]
	lastHeartbeat int64 // last time for receive leader's heartbeat(follower state)

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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

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
	switch rf.identity {
	case Leader, Candidate:
		if args.Term > rf.currentTerm {
			// receive bigger term, back to follower, grant vote
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			rf.leaderId = -1
			rf.identity = Follower
			reply.VoteGranted = true
		}
	case Follower:
		if rf.currentTerm > args.Term {
			// args term less than current term
			return
		}

		if rf.currentTerm == args.Term {
			if rf.votedFor >= 0 {
				// has voted
				return
			}

			if len(rf.logs) == 0 {
				// not have logs, grant vote directly
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				return
			}

			if args.LastLogIndex > 0 && args.LastLogIndex >= len(rf.logs) && args.LastLogTerm == rf.logs[args.LastLogIndex-1].Term {
				// log index up-to-date than now
				rf.votedFor = args.CandidateId
				reply.VoteGranted = true
				return
			}
			return
		}

		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
			return
		}
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
AppendEntries rules:
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
	Term    int  // currentTerm, for leader to update itself
	Me      int  // receive server index
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("AppendEntries start args:%v, me:%d", args, rf.me)

	reply.Term = rf.currentTerm
	reply.Me = rf.me
	switch rf.identity {
	case Leader:
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.lastHeartbeat = time.Now().UnixMilli()
			rf.identity = Follower
			rf.votedFor = -1
			rf.leaderId = args.LeaderId
		}
	case Candidate:
		// in election, but receive bigger term leader msg
		if args.Term >= rf.currentTerm {
			rf.currentTerm = args.Term
			rf.lastHeartbeat = time.Now().UnixMilli()
			rf.leaderId = args.LeaderId
			rf.identity = Follower
			rf.votedFor = args.LeaderId
		}

		// update log entries TODO
	case Follower:
		rf.leaderId = args.LeaderId
		rf.lastHeartbeat = time.Now().UnixMilli()

		// update log entries TODO

	}
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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
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
		case Leader:
			rf.mu.Unlock()
		case Candidate:
			// TODO
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

func (rf *Raft) startLeaderTicker() {
	rf.leaderTimer = time.NewTimer(0) // first heartbeat send soon
	for range rf.leaderTimer.C {
		if rf.killed() {
			// already killed
			DPrintf("startLeaderTicker has been killed, me:%d", rf.me)
			rf.leaderTimer.Stop()
			return
		}

		rf.mu.Lock()

		if rf.identity != Leader {
			// not already leader
			DPrintf("startLeaderTicker not leader return, me:%d", rf.me)
			rf.leaderTimer.Stop()
			rf.mu.Unlock()
			return
		}

		DPrintf("startLeaderTicker start send heartbeat, me:%d", rf.me)

		args := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: 0,
			PrevLogTerm:  0,
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}

		var (
			returnSign   bool
			receiveCount int32
			replyCh      = make(chan *AppendEntriesReply, len(rf.peers)-1)
		)
		for index := range rf.peers {
			if index == rf.me {
				continue
			}

			go func(server int) {
				reply := AppendEntriesReply{}
				ok := rf.sendAppendEntries(server, &args, &reply)
				if !ok {
					DPrintf("startLeaderTicker AppendEntries to peer:%d !ok, me:%d", server, rf.me)
					return
				}

				if returnSign {
					return
				}

				replyCh <- &reply
				if atomic.AddInt32(&receiveCount, 1) == int32(len(rf.peers)-1) {
					close(replyCh)
				}
			}(index)
		}

		cancelTimer := time.NewTimer(getWaitDuration())

	innerTimer:
		for {
			select {
			case <-cancelTimer.C:
				cancelTimer.Stop()
				DPrintf("startLeaderTicker timeout break, me:%d", rf.me)
				break innerTimer
			case reply := <-replyCh:
				if reply == nil {
					break innerTimer
				}

				DPrintf("startLeaderTicker recevied reply:%v, me:%d", reply, rf.me)

				if reply.Term > rf.currentTerm {
					// receive bigger term, become to follower
					DPrintf("startLeaderTicker reply term:%d big than %d !ok, me:%d", reply.Term, rf.currentTerm, rf.me)
					rf.currentTerm = reply.Term
					rf.leaderId = -1
					rf.votedFor = -1
					rf.identity = Follower
					rf.lastHeartbeat = time.Now().UnixMilli()
					rf.mu.Unlock()
					returnSign = true
					rf.leaderTimer.Stop()
					return
				}

				if !reply.Success {
					continue
				}

				// update log index state each server TODO
			}
		}

		DPrintf("startLeaderTicker receive reply end, me:%d", rf.me)
		rf.leaderTimer.Reset(getHeartbeatDuration())
		rf.mu.Unlock()
	}
}

func (rf *Raft) startElection() (newElection bool) {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.identity = Candidate

	args := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}
	if len(rf.logs) > 0 {
		args.LastLogIndex = len(rf.logs)
		args.LastLogTerm = rf.logs[len(rf.logs)-1].Term
	}

	var (
		returnSign   bool
		receiveCount int32
		replyCh      = make(chan *RequestVoteReply, len(rf.peers)-1)
	)
	for index := range rf.peers {
		if index == rf.me {
			continue
		}

		// send request vote rpc in parallel to each the other servers in the cluster
		go func(server int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(server, &args, &reply)
			if !ok {
				DPrintf("startElection sendRequestVote to peer:%d !ok,args:%v me:%d", server, args, rf.me)
				return
			}

			if returnSign {
				return
			}

			replyCh <- &reply
			if atomic.AddInt32(&receiveCount, 1) == int32(len(rf.peers)-1) {
				close(replyCh)
			}
		}(index)
	}

	cancelTimer := time.NewTimer(getWaitDuration())

	var hasVote int32 = 1
	for {
		select {
		case <-cancelTimer.C:
			// election timeout, back to follower for new election
			cancelTimer.Stop()
			rf.identity = Follower
			rf.votedFor = -1
			rf.leaderId = -1
			returnSign = true
			DPrintf("startElection timeout cancel, me:%d", rf.me)
			return
		case reply := <-replyCh:
			if reply == nil {
				// reply end, but vote not enough
				rf.identity = Follower
				rf.votedFor = -1
				rf.leaderId = -1
				DPrintf("startElection vote not enough, hasVote:%d, me:%d", hasVote, rf.me)
				return
			}

			DPrintf("startElection receive reply:%v, me:%d", reply, rf.me)

			if reply.Term > rf.currentTerm {
				// receive bigger term, become follower
				rf.currentTerm = reply.Term
				rf.identity = Follower
				rf.votedFor = -1
				rf.leaderId = -1
				returnSign = true
				DPrintf("startElection vote term more than me, replyTerm:%d, current:%d, me:%d", reply.Term, rf.currentTerm, rf.me)
				return
			}

			if !reply.VoteGranted {
				DPrintf("startElection vote not granted,server:%d me:%d", reply.Me, rf.me)
				continue
			}

			hasVote++
			DPrintf("startElection vote granted,hasVote:%d server:%d me:%d", hasVote, reply.Me, rf.me)
			if hasVote <= int32(len(rf.peers)/2) {
				continue
			}

			// wins the election
			DPrintf("startElection win the election, me:%d", rf.me)
			rf.leaderId = rf.me
			rf.identity = Leader
			go rf.startLeaderTicker()
			return
		}
	}

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
		identity:      Follower,
		leaderId:      -1,
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
	rf.readPersist(persister.ReadRaftState())

	DPrintf("me:%d start working", rf.me)

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func getElectionTimeOut() int64 {
	return rand.Int63n(200) + 300 // 300-500ms
}

func getWaitDuration() time.Duration {
	return time.Duration(rand.Int63n(50)+50) * time.Millisecond // 50-100ms
}

func getHeartbeatDuration() time.Duration {
	return time.Duration(rand.Int63n(50)+150) * time.Millisecond // 150-200ms
}
