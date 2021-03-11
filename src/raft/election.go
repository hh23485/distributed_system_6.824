package raft

import (
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

var Role_Follower int = 0
var Role_Candidate int = 1
var Role_Leader int = 2

var heartBeatTimeout time.Duration = time.Millisecond * 20
var rpcTimeout time.Duration = time.Millisecond * 5

func (rf *Raft) SetAsLeader() bool {
	rf.statusChangeMutex.Lock()
	defer rf.statusChangeMutex.Unlock()

	rf.role = Role_Leader
	atomic.StoreInt32(&rf.votedFor, -1)
	rf.UpdateLeader(rf.me)
	log.Printf("in [term %d], [node %d] begin to be a leader ----------------------", rf.currentTerm, rf.me)
	return true
}

func (rf *Raft) SetAsCandidate() bool {
	rf.statusChangeMutex.Lock()
	defer rf.statusChangeMutex.Unlock()

	// if connected, exit election
	if rf.IsLeaderConnected(false) {
		return false
	}
	//if rf.GetCurrentVotedFor() != -1 {
	//	return false
	//}

	rf.role = Role_Candidate
	rf.UpdateVotedFor(rf.me)
	rf.UpdateLeader(-1)

	currentTerm := rf.GetCurrentTerm()
	rf.IncrementTerm()
	log.Printf("in [term %d -> %d], [node %d] begin to be a candidate ----------------------", currentTerm, rf.GetCurrentTerm(), rf.me)
	return true
}

func (rf *Raft) SetAsFollowerWithLock(term int, leader int) bool {
	rf.statusChangeMutex.Lock()
	defer rf.statusChangeMutex.Unlock()

	if term < rf.GetCurrentTerm() {
		log.Printf("in [term %d], [node %d] failed to set as follower, target %d less than current term %d", rf.currentTerm, rf.me, rf.GetCurrentTerm(), term)
		return false
	}

	if term != rf.GetCurrentTerm() || leader != rf.GetCurrentLeader() {
		log.Printf("in [term %d -> %d], [node %d] begin to be a follower of leader %d ----------------------", rf.currentTerm, term, rf.me, leader)
	}

	rf.role = Role_Follower
	rf.UpdateVotedFor(leader)
	rf.UpdateTerm(term)
	rf.UpdateLeader(leader)
	return true
}

func (rf *Raft) DoElection() bool {
	randTimeBeforeElection := GetRandomBetween(100, 300)
	time.Sleep(time.Duration(randTimeBeforeElection) * time.Millisecond)

	if rf.IsLeaderConnected(false) {
		// if find leader, stop election
		return false
	}

	if rf.killed() {
		// if killed, stop election
		return false
	}

	log.Printf("in [term %d], [node %d]'s leader %d is disconnected, go on for election", rf.GetCurrentTerm(), rf.me, rf.GetCurrentLeader())

	if !rf.SetAsCandidate() {
		log.Printf("in [term %d], [node %d] failed to be a candidate, give up current election", rf.GetCurrentTerm(), rf.me)
		return false
	}

	currentTerm := rf.GetCurrentTerm()
	args := RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}

	voteJobCtx := &VoteJobContext{
		&args,
		map[int]*RequestVoteReply{},
		int32(args.Term),
		&sync.Mutex{},
	}

	electionTimeoutMilli := GetRandomBetween(100, 500)
	notTimeout, isWin := RunJobWithTimeLimit(electionTimeoutMilli, func() bool {
		log.Printf("in [term %d], [node %d] 🏁 start a election *******************", currentTerm, rf.me)
		reqContext := voteJobCtx
		waitGroup := sync.WaitGroup{}

		// send to all peers in go routes
		for idx, _ := range rf.peers {
			waitGroup.Add(1)
			go func(idx int) {
				defer waitGroup.Done()
				if idx == rf.me {
					// lock and set reply
					reqContext.SetReplyWithLock(idx, &RequestVoteReply{
						args.Term,
						true,
					})
					return
				}

				if rf.killed() {
					// quick return
					return
				}

				nTimeout, _ := RunJobWithTimeLimit(rpcTimeout.Milliseconds(), func() bool {
					copiedReply := RequestVoteReply{}
					res := rf.sendRequestVote(idx, reqContext.args, &copiedReply)
					{
						reqContext.SetReplyWithLock(idx, &copiedReply)
					}
					return res
				})

				if !nTimeout {
					log.Printf("[Async] in [term %d], [node %d] ⚠️️ sendRequestVote %d -> %d, timeout", args.Term, rf.me, rf.me, idx)
				}
			}(idx)
		}
		waitGroup.Wait()

		if rf.killed() {
			return false
		}

		// check result and set to context
		totalCount := len(rf.peers)
		granted := 0
		maxTerm := 0
		maxTermNode := -1
		for idx, _ := range rf.peers {
			if idx == rf.me {
				granted++
				continue
			}
			reply := reqContext.GetReply(idx)
			if reply == nil {
				log.Printf("[Async] in [term %d], [candidate %d] ↪️⚠️️ not recevied vote request from node %d", args.Term, rf.me, idx)
				continue
			}
			if reply.VoteGranted {
				granted++
			}
			if reply.Term > maxTerm {
				maxTerm = reply.Term
				maxTermNode = idx
			}
		}
		AtomicStoreInt(&reqContext.MaxTerm, maxTerm)

		// check if meet higher
		if maxTerm > args.Term {
			log.Printf("[Async] in [term %d], [candidate %d] ❌ meet higher term %d from peer %d", args.Term, rf.me, maxTerm, maxTermNode)
			return false
		}

		isWin := granted > (totalCount / 2)
		result := "win"
		if !isWin {
			result = "lose"
		}
		log.Printf("[Async] in [term %d], [candidate %d] %s the election, win %d/%d", args.Term, rf.me, result, granted, totalCount)
		return isWin
	})

	cCurrentTerm := rf.GetCurrentTerm()
	if !notTimeout {
		log.Printf("in [term %d], 🏳️ election for [candidate %d], timeout ********************", args.Term, rf.me)
	} else if args.Term < cCurrentTerm {
		log.Printf("in [term %d], [node %d] 🏳️ 's current term changed to %d during election ********************", args.Term, rf.me, cCurrentTerm)
	} else {
		//	valid
		if isWin {
			if rf.SetAsLeader() {
				log.Printf("in [term %d], [node %d] 🚩 win the election ********************", cCurrentTerm, rf.me)
				return true
			}
			log.Printf("in [term %d], [node %d] set as leader failed ********************", cCurrentTerm, rf.me)
		} else {
			log.Printf("in [term %d], [node %d] 🏳️ lose the election for candidate ********************", cCurrentTerm, rf.me)
		}
	}

	//rf.UpdateVotedFor(-1)
	maxTerm := AtomicLoadInt(&voteJobCtx.MaxTerm)
	nextTerm := Max(maxTerm, rf.GetCurrentTerm())
	rf.SetAsFollowerWithLock(nextTerm, -1)

	return false
}

func (rf *Raft) IsLeaderConnected(verbose bool) bool {
	rf.pingMutex.Lock()
	defer rf.pingMutex.Unlock()

	if rf.IsLeader() {
		return true
	}

	currentTerm := rf.GetCurrentTerm()
	timeAfterLastPing := time.Now().Sub(rf.lastPing)

	if timeAfterLastPing >= rf.pingTimeout {
		if verbose {
			log.Printf("in [term %d], [node %d] long time no hear from leader %d, timeAfterLastPing: %d millisec", currentTerm, rf.me, rf.GetCurrentLeader(), timeAfterLastPing.Milliseconds())
		}
		return false
	}

	return true
}

func (rf *Raft) WaitingForPingTimeout() (isTimeout bool) {
	for rf.killed() == false {
		// if current node is leader, return
		if rf.IsLeader() {
			return false
		}
		if !rf.IsLeaderConnected(true) {
			return true
		}
		// check a few seconds
		time.Sleep(1 * time.Millisecond)
	}
	return false
}

func GetRandomBetween(min, max int64) int64 {
	diff := max - min
	return rand.Int63n(diff) + min
}

func (rf *Raft) startSendHeartBeat() {
	for rf.killed() == false {
		waitGroup := sync.WaitGroup{}

		currentTerm, isLeader := rf.GetState()
		if !isLeader {
			log.Printf("in [term %d], [node %d] is not leader now, stop send heart beat, new leader: %d", currentTerm, rf.me, rf.GetCurrentLeader())
			return
		}

		args := &AppendEntriesArgs{}
		args.Term = rf.GetCurrentTerm()
		args.LeaderId = rf.me

		jobContext := &AppendEntriesJobContext{
			ReplyMap{
				replyMap: map[int]interface{}{},
				mutex:    &sync.Mutex{},
			},
			int32(args.Term),
			args,
		}

		for idx, _ := range rf.peers {
			waitGroup.Add(1)
			go func(idx int) {
				defer waitGroup.Done()
				if idx == rf.me {
					return
				}

				currentTermWhenSendingHeartBeat, isLeader := rf.GetState()
				// quick fail
				if !isLeader {
					log.Printf("in [term %d -> %d], [node %d] ⛔️ is not leader now, stop send heart beat, new leader: %d", currentTerm, currentTermWhenSendingHeartBeat, rf.me, rf.GetCurrentLeader())
					return
				}
				if rf.killed() {
					log.Printf("killed now, [node %d] stop send heart beat", rf.me)
					return
				}

				log.Printf("in [term %d], [node %d] [StartSendHeartBeat] ➡️ send heart beat from leader: %d -> %d, isLeader: %v, term: %d", currentTermWhenSendingHeartBeat, rf.me, rf.me, idx, isLeader, currentTermWhenSendingHeartBeat)

				notTimeout, _ := RunJobWithTimeLimit(rpcTimeout.Milliseconds(), func() bool {
					reply:=&AppendEntriesReply{}
					res := rf.sendHeartBeat(idx, args, reply)
					jobContext.ReplyMap.SetReplyWithLock(idx, reply)
					return res
				})
				cCurrentTerm := rf.GetCurrentTerm()
				if !notTimeout {
					log.Printf("in [term %d], [node %d] ❌ send heart beat to node %d timeout", cCurrentTerm, rf.me, idx)
				}

			}(idx)
		}
		waitGroup.Wait()

		if rf.GetCurrentLeader() != rf.me {
			log.Printf("in [term %d], [node %d] ⛔️ is not leader now, stop send heart beat, new leader: %d", rf.GetCurrentTerm(), rf.me, rf.leader)
			return
		}

		cCurrentTerm := rf.GetCurrentTerm()
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			reply := jobContext.GetReply(idx)
			if reply == nil {
				log.Printf("in [term %d], [node %d] ↪️⚠️️ not received reject heart beat from node %d", cCurrentTerm, rf.me, idx)
				continue
			}
			replyContext := reply.(*AppendEntriesReply)
			if replyContext.Term > cCurrentTerm {
				log.Printf("in [term %d], [node %d] ↪️ send heart beat met higher term from node %d, back to follower", cCurrentTerm, rf.me, idx)
				rf.SetAsFollowerWithLock(replyContext.Term, -1)
				return
			}
			if !replyContext.Success {
				log.Printf("in [term %d], [node %d] ↪️ received reject heart beat from node %d", cCurrentTerm, rf.me, idx)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) UpdateTerm(term int) {
	atomic.StoreInt32(&rf.currentTerm, int32(term))
}

//func (rf *Raft) UpdateTermWithLock(term int) {
//	rf.statusChangeMutex.Lock()
//	defer rf.statusChangeMutex.Unlock()
//	rf.UpdateTerm(term)
//}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm := rf.GetCurrentTerm()
	if len(args.Entries) == 0 {
		if currentTerm <= args.Term {
			// if failed to be the follower, stop next actions
			if !rf.SetAsFollowerWithLock(args.Term, args.LeaderId) {
				reply.Success = false
				reply.Term = rf.GetCurrentTerm()
				log.Printf("in [term %d], [node %d] ⬅️ reject heart beat, current term: %d > heart term: %d", rf.GetCurrentTerm(), rf.me, rf.GetCurrentTerm(), args.Term)
				return
			}
			//rf.heartBeatMutex.Lock()
			//defer rf.heartBeatMutex.Unlock()
			rf.UpdatePingWithLock()
			reply.Term = rf.GetCurrentTerm()
			reply.Success = true
			log.Printf("in [term %d], [node %d] ⬅️👌 [AppendEntries received] heart beat, %d -> %d, current term: %d, next term: %d", currentTerm, rf.me, currentTerm, rf.me, reply.Term, args.Term)
			return
		}
		currentTerm = rf.GetCurrentTerm()
		log.Printf("in [term %d], [node %d] ⬅️ reject heart beat, current term: %d > heart term: %d", currentTerm, rf.me, currentTerm, args.Term)
	}

	reply.Success = false
	reply.Term = rf.GetCurrentTerm()
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	currentTerm := rf.GetCurrentTerm()
	log.Printf("in [term %d], [node %d] [RequestVote received] ⬅️ %v", currentTerm, rf.me, args)

	if args.Term <= currentTerm {
		reply.VoteGranted = false
		log.Printf("in [term %d], [node %d] ⬅️ reject voted for %d, curretn term %d >= vote term %d", currentTerm, rf.me, args.CandidateId, currentTerm, args.Term)
		return
	}

	reply.Term = currentTerm

	//higher one
	if args.Term > currentTerm {

		rf.statusChangeMutex.Lock()
		defer rf.statusChangeMutex.Unlock()

		currentTermInLock := rf.GetCurrentTerm()
		if args.Term <= currentTermInLock {
			log.Printf("in [term %d], [node %d] ⬅️ reject voted for %d, curretn term %d > vote term %d", currentTermInLock, rf.me, args.CandidateId, currentTermInLock, args.Term)
			return
		}

		log.Printf("in [term %d -> term %d], [node %d] ⬅️👍 voted for %d, term: %d", currentTermInLock, args.Term, rf.me, args.CandidateId, args.Term)

		rf.UpdateTerm(args.Term)
		rf.UpdateVotedFor(args.CandidateId)
		rf.UpdatePingWithLock()

		// update status
		reply.VoteGranted = true
		reply.Term = currentTermInLock
		return
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func (rf *Raft) UpdateLeader(leader int) {
	AtomicStoreInt(&rf.leader, leader)
}

func (rf *Raft) GetCurrentVotedFor() int {
	return AtomicLoadInt(&rf.votedFor)
}

func (rf *Raft) UpdateVotedFor(votedFor int) {
	AtomicStoreInt(&rf.votedFor, votedFor)
}

func (rf *Raft) IncrementTerm() {
	atomic.AddInt32(&rf.currentTerm, 1)
}

func (rf *Raft) GetDurationSinceLastPing() time.Duration {
	return time.Now().Sub(rf.lastPing)
}

func (rf *Raft) UpdatePingWithLock() {
	rf.pingMutex.Lock()
	defer rf.pingMutex.Unlock()
	rf.UpdatePing()
}

func (rf *Raft) UpdatePing() {
	rf.lastPing = time.Now()
	rf.pingTimeout = time.Duration(GetRandomBetween(100, 300)) * time.Millisecond
}


func AtomicLoadInt(addr *int32) int {
	return int(atomic.LoadInt32(addr))
}

func AtomicStoreInt(addr *int32, val int) {
	atomic.StoreInt32(addr, int32(val))
}
