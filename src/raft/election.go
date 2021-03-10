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

var heartBeatTimeout time.Duration = time.Millisecond * 600
var rpcTimeout time.Duration = time.Millisecond * 100

func (rf *Raft) SetAsLeader() bool {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	rf.role = Role_Leader
	atomic.StoreInt32(&rf.votedFor, -1)
	rf.UpdateLeader(rf.me)
	log.Printf("in [term %d], [node %d] begin to be a leader ----------------------", rf.currentTerm, rf.me)
	return true
}

func (rf *Raft) SetAsCandidate() bool {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	if rf.GetCurrentVotedFor() != -1 {
		log.Printf("")
		return false
	}

	rf.role = Role_Candidate
	rf.UpdateVotedFor(rf.me)
	rf.UpdateLeader(-1)

	currentTerm := rf.GetCurrentTerm()
	rf.IncrementTerm()
	log.Printf("in [term %d -> %d], [node %d] begin to be a candidate ----------------------", currentTerm, rf.GetCurrentTerm(), rf.me)
	return true
}

func (rf *Raft) SetAsFollower(term int, leader int) {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	if term != rf.GetCurrentTerm() && leader != rf.GetCurrentLeader() {
		log.Printf("in [term %d -> %d], [node %d] begin to be a follower of leader %d ----------------------", rf.currentTerm, term, rf.me, leader)
	}

	rf.role = Role_Follower
	rf.UpdateVotedFor(leader)
	rf.UpdateTerm(term)
	rf.UpdateLeader(leader)
}

func (rf *Raft) DoElection() bool {
	var maxMilli int64 = 1000
	var minMilli int64 = 100
	electionTimeoutMilli := GetRandomBetween(minMilli, maxMilli)
	time.Sleep(time.Duration(electionTimeoutMilli) * time.Millisecond)

	if rf.IsLeaderConnected() {
		return false
	}

	if rf.killed() {
		return false
	}

	log.Printf("in [term %d], [node %d]'s leader %d is disconnected, go on for election", rf.GetCurrentTerm(), rf.me, rf.GetCurrentLeader())

	rf.SetAsCandidate()

	currentTerm := rf.GetCurrentTerm()
	arg := RequestVoteArgs{
		Term:        currentTerm,
		CandidateId: rf.me,
	}

	voteJobCtx := &VoteJobContext{
		&arg,
		map[int]*RequestVoteReply{},
		false,
		0,
		&sync.Mutex{},
	}

	notTimeout, _ := RunJobWithTimeLimit(electionTimeoutMilli, func() bool {
		log.Printf("in [term %d], [node %d] start a election *******************", currentTerm, rf.me)
		reqContext := voteJobCtx
		waitGroup := sync.WaitGroup{}
		replyMap := reqContext.voteResults

		//todo cancel initialization
		for idx, _ := range rf.peers {
			replyMap[idx] = &RequestVoteReply{
				arg.Term,
				false,
			}
		}

		// send to all peers in go routes
		for idx, _ := range rf.peers {
			waitGroup.Add(1)
			go func(idx int) {
				defer waitGroup.Done()
				reply := replyMap[idx]
				if idx == rf.me {
					reqContext.mutex.Lock()
					defer reqContext.mutex.Unlock()
					reply.Term = arg.Term
					reply.VoteGranted = true
					return
				}

				if rf.killed() {
					return
				}

				nTimeout, _ := RunJobWithTimeLimit(rpcTimeout.Milliseconds(), func() bool {
					return rf.sendRequestVote(idx, reqContext.args, reply)
				})

				if !nTimeout {
					log.Printf("[Async] in [term %d], [node %d] sendRequestVote %d -> %d, timeout", arg.Term, rf.me, rf.me, idx)
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
		maxTermPeer := -1
		for idx, reply := range replyMap {
			// todo reduce lock scope
			reqContext.mutex.Lock()
			if reply == nil {
				continue
			}
			if reply.VoteGranted {
				granted++
			}
			if reply.Term > maxTerm {
				maxTerm = reply.Term
				maxTermPeer = idx
			}
			reqContext.mutex.Unlock()
		}
		AtomicStoreInt(&reqContext.MaxTerm, maxTerm)

		// check if meet higher
		if maxTerm > arg.Term {
			log.Printf("[Async] in [term %d], candidate %d meet higher term %d from peer %d", arg.Term, rf.me, maxTerm, maxTermPeer)
			return false
		}

		reqContext.succ = granted > (totalCount / 2)
		win := "win"
		if !reqContext.succ {
			win = "lose"
		}
		log.Printf("[Async] %s the election for candidate %d in [term %d], win %d/%d", win, rf.me, arg.Term, granted, totalCount)
		return reqContext.succ
	})

	cCurrentTerm := rf.GetCurrentTerm()
	if !notTimeout {
		log.Printf("in [term %d], election for candidate %d, timeout ********************", arg.Term, rf.me)
	} else if arg.Term < cCurrentTerm {
		log.Printf("in [term %d], [node %d]'s current term changed to %d during election ********************", arg.Term, rf.me, cCurrentTerm)
	} else {
		//	valid
		if voteJobCtx.succ {
			if rf.SetAsLeader() {
				log.Printf("in [term %d], [node %d] win the election ********************", cCurrentTerm, rf.me)
				return true
			}
			log.Printf("in [term %d], [node %d] set as leader failed ********************", cCurrentTerm, rf.me)
		} else {
			log.Printf("in [term %d], [node %d] lose the election for candidate ********************", cCurrentTerm, rf.me)
		}
	}
	rf.UpdateVotedFor(-1)
	maxTerm := AtomicLoadInt(&voteJobCtx.MaxTerm)
	if maxTerm > rf.GetCurrentTerm() {
		rf.SetAsFollower(maxTerm, -1)
	}
	return false
}

func (rf *Raft) IsLeaderConnected() bool {
	currentLeader := rf.GetCurrentLeader()
	if currentLeader == rf.me {
		return true
	}

	currentTerm := rf.GetCurrentTerm()
	timeAfterLastPing := rf.GetDurationSinceLastPing()
	if timeAfterLastPing > heartBeatTimeout {
		log.Printf("in [term %d], [node %d] long time no hear from leader %d, timeAfterLastPing: %d millisec", currentTerm, rf.me, rf.leader, timeAfterLastPing.Milliseconds())
		return false
	}
	return true
}

func (rf *Raft) WaitingForTimeout() {
	for rf.killed() == false {
		if _, isLeader := rf.GetState(); isLeader {
			return
		}
		if !rf.IsLeaderConnected() {
			return
		}
		sleepMilli := GetRandomBetween(0, heartBeatTimeout.Milliseconds()/2)
		time.Sleep(time.Duration(sleepMilli) * time.Millisecond)
	}
}

func GetRandomBetween(min, max int64) int64 {
	diff := max - min
	return rand.Int63n(diff) + min
}

func (rf *Raft) startSendHeartBeat() {
	for rf.killed() == false {
		waitGroup := sync.WaitGroup{}
		replyMap := map[int]*AppendEntriesReply{}
		mapMutex := sync.Mutex{}

		currentTerm, isLeader := rf.GetState()
		if !isLeader {
			log.Printf("in [term %d], [node %d] is not leader now, stop send heart beat, new leader: %d", currentTerm, rf.me, rf.GetCurrentLeader())
			return
		}

		for idx, _ := range rf.peers {
			waitGroup.Add(1)
			go func(idx int) {
				defer waitGroup.Done()
				if idx == rf.me {
					return
				}
				currentReply := replyMap[idx]
				{
					mapMutex.Lock()
					defer mapMutex.Unlock()
					currentReply = &AppendEntriesReply{}
				}

				currentTermWhenSendingHeartBeat, isLeader := rf.GetState()
				// quick fail
				if !isLeader {
					log.Printf("in [term %d -> %d], [node %d] is not leader now, stop send heart beat, new leader: %d", currentTerm, currentTermWhenSendingHeartBeat, rf.me, rf.GetCurrentLeader())
					return
				}
				if rf.killed() {
					log.Printf("killed now, [node %d] stop send heart beat", rf.me)
					return
				}

				log.Printf("in [term %d], [node %d] [StartSendHeartBeat] send heart beat from leader: %d -> %d, isLeader: %v, term: %d", currentTermWhenSendingHeartBeat, rf.me, rf.me, idx, isLeader, currentTermWhenSendingHeartBeat)
				args := &AppendEntriesArgs{}
				args.Term = currentTermWhenSendingHeartBeat
				args.LeaderId = rf.me
				notTimeout, _ := RunJobWithTimeLimit(rpcTimeout.Milliseconds(), func() bool {
					return rf.sendHeartBeat(idx, args, currentReply)
				})
				cCurrentTerm := rf.GetCurrentTerm()
				if !notTimeout {
					log.Printf("in [term %d], [node %d] send heart beat to node %d timeout", cCurrentTerm, rf.me, idx)
				}

			} (idx)
		}
		waitGroup.Wait()

		if rf.GetCurrentLeader() != rf.me {
			log.Printf("in [term %d], [node %d] is not leader now, stop send heart beat, new leader: %d", rf.GetCurrentTerm(), rf.me, rf.leader)
			return
		}

		cCurrentTerm := rf.GetCurrentTerm()
		for idx, reply := range replyMap {
			mapMutex.Lock()
			if reply.Term > cCurrentTerm {
				log.Printf("in [term %d], [node %d] send heart beat met higher term from node %d, back to follower", cCurrentTerm, rf.me, idx)
				rf.SetAsFollower(reply.Term, -1)
			}
			mapMutex.Unlock()
		}
		time.Sleep(200 * time.Millisecond)
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
//	rf.roleChangeMutex.Lock()
//	defer rf.roleChangeMutex.Unlock()
//	rf.UpdateTerm(term)
//}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm, _ := rf.GetState()
	if len(args.Entries) == 0 {
		if currentTerm <= args.Term {
			// todo load lock
			rf.SetAsFollower(args.Term, args.LeaderId)
			//rf.heartBeatMutex.Lock()
			//defer rf.heartBeatMutex.Unlock()
			rf.UpdatePing()
			reply.Term = currentTerm
			reply.Success = true
			log.Printf("in [term %d], [node %d] [AppendEntries received] heart beat, %d -> %d, current term: %d, next term: %d", currentTerm, rf.me, rf.leader, rf.me, currentTerm, args.Term)
			rf.UpdateTerm(args.Term)
			return
		}
		log.Printf("in [term %d], [node %d] reject heart beat, %d -> %d, current term: %d, next term: %d", currentTerm, rf.me, args.LeaderId, rf.me, currentTerm, args.Term)
	}

	reply.Term = currentTerm
	reply.Success = false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.voteMutex.Lock()
	defer rf.voteMutex.Unlock()
	currentTerm, _ := rf.GetState()
	log.Printf("in [term %d], [node %d] [RequestVote received] %v", currentTerm, rf.me, args)
	reply.Term = currentTerm
	if args.Term < currentTerm {
		reply.VoteGranted = false
		log.Printf("in [term %d], [node %d] reject voted for %d, curretn term %d > vote %d", currentTerm, rf.me, args.CandidateId, currentTerm, args.Term)
		return
	}

	//higher one
	if args.Term > currentTerm {
		log.Printf("in [term %d -> term %d], [node %d] voted for %d, term: %d", currentTerm, args.Term, rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.UpdateTerm(args.Term)
		rf.UpdatePing()
		rf.UpdateVotedFor(args.CandidateId)
	}

	// same one
	votedFor := rf.GetCurrentVotedFor()
	if votedFor != -1 {
		reply.VoteGranted = false
		log.Printf("in [term %d], [node %d] reject voted for %d, already voted for %d", currentTerm, rf.me, args.CandidateId, votedFor)
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
	rf.pingMutex.Lock()
	defer rf.pingMutex.Unlock()
	return time.Now().Sub(rf.lastPing)
}

func (rf *Raft) UpdatePing() {
	rf.pingMutex.Lock()
	defer rf.pingMutex.Unlock()
	rf.lastPing = time.Now()
}

func AtomicLoadInt(addr *int32) int{
	return int(atomic.LoadInt32(addr))
}

func AtomicStoreInt(addr *int32, val int) {
	atomic.StoreInt32(addr, int32(val))
}