package raft

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

var Role_Follower int = 0
var Role_Candidate int = 1
var Role_Leader int = 2

var heartBeatTimeout time.Duration = time.Millisecond * 1500

func (rf *Raft) SetAsLeader() bool {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	rf.role = Role_Leader
	rf.votedFor = -1
	rf.leader = rf.me
	log.Printf("in term %d, node %d begin to be a leader", rf.currentTerm, rf.me)
	return true
}

func (rf *Raft) SetAsCandidate() bool {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	if rf.votedFor != -1 {
		log.Printf("")
		return false
	}

	rf.role = Role_Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	log.Printf("in term %d, %d begin to be a candidate", rf.currentTerm, rf.me)
	return true
}

func (rf *Raft) SetAsFollower(term int, leader int) {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()

	rf.role = Role_Follower
	rf.votedFor = -1
	rf.UpdateTerm(term)
	rf.leader = leader
	log.Printf("in term %d, %d begin to be a follower, current leader is %d", rf.currentTerm, rf.me, rf.leader)
}

func (rf *Raft) GetStateWithoutLock() (int, bool) {
	return rf.currentTerm, rf.leader == rf.me
}

func (rf *Raft) DoElection() bool {
	var maxMilli int64 = 500
	var minMilli int64 = 100
	electionTimeoutMilli := GetRandomBetween(minMilli, maxMilli)
	time.Sleep(time.Duration(electionTimeoutMilli) * time.Millisecond)

	rf.SetAsCandidate()

	arg := RequestVoteArgs{
		Term:        rf.currentTerm,
		CandidateId: rf.me,
	}

	voteJobCtx := &VoteJobContext{
		&arg,
		map[int]*RequestVoteReply{},
		false,
		0,
		&sync.Mutex{},
	}

	notTimeout := RunJobWithTimeLimit(electionTimeoutMilli, func() bool {
		log.Printf("in term %d, node %d start a election", rf.currentTerm, rf.me)
		reqContext := voteJobCtx
		waitGroup := sync.WaitGroup{}
		replyMap := reqContext.voteResults

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
					//reqContext.mutex.Lock()
					//defer reqContext.mutex.Unlock()
					reply.Term = arg.Term
					reply.VoteGranted = true
					return
				}
				ok := rf.sendRequestVote(idx, reqContext.args, reply)
				if !ok {
					//reqContext.mutex.Lock()
					//defer reqContext.mutex.Unlock()
					reply = nil
					log.Printf("[Async] in term %d, sendRequestVote from candidate %d -> peer %d, failed", rf.me, arg.Term, idx)
				}
			}(idx)
		}
		waitGroup.Wait()

		// check result and set to context
		totalCount := len(rf.peers)
		granted := 0
		maxTerm := 0
		maxTermPeer := -1
		for idx, reply := range replyMap {
			if reply == nil {
				return false
			}
			if reply.VoteGranted {
				granted++
			}
			if reply.Term > maxTerm {
				maxTerm = reply.Term
				maxTermPeer = idx
			}
		}
		reqContext.MaxTerm = maxTerm

		// check if meet higher
		if maxTerm > arg.Term {
			log.Printf("[Async] in term %d, candidate %d meet higher term %d from peer %d", arg.Term, rf.me, maxTerm, maxTermPeer)
			return false
		}

		reqContext.succ = granted > (totalCount / 2)
		win := "win"
		if !reqContext.succ {
			win = "lose"
		}
		log.Printf("[Async] %s the election for candidate %d in term %d, win %d/%d", win, rf.me, arg.Term, granted, totalCount)
		return reqContext.succ
	})

	currentTerm, _ := rf.GetState()
	if !notTimeout {
		log.Printf("in term %d, election for candidate %d, timeout", arg.Term, rf.me)
	} else if arg.Term < currentTerm {
		log.Printf("in term %d, node %d's current term changed to %d during election", arg.Term, rf.me, currentTerm)
	} else {
		//	valid
		if voteJobCtx.succ {
			if rf.SetAsLeader() {
				log.Printf("in term %d, node %d win the election", arg.Term, rf.me)
				return true
			}
			log.Printf("in term %d, node %d set as leader failed", arg.Term, rf.me)
		} else {
			log.Printf("in term %d, node %d lose the election for candidate", arg.Term, rf.me)
		}
	}
	return false
}

func (rf *Raft) IsLeaderConnected() bool {
	if rf.leader == rf.me {
		return true
	}

	if time.Now().Sub(rf.lastPing) > heartBeatTimeout {
		return true
	}
	return false
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
		for idx, _ := range rf.peers {
			if idx == rf.me {
				continue
			}
			currentTerm, isLeader := rf.GetState()
			if !isLeader {
				log.Printf("not leader now, stop send heart beat, leader: %d, new leader: %d", rf.me, rf.leader)
				return
			}
			log.Printf("[async] send heart beat from leader: %d, to peer: %d, isLeader: %v, term: %d", rf.me, idx, isLeader, currentTerm)
			args := &AppendEntriesArgs{}
			args.Term = currentTerm
			args.LeaderId = rf.me
			reply := &AppendEntriesReply{}
			rf.sendHeartBeat(idx, args, reply)
			if reply.Term > currentTerm {
				log.Printf("send heart beat met higher term, back to follower")
				rf.SetAsFollower(reply.Term, -1)
			}
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartBeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) UpdateTerm(term int) {
	rf.currentTerm = term
}

func (rf *Raft) UpdateTermWithLock(term int) {
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()
	rf.UpdateTerm(term)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	currentTerm, _ := rf.GetState()
	if len(args.Entries) == 0 {
		if currentTerm <= args.Term {
			rf.SetAsFollower(args.Term, args.LeaderId)
			//rf.heartBeatMutex.Lock()
			//defer rf.heartBeatMutex.Unlock()
			rf.lastPing = time.Now()
			reply.Term = currentTerm
			reply.Success = true
			log.Printf("in term %d, node %d received heart beat, %d -> %d, current term: %d, next term: %d", currentTerm, rf.me, rf.leader, rf.me, currentTerm, args.Term)
			rf.UpdateTermWithLock(args.Term)
			return
		}
		log.Printf("in term %d, node %d, reject heart beat, %d -> %d, current term: %d, next term: %d", currentTerm, rf.me, args.LeaderId, rf.me, currentTerm, args.Term)
	}

	reply.Term = currentTerm
	reply.Success = false
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	log.Printf("peer %d received %v", rf.me, args)
	rf.roleChangeMutex.Lock()
	defer rf.roleChangeMutex.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		log.Printf("follower %d reject voted for %d, curretn term %d > vote %d", rf.me, args.CandidateId, rf.currentTerm, args.Term)
		return
	}
	if rf.votedFor != -1 {
		reply.VoteGranted = false
		log.Printf("follower %d reject voted for %d, already voted for %d", rf.me, args.CandidateId, rf.votedFor)
		return
	}

	if args.Term > rf.currentTerm {
		log.Printf("follower %d voted for %d, term: %d", rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	}
}

func Max(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}
