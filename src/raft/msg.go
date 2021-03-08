package raft

import "fmt"

type RequestVoteArgs struct {
	Term        int
	CandidateId int
}

func (r RequestVoteArgs) String() string {
	return fmt.Sprintf("RequestVoteArgs: [Term: %d], from: %d ", r.Term, r.CandidateId)
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A, 2B).
	Term         int
	LeaderId     int
	Entries      []*string
	LeaderCommit int64
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term    int
	Success bool
}