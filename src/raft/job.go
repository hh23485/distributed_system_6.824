package raft

import (
	"context"
	"sync"
	"time"
)

//type JobContext interface {
	//IsSucc() bool
	//GetResult() interface{}
//}

type VoteJobContext struct {
	args        *RequestVoteArgs
	voteResults map[int]*RequestVoteReply
	succ        bool
	MaxTerm     int
	mutex		*sync.Mutex
}
//
//func (v VoteJobContext) IsSucc() bool {
//	return v.succ
//}

func RunJobWithTimeLimit(timeoutMilli int64, job func() bool) bool {
	timeout := time.Duration(timeoutMilli) * time.Millisecond
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	resultChan := make(chan bool)

	go func(ctx context.Context) {
		resultChan <- job()
	}(ctx)

	select {
	case <-resultChan:
		return true
	case <-ctx.Done():
		return false
	}
}
