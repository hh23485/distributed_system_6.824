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
	MaxTerm     int32
	mutex		*sync.Mutex
}

func RunJobWithTimeLimit(timeoutMilli int64, job func() bool) (bool, bool) {
	timeout := time.Duration(timeoutMilli) * time.Millisecond
	ctx, _ := context.WithTimeout(context.Background(), timeout)
	jobFinishChan := make(chan bool)

	var jobResult bool
	go func(ctx context.Context, ret *bool) {
		jobResult = job()
		jobFinishChan <- true
	}(ctx, &jobResult)

	select {
	case <-jobFinishChan:
		return true, jobResult
	case <-ctx.Done():
		return false, false
	}
}
