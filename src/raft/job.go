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
	args     *RequestVoteArgs
	replyMap map[int]*RequestVoteReply
	MaxTerm  int32
	mutex    *sync.Mutex
}

type AppendLogContext struct {
	replyMap map[int]*AppendEntriesReply
	mutex    *sync.Mutex
}

func (ctx *VoteJobContext) SetReplyWithLock(idx int, reply *RequestVoteReply) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.replyMap[idx] = reply
}

func (ctx *VoteJobContext) GetReply(idx int) *RequestVoteReply {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.replyMap[idx]
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
