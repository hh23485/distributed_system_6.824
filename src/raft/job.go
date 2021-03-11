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

type ReplyMap struct {
	replyMap map[int]interface{}
	mutex    *sync.Mutex
}

func (ctx *ReplyMap) SetReplyWithLock(idx int, reply interface{}) {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	ctx.replyMap[idx] = reply
}

func (ctx *ReplyMap) GetReply(idx int) interface{} {
	ctx.mutex.Lock()
	defer ctx.mutex.Unlock()
	return ctx.replyMap[idx]
}

type VoteJobContext struct {
	args     *RequestVoteArgs
	replyMap map[int]*RequestVoteReply
	MaxTerm  int32
	mutex    *sync.Mutex
}

type AppendEntriesJobContext struct {
	ReplyMap
	MaxTerm int32
	args *AppendEntriesArgs
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
