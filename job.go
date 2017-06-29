package main

import (
	"github.com/siddontang/go-mysql/mysql"
)

type opType byte

const (
	insert = iota + 1
	update
	del
	ddl
	xid
	flush
)

type job struct {
	tp      opType
	sql     string
	args    []interface{}
	key     string
	retry   bool
	pos     mysql.Position
	gtidSet GTIDSet
}

func newJob(tp opType, sql string, args []interface{}, key string, retry bool, pos mysql.Position, gtidSet GTIDSet) *job {
	return &job{tp: tp, sql: sql, args: args, key: key, retry: retry, pos: pos, gtidSet: gtidSet}
}

func newXIDJob(pos mysql.Position, gtidSet GTIDSet) *job {
	return &job{tp: xid, pos: pos, gtidSet: gtidSet}
}

func newFlushJob() *job {
	return &job{tp: flush}
}
