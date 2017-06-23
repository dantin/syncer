package main

import (
	"sync"
	"time"

	"github.com/siddontang/go-mysql/mysql"
)

// Meta is the binlog meta information from sync source.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type Meta interface {
	// Load loads meta information.
	Load() error

	// Save saves meta information.
	Save(pos mysql.Position, gtid GTIDSet, force bool) error

	// Flush write meta information.
	Flush() error

	// Check checks whether we should save meta.
	Check() bool

	// Pos gets position information.
	Pos() mysql.Position

	// GTID() returns gtid information.
	GTID() (GTIDSet, error)
}

type LocalMeta struct {
	sync.RWMutex

	filename   string
	saveTime   time.Time

	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
	BinLogGTID string `toml:"binlog-gtid" json:"binlog-gtid"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(filename string) *LocalMeta {
	return &LocalMeta{filename: filename, BinLogPos: 4}
}
