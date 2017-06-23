package main

import (
	"sync"

	"fmt"
)

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.Mutex

	cfg *Config

	meta Meta

	done chan struct{}
}

func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.meta = NewLocalMeta(cfg.Meta)

	syncer.done = make(chan struct{})

	return syncer
}

func (s *Syncer) Start() error {

	s.done <- struct{}{}

	return nil
}

// Close close syncer.
func (s *Syncer) Close() {

	<-s.done

	fmt.Println("close syncer")
}
