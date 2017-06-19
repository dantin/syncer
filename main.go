package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/dantin/syncer/config"
	"github.com/juju/errors"
	"github.com/ngaut/log"
)

func main() {
	cfg := config.NewConfig()
	err := cfg.Parse(os.Args[1:])
	switch errors.Cause(err) {
	case nil:
	case flag.ErrHelp:
		os.Exit(0)
	default:
		log.Errorf("parse cmd flags err %s", err)
		os.Exit(2)
	}

	log.SetLevelByString(cfg.LogLevel)

	fmt.Println(cfg)
}
