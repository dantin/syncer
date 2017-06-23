package main

import (
	"database/sql"

	"github.com/juju/errors"
	"github.com/ngaut/log"
)

var safeMode bool

func closeDBs(dbs ...*sql.DB) {

	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Errorf("close db failed: %v", err)
		}
	}
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}
