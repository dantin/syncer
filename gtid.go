package main

import (
	"github.com/siddontang/go-mysql/mysql"
	"github.com/juju/errors"
)

// GTIDSet wraps mysql.MysqlGTIDSet
type GTIDSet struct {
	*mysql.MysqlGTIDSet
}

func parseGTIDSet(gtidStr string) (GTIDSet, error) {
	gs, err := mysql.ParseMysqlGTIDSet(gtidStr)
	if err != nil {
		return GTIDSet{}, errors.Trace(err)
	}

	return GTIDSet{gs.(*mysql.MysqlGTIDSet)}, nil
}
