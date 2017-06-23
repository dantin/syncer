package main

import "github.com/siddontang/go-mysql/mysql"

// GTIDSet wraps mysql.MysqlGTIDSet
type GTIDSet struct {
	*mysql.MysqlGTIDSet
}
