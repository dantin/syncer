package config

import (
	"flag"
	"fmt"
	"strings"

	"github.com/dantin/syncer/db"
)

// Config is the configuration.
type Config struct {
	*flag.FlagSet `json:"-"`

	LogLevel  string `toml:"log-level" json:"log-level"`
	LogFile   string `toml:"log-file" json:"log-file"`
	LogRotate string `toml:"log-rotate" json:"log-rotate"`

	StatusAddr string `toml:"status-addr" json:"status-addr"`

	ServerID int    `toml:"server-id" json:"server-id"`
	Meta     string `toml:"meta" json:"meta"`

	WorkerCount int `toml:"worker-count" json:"worker-count"`
	Batch       int `toml:"batch" json:"batch"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-do-table
	DoTables []*TableName `toml:"replicate-do-table" json:"replicate-do-table"`
	DoDBs    []string     `toml:"replicate-do-db" json:"replicate-do-db"`

	// Ref: http://dev.mysql.com/doc/refman/5.7/en/replication-options-slave.html#option_mysqld_replicate-ignore-db
	IgnoreTables []*TableName `toml:"replicate-ignore-table" json:"replicate-ignore-table"`
	IgnoreDBs    []string     `toml:"replicate-ignore-db" json:"replicate-ignore-db"`

	SkipSQLs []string `toml:"skip-sqls" json:"skip-sqls"`

	RouteRules []*RouteRule `toml:"route-rules" json:"route-rules"`

	From DBConfig `toml:"from" json:"from"`
	To   DBConfig `toml:"to" json:"to"`

	EnableGTID  bool `toml:"enable-gtid" json:"enable-gtid"`
	AutoFixGTID bool `toml:"auto-fix-gtid" json:"auto-fix-gtid"`

	configFile   string
	printVersion bool
}

func (c Config) String() string {
	doTables := make([]string, len(c.DoTables))
	ignoreTables := make([]string, len(c.IgnoreTables))
	routeRules := make([]string, len(c.RouteRules))
	for i, table := range c.DoTables {
		doTables[i] = fmt.Sprintf("%+v", *table)
	}
	for i, table := range c.IgnoreTables {
		ignoreTables[i] = fmt.Sprintf("%+v", *table)
	}
	for i, rule := range c.RouteRules {
		routeRules[i] = fmt.Sprintf("%+v", *rule)
	}
	doTablesStr := fmt.Sprintf("[%s]", strings.Join(doTables, ";"))
	ingnoreTablesStr := fmt.Sprintf("[%s]", strings.Join(ignoreTables, ";"))
	routeRulesStr := fmt.Sprintf("[%s]", strings.Join(routeRules, ";"))
	return fmt.Sprintf(`log-level:%s log-file:%s log-rotate:%s status-addr:%s `+
		`server-id:%d worker-count:%d batch:%d meta-file:%s `+
		`do-tables:%v do-dbs:%v ignore-tables:%v ignore-dbs:%v `+
		`from:%s to:%s skip-sqls:%v route-rules:%v enable-gtid:%v safe-mode:%v`,
		c.LogLevel, c.LogFile, c.LogRotate, c.StatusAddr,
		c.ServerID, c.WorkerCount, c.Batch, c.Meta,
		doTablesStr, c.DoDBs, ingnoreTablesStr, c.IgnoreDBs,
		c.From, c.To, c.SkipSQLs, routeRulesStr, c.EnableGTID, db.SafeMode)
}

// DBConfig is the DB configuration.
type DBConfig struct {
	Host     string `toml:"host" json:"host"`
	User     string `toml:"user" json:"user"`
	Password string `toml:"password" json:"password"`
	Port     int    `toml:"port" json:"port"`
}

// TableName is the Table configuration
// slave restrict replication to a given table
type TableName struct {
	Schema string `toml:"db-name" json:"db-name"`
	Name   string `toml:"tbl-name" json:"tbl-name"`
}

// RouteRule is route rule that syncing
// schema/table to specified schema/table
type RouteRule struct {
	PatternSchema string `toml:"pattern-schema" json:"pattern-schema"`
	PatternTable  string `toml:"pattern-table" json:"pattern-table"`
	TargetSchema  string `toml:"target-schema" json:"target-schema"`
	TargetTable  string `toml:"target-table" json:"target-table"`
}

func NewConfig() *Config {
	cfg := &Config{}

	return cfg
}
