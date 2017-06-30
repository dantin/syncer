package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	_ "github.com/go-sql-driver/mysql"
	"github.com/ngaut/log"
	. "github.com/pingcap/check"
	gmysql "github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"golang.org/x/net/context"
)

var _ = Suite(&testSyncerSuite{})

func TestSuite(t *testing.T) {
	TestingT(t)
}

type testSyncerSuite struct {
	db       *sql.DB
	syncer   *replication.BinlogSyncer
	streamer *replication.BinlogStreamer
	cfg      *Config
}

func (s *testSyncerSuite) SetUpSuite(c *C) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port, _ := strconv.Atoi(os.Getenv("MYSQL_PORT"))
	if port == 0 {
		port = 3306
	}
	user := os.Getenv("MYSQL_USER")
	if user == "" {
		user = "root"
	}
	pswd := os.Getenv("MYSQL_PSWD")

	s.cfg = &Config{
		From: DBConfig{
			Host:     host,
			User:     user,
			Password: pswd,
			Port:     port,
		},
		ServerID: 101,
	}

	var err error
	dbAddr := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", s.cfg.From.User, s.cfg.From.Password, s.cfg.From.Host, s.cfg.From.Port)
	s.db, err = sql.Open("mysql", dbAddr)
	if err != nil {
		log.Fatal(err)
	}

	s.syncer = replication.NewBinlogSyncer(&replication.BinlogSyncerConfig{
		ServerID: uint32(s.cfg.ServerID),
		Flavor:   "mysql",
		Host:     s.cfg.From.Host,
		Port:     uint16(s.cfg.From.Port),
		User:     s.cfg.From.User,
		Password: s.cfg.From.Password,
	})
	s.resetMaster()
	s.streamer, err = s.syncer.StartSync(gmysql.Position{Name: "", Pos: 4})
	if err != nil {
		log.Fatal(err)
	}

	s.db.Exec("SET GLOBAL binlog_format = 'ROW';")
}

func (s *testSyncerSuite) TearDownSuite(c *C) {
	s.db.Close()
}

func (s *testSyncerSuite) resetMaster() {
	s.db.Exec("reset master")
}

func (s *testSyncerSuite) clearRules() {
	s.cfg.DoDBs = nil
	s.cfg.DoTables = nil
	s.cfg.IgnoreDBs = nil
	s.cfg.IgnoreTables = nil
}

func (s *testSyncerSuite) TestSelectDB(c *C) {
	s.cfg.DoDBs = []string{"~^b.*", "s1", "stest"}
	sqls := []string{
		"create database s1",
		"drop database s1",
		"create database s2",
		"drop database s2",
		"create database btest",
		"drop database btest",
		"create database b1",
		"drop database b1",
		"create database stest",
		"drop database stest",
		"create database st",
		"drop database st",
	}
	res := []bool{false, false, true, true, false, false, false, false, false, false, true, true}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genRegexMap()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		c.Assert(err, IsNil)
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		if syncer.skipQueryEvent(sql) {
			continue
		}

		tableNames, err := syncer.fetchDDLTableNames(sql, string(ev.Schema))
		c.Assert(err, IsNil)
		r := syncer.skipQueryDDL(sql, tableNames[1])
		c.Assert(r, Equals, res[i])
		i++
	}
	s.clearRules()
}

func (s *testSyncerSuite) TestSelectTable(c *C) {
	s.cfg.DoDBs = []string{"t2"}
	s.cfg.DoTables = []*TableName{
		{Schema: "stest", Name: "log"},
		{Schema: "stest", Name: "~^t.*"},
	}
	sqls := []string{
		"create database s1",
		"create table s1.log(id int)",
		"drop database s1",

		"create table mysql.test(id int)",
		"drop table mysql.test",
		"create database stest",
		"create table stest.log(id int)",
		"create table stest.t(id int)",
		"create table stest.log2(id int)",
		"insert into stest.t(id) values (10)",
		"insert into stest.log(id) values (10)",
		"insert into stest.log2(id) values (10)",
		"drop table stest.log,stest.t,stest.log2",
		"drop database stest",

		"create database t2",
		"create table t2.log(id int)",
		"create table t2.log1(id int)",
		"drop table t2.log",
		"drop database t2",
	}
	res := [][]bool{
		{true},
		{true},
		{true},

		{true},
		{true},
		{false},
		{false},
		{false},
		{true},
		{false},
		{false},
		{true},
		{false, false, true},
		{false},

		{false},
		{false},
		{false},
		{false},
		{false},
	}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genRegexMap()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)
			if syncer.skipQueryEvent(query) {
				continue
			}

			querys, err := resolveDDLSQL(query)
			if err != nil {
				log.Fatalf("ResolveDDlSQL failed %v", err)
			}
			if len(querys) == 0 {
				continue
			}
			log.Debugf("querys:%+v", querys)
			for j, q := range querys {
				tableNames, err := syncer.fetchDDLTableNames(q, string(ev.Schema))
				c.Assert(err, IsNil)
				r := syncer.skipQueryDDL(q, tableNames[1])
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r := syncer.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table))
			c.Assert(r, Equals, res[i][0])

		default:
			continue
		}

		i++

	}
	s.clearRules()
}

func (s *testSyncerSuite) TestIgnoreDB(c *C) {
	s.cfg.IgnoreDBs = []string{"~^b.*", "s1", "stest"}
	sqls := []string{
		"create database s1",
		"drop database s1",
		"create database s2",
		"drop database s2",
		"create database btest",
		"drop database btest",
		"create database b1",
		"drop database b1",
		"create database stest",
		"drop database stest",
		"create database st",
		"drop database st",
	}
	res := []bool{true, true, false, false, true, true, true, true, true, true, false, false}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genRegexMap()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}

		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		ev, ok := e.Event.(*replication.QueryEvent)
		if !ok {
			continue
		}
		sql := string(ev.Query)
		if syncer.skipQueryEvent(sql) {
			continue
		}
		tableNames, err := syncer.fetchDDLTableNames(sql, string(ev.Schema))
		c.Assert(err, IsNil)
		r := syncer.skipQueryDDL(sql, tableNames[1])
		c.Assert(r, Equals, res[i])
		i++
	}
	s.clearRules()
}

func (s *testSyncerSuite) TestIgnoreTable(c *C) {
	s.cfg.IgnoreDBs = []string{"t2"}
	s.cfg.IgnoreTables = []*TableName{
		{Schema: "stest", Name: "log"},
		{Schema: "stest", Name: "~^t.*"},
	}
	sqls := []string{
		"create database s1",
		"create table s1.log(id int)",
		"drop database s1",

		"create table mysql.test(id int)",
		"drop table mysql.test",
		"create database stest",
		"create table stest.log(id int)",
		"create table stest.t(id int)",
		"create table stest.log2(id int)",
		"insert into stest.t(id) values (10)",
		"insert into stest.log(id) values (10)",
		"insert into stest.log2(id) values (10)",
		"drop table stest.log,stest.t,stest.log2",
		"drop database stest",

		"create database t2",
		"create table t2.log(id int)",
		"create table t2.log1(id int)",
		"drop table t2.log",
		"drop database t2",
	}
	res := [][]bool{
		{false},
		{false},
		{false},

		{true},
		{true},
		{true},
		{true},
		{true},
		{false},
		{true},
		{true},
		{false},
		{true, true, false},
		{true},

		{true},
		{true},
		{true},
		{true},
		{true},
	}

	for _, sql := range sqls {
		s.db.Exec(sql)
	}

	syncer := NewSyncer(s.cfg)
	syncer.genRouter()
	syncer.genRegexMap()
	i := 0
	for {
		if i >= len(sqls) {
			break
		}
		e, err := s.streamer.GetEvent(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		switch ev := e.Event.(type) {
		case *replication.QueryEvent:
			query := string(ev.Query)
			if syncer.skipQueryEvent(query) {
				continue
			}

			querys, err := resolveDDLSQL(query)
			if err != nil {
				log.Fatalf("ResolveDDlSQL failed %v", err)
			}
			if len(querys) == 0 {
				continue
			}

			for j, q := range querys {
				tableNames, err := syncer.fetchDDLTableNames(q, string(ev.Schema))
				c.Assert(err, IsNil)
				r := syncer.skipQueryDDL(q, tableNames[1])
				c.Assert(r, Equals, res[i][j])
			}
		case *replication.RowsEvent:
			r := syncer.skipRowEvent(string(ev.Table.Schema), string(ev.Table.Table))
			c.Assert(r, Equals, res[i][0])

		default:
			continue
		}

		i++

	}
	s.clearRules()
}

func (s *testSyncerSuite) TestQueryEvent(c *C) {

	sqls := []string{
		"CREATE DEFINER=`ab`@`%` TRIGGER `records` AFTER INSERT ON `other_records` FOR EACH ROW BEGIN   set @ret = http_get(concat('http://test/id/',new.id));",
	}
	syncer := NewSyncer(s.cfg)

	res := []bool{true}
	for i, sql := range sqls {
		r := syncer.skipQueryEvent(sql)
		c.Assert(r, Equals, res[i])
	}
}
