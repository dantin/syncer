package main

import (
	"database/sql"
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb-enterprise-tools/pkg/tableroute"
	"github.com/satori/go.uuid"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
	"golang.org/x/net/context"
)

var (
	maxRetryCount = 100

	retryTimeout = 3 * time.Second
	waitTime     = 10 * time.Millisecond
	maxWaitTime  = 3 * time.Second
	eventTimeout = 1 * time.Hour
	statusTime   = 30 * time.Second

	maxDMLConnectionTimeout = "3s"
	maxDDLConnectionTimeout = "3h"
)

// Syncer can sync your MySQL data to another MySQL database.
type Syncer struct {
	sync.Mutex

	cfg *Config

	meta Meta

	syncer *replication.BinlogSyncer

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables map[string]*table

	fromDB *sql.DB
	toDBs  []*sql.DB
	ddlDB  *sql.DB

	done chan struct{}
	jobs []chan *job
	c    *causality

	tableRouter route.TableRouter

	closed sync2.AtomicBool

	start    time.Time
	lastTime time.Time

	lastCount sync2.AtomicInt64
	count     sync2.AtomicInt64

	ctx    context.Context
	cancel context.CancelFunc

	patternMap map[string]*regexp.Regexp

	lackOfReplClientPrivilege bool
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.meta = NewLocalMeta(cfg.Meta)
	syncer.closed.Set(false)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.done = make(chan struct{})
	syncer.jobs = newJobChans(cfg.WorkerCount + 1)
	syncer.tables = make(map[string]*table)
	syncer.c = newCausality()
	syncer.ctx, syncer.cancel = context.WithCancel(context.Background())
	syncer.patternMap = make(map[string]*regexp.Regexp)
	return syncer
}

func newJobChans(count int) []chan *job {
	jobs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobs = append(jobs, make(chan *job, 1000))
	}

	return jobs
}

func closeJobChans(jobs []chan *job) {
	for _, ch := range jobs {
		close(ch)
	}
}

// Start starts syncer.
func (s *Syncer) Start() error {
	err := s.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	s.wg.Add(1)

	err = s.run()
	if err != nil {
		return errors.Trace(err)
	}

	s.done <- struct{}{}

	return nil
}

func (s *Syncer) checkBinlogFormat() error {
	rows, err := s.fromDB.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)

		if err != nil {
			return errors.Trace(err)
		}

		if variable == "binlog_format" && value != "ROW" {
			log.Fatalf("binlog_format is not 'ROW': %v", value)
		}

	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

// todo: use "github.com/siddontang/go-mysql/mysql".MysqlGTIDSet to represent gtid
// assume that reset master before switching to new master, and only the new master would write
func (s *Syncer) retrySyncGTIDs() error {
	log.Info("start retry sync gtid")

	gs, err := s.meta.GTID()
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("old gtid set %v", gs)
	_, newGS, err := s.getMasterStatus()
	if err != nil {
		return errors.Trace(err)
	}
	// find master UUID and ignore it
	masterUUID, err := s.getServerUUID()
	if err != nil {
		return errors.Trace(err)
	}
	log.Infof("new master gtid set %v, master uuid %s", newGS, masterUUID)
	// remove master gtid from currentGTIDs
	newGS.delete(masterUUID)
	// remove useless gtid from
	for uuid := range gs.all() {
		if ok := newGS.contain(uuid); !ok {
			gs.delete(uuid)
		}
	}
	// add unknow gtid
	for uuid, uuidSet := range newGS.all() {
		if ok := gs.contain(uuid); !ok {
			gs.AddSet(uuidSet)
		}
	}
	// force to save in meta file
	s.meta.Save(s.meta.Pos(), gs, true)

	return nil
}

func (s *Syncer) getServerUUID() (string, error) {
	return getServerUUID(s.fromDB)
}

func (s *Syncer) getMasterStatus() (mysql.Position, GTIDSet, error) {
	return getMasterStatus(s.fromDB)
}

func (s *Syncer) clearTables() {
	s.tables = make(map[string]*table)
}

func (s *Syncer) getTableFromDB(db *sql.DB, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name
	table.indexColumns = make(map[string][]*column)

	err := s.getTableColumns(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = s.getTableIndex(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func (s *Syncer) getTableColumns(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW COLUMNS FROM `%s`.`%s`", table.schema, table.name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	idx := 0
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		column := &column{}
		column.idx = idx
		column.name = string(data[0])

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(data[1])), "unsigned") {
			column.unsigned = true
		}

		table.columns = append(table.columns, column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func (s *Syncer) getTableIndex(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}

	query := fmt.Sprintf("SHOW INDEX FROM `%s`.`%s`", table.schema, table.name)
	rows, err := querySQL(db, query)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	var columns = make(map[string][]string)
	for rows.Next() {
		data := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &data[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		nonUnique := string(data[1])
		if nonUnique == "0" {
			keyName := string(data[2])
			columns[keyName] = append(columns[keyName], string(data[4]))
		}
	}
	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.indexColumns = findColumns(table.columns, columns)
	return nil
}

func (s *Syncer) getTable(schema string, table string) (*table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, nil
	}

	db := s.toDBs[len(s.toDBs)-1]
	t, err := s.getTableFromDB(db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.tables[key] = t
	return t, nil
}

func (s *Syncer) addCount(tp opType, n int64) {
	switch tp {
	case insert:
		sqlJobsTotal.WithLabelValues("insert").Add(float64(n))
	case update:
		sqlJobsTotal.WithLabelValues("update").Add(float64(n))
	case del:
		sqlJobsTotal.WithLabelValues("del").Add(float64(n))
	case ddl:
		sqlJobsTotal.WithLabelValues("ddl").Add(float64(n))
	case xid:
		// skip xid jobs
	default:
		panic("unreachable")
	}

	s.count.Add(n)
}

func (s *Syncer) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.meta.Check() {
		return true
	}

	return false
}

func (s *Syncer) addJob(job *job) error {
	switch job.tp {
	case xid:
		return s.meta.Save(job.pos, job.gtidSet, false)
	case ddl:
		// while meet ddl, we should wait all dmls finished firstly
		s.jobWg.Wait()
	case flush:
		s.jobWg.Wait()
		err := s.meta.Flush()
		return errors.Trace(err)
	}

	if len(job.sql) > 0 {
		s.jobWg.Add(1)
		if job.tp == ddl {
			s.jobs[s.cfg.WorkerCount] <- job
		} else {
			idx := int(genHashKey(job.key)) % s.cfg.WorkerCount
			s.jobs[idx] <- job
		}
	}

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()
		s.c.reset()
	}

	err := s.meta.Save(job.pos, job.gtidSet, wait)
	return errors.Trace(err)
}

func (s *Syncer) sync(db *sql.DB, jobChan chan *job) {
	defer s.wg.Done()

	idx := 0
	count := s.cfg.Batch
	sqls := make([]string, 0, count)
	args := make([][]interface{}, 0, count)
	lastSyncTime := time.Now()
	tpCnt := make(map[opType]int64)

	clearF := func() {
		for i := 0; i < idx; i++ {
			s.jobWg.Done()
		}

		idx = 0
		sqls = sqls[0:0]
		args = args[0:0]
		lastSyncTime = time.Now()
		for tpName, v := range tpCnt {
			s.addCount(tpName, v)
			tpCnt[tpName] = 0
		}
	}

	var err error
	for {
		select {
		case job, ok := <-jobChan:
			if !ok {
				return
			}
			idx++

			if job.tp == ddl {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}

				err = executeSQL(db, []string{job.sql}, [][]interface{}{job.args}, false)
				if err != nil {
					if !ignoreDDLError(err) {
						log.Fatalf(errors.ErrorStack(err))
					} else {
						log.Warnf("[ignore ddl error][sql]%s[args]%v[error]%v", job.sql, job.args, err)
					}
				}

				tpCnt[job.tp]++
				clearF()

			} else {
				sqls = append(sqls, job.sql)
				args = append(args, job.args)
				tpCnt[job.tp]++
			}

			if idx >= count {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

		default:
			now := time.Now()
			if now.Sub(lastSyncTime) >= maxWaitTime {
				err = executeSQL(db, sqls, args, true)
				if err != nil {
					log.Fatalf(errors.ErrorStack(err))
				}
				clearF()
			}

			time.Sleep(waitTime)
		}
	}
}

func (s *Syncer) run() (err error) {
	defer func() {
		if err1 := recover(); err1 != nil {
			log.Errorf("panic. err: %s, stack: %s", err1, debug.Stack())
			err = errors.Errorf("panic error: %v", err1)
		}
		if err1 := s.flushJobs(); err1 != nil {
			log.Errorf("fail to finish all jobs error: %v", err1)
		}
		s.wg.Done()
	}()

	cfg := replication.BinlogSyncerConfig{
		ServerID: uint32(s.cfg.ServerID),
		Flavor:   "mysql",
		Host:     s.cfg.From.Host,
		Port:     uint16(s.cfg.From.Port),
		User:     s.cfg.From.User,
		LogLevel: s.cfg.LogLevel,
		Password: s.cfg.From.Password,
	}

	s.syncer = replication.NewBinlogSyncer(&cfg)

	err = s.createDBs()
	if err != nil {
		return errors.Trace(err)
	}

	err = s.checkBinlogFormat()
	if err != nil {
		return errors.Trace(err)
	}

	// support regex
	s.genRegexMap()
	err = s.genRouter()
	if err != nil {
		return errors.Trace(err)
	}

	streamer, isGTIDMode, err := s.getBinlogStreamer()
	if err != nil {
		return errors.Trace(err)
	}

	s.start = time.Now()
	s.lastTime = s.start

	s.wg.Add(s.cfg.WorkerCount)
	for i := 0; i < s.cfg.WorkerCount; i++ {
		go s.sync(s.toDBs[i], s.jobs[i])
	}
	go s.sync(s.ddlDB, s.jobs[s.cfg.WorkerCount])

	s.wg.Add(1)
	go s.printStatus()

	pos := s.meta.Pos()
	gs, err := s.meta.GTID()
	if err != nil {
		return errors.Trace(err)
	}
	var (
		uuidSet   *mysql.UUIDSet
		tryReSync = true
	)
	for {
		ctx, cancel := context.WithTimeout(s.ctx, eventTimeout)
		e, err := streamer.GetEvent(ctx)
		cancel()

		if err == context.Canceled {
			log.Infof("ready to quit! [%v]", pos)
			return nil
		} else if err == context.DeadlineExceeded {
			log.Info("deadline exceeded.")
			if s.needResync() {
				log.Info("timeout, resync")
				streamer, isGTIDMode, err = s.reopen(&cfg)
			}
			continue
		}

		if err != nil {
			log.Errorf("get binlog error %v", err)
			// try to re-sync in gtid mode
			if tryReSync && isGTIDMode && isBinlogPurgedError(err) && s.cfg.AutoFixGTID {
				time.Sleep(retryTimeout)
				streamer, isGTIDMode, err = s.reSyncBinlog(&cfg)
				if err != nil {
					return errors.Trace(err)
				}
				tryReSync = false
				continue
			}

			return errors.Trace(err)
		}
		// get binlog event, reset tryReSync, so we can re-sync binlog while syncer meets errors next time
		tryReSync = true
		binlogPos.WithLabelValues("syncer").Set(float64(e.Header.LogPos))
		binlogFile.WithLabelValues("syncer").Set(getBinlogIndex(s.meta.Pos().Name))

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			binlogEventsTotal.WithLabelValues("rotate").Inc()

			currentPos := mysql.Position{
				Name: string(ev.NextLogName),
				Pos:  uint32(ev.Position),
			}
			if compareBinlogPos(currentPos, pos, 0) <= 0 {
				continue
			}

			pos = currentPos
			err = s.meta.Save(pos, gs, true)
			if err != nil {
				return errors.Trace(err)
			}
			log.Infof("rotate binlog to %v", pos)
		case *replication.RowsEvent:
			// binlogEventsTotal.WithLabelValues("type", "rows").Add(1)
			//
			schemaName, tableName := s.renameShardingSchema(string(ev.Table.Schema), string(ev.Table.Table))
			table := &table{}
			if s.skipRowEvent(schemaName, tableName) {
				binlogSkippedEventsTotal.WithLabelValues("rows").Inc()
				if err = s.recordSkipSQLsPos(insert, pos, gs); err != nil {
					return errors.Trace(err)
				}

				log.Warnf("[skip RowsEvent]source-db:%s table:%s; target-db:%s table:%s", ev.Table.Schema, ev.Table.Table, schemaName, tableName)
				continue
			}
			table, err = s.getTable(schemaName, tableName)
			if err != nil {
				return errors.Trace(err)
			}

			log.Debugf("source-db:%s table:%s; target-db:%s table:%s, RowsEvent data: %v", ev.Table.Schema, ev.Table.Table, table.schema, table.name, ev.Rows)
			var (
				sqls []string
				keys [][]string
				args [][]interface{}
			)
			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("write_rows").Inc()

				sqls, keys, args, err = genInsertSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(insert, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("update_rows").Inc()

				sqls, keys, args, err = genUpdateSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(update, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				binlogEventsTotal.WithLabelValues("delete_rows").Inc()

				sqls, keys, args, err = genDeleteSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v, schema: %s, table: %s", err, table.schema, table.name)
				}

				for i := range sqls {
					err = s.commitJob(del, sqls[i], args[i], keys[i], true, pos, gs)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			binlogEventsTotal.WithLabelValues("query").Inc()

			sql := string(ev.Query)

			lastPos := pos
			pos.Pos = e.Header.LogPos
			gs.AddSet(uuidSet)
			log.Infof("[query]%s [pos]%v [next pos]%v [gtid]%v", sql, lastPos, pos, gs)

			sqls, err := resolveDDLSQL(sql)
			if err != nil {
				if s.skipQueryEvent(sql) {
					binlogSkippedEventsTotal.WithLabelValues("query").Inc()
					log.Warnf("[skip query-sql]%s  [schema]:%s", sql, ev.Schema)
					continue
				}
				return errors.Errorf("parse query event failed: %v", err)
			}

			for _, sql := range sqls {
				tableNames, err := s.fetchDDLTableNames(sql, string(ev.Schema))
				if err != nil {
					return errors.Trace(err)
				}
				if s.skipQueryDDL(sql, tableNames[1]) {
					binlogSkippedEventsTotal.WithLabelValues("query_ddl").Inc()
					if err = s.recordSkipSQLsPos(ddl, pos, gs); err != nil {
						return errors.Trace(err)
					}

					log.Warnf("[skip query-ddl-sql]%s [schema]%s", sql, ev.Schema)
					continue
				}

				sql, err = genDDLSQL(sql, tableNames[0], tableNames[1])
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][schema]%s [start]%s", string(ev.Schema), sql)

				job := newJob(ddl, sql, nil, "", false, pos, gs)
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}

				log.Infof("[ddl][end]%s", sql)

				s.clearTables()
			}
		case *replication.XIDEvent:
			pos.Pos = e.Header.LogPos
			gs.AddSet(uuidSet)
			job := newXIDJob(pos, gs)
			s.addJob(job)
		case *replication.GTIDEvent:
			pos.Pos = e.Header.LogPos
			u, err := uuid.FromBytes(ev.SID)
			if err != nil {
				return errors.Trace(err)
			}
			gtid := fmt.Sprintf("%s:1-%d", u.String(), ev.GNO)
			uuidSet, err = mysql.ParseUUIDSet(gtid)
			if err != nil {
				return errors.Trace(err)
			}
			log.Debugf("gtid information: binlog %v, gtid %s", pos, gtid)

			binlogGTID.WithLabelValues("syncer").Set(float64(ev.GNO))
		}
	}
}

func (s *Syncer) commitJob(tp opType, sql string, args []interface{}, keys []string, retry bool, pos mysql.Position, gs GTIDSet) error {
	key, err := s.resolveCasuality(keys)
	if err != nil {
		return errors.Errorf("resolve karam error %v", err)
	}
	job := newJob(tp, sql, args, key, retry, pos, gs)
	err = s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) resolveCasuality(keys []string) (string, error) {
	if s.c.detectConflict(keys) {
		if err := s.flushJobs(); err != nil {
			return "", errors.Trace(err)
		}
		s.c.reset()
	}
	if err := s.c.add(keys); err != nil {
		return "", errors.Trace(err)
	}
	var key string
	if len(keys) > 0 {
		key = keys[0]
	}
	return s.c.get(key), nil
}

func (s *Syncer) genRouter() error {
	s.tableRouter = route.NewTrieRouter()
	for _, rule := range s.cfg.RouteRules {
		err := s.tableRouter.Insert(rule.PatternSchema, rule.PatternTable, rule.TargetSchema, rule.TargertTable)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}

func (s *Syncer) genRegexMap() {
	for _, db := range s.cfg.DoDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := s.patternMap[db]; !ok {
			s.patternMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, db := range s.cfg.IgnoreDBs {
		if db[0] != '~' {
			continue
		}
		if _, ok := s.patternMap[db]; !ok {
			s.patternMap[db] = regexp.MustCompile(db[1:])
		}
	}

	for _, tb := range s.cfg.DoTables {
		if tb.Name[0] == '~' {
			if _, ok := s.patternMap[tb.Name]; !ok {
				s.patternMap[tb.Name] = regexp.MustCompile(tb.Name[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := s.patternMap[tb.Schema]; !ok {
				s.patternMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}

	for _, tb := range s.cfg.IgnoreTables {
		if tb.Name[0] == '~' {
			if _, ok := s.patternMap[tb.Name]; !ok {
				s.patternMap[tb.Name] = regexp.MustCompile(tb.Name[1:])
			}
		}
		if tb.Schema[0] == '~' {
			if _, ok := s.patternMap[tb.Schema]; !ok {
				s.patternMap[tb.Schema] = regexp.MustCompile(tb.Schema[1:])
			}
		}
	}
}

func (s *Syncer) printStatus() {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	var (
		err                 error
		latestMasterPos     mysql.Position
		latestmasterGTIDSet GTIDSet
	)

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - s.lastTime.Unix()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			if !s.lackOfReplClientPrivilege {
				latestMasterPos, latestmasterGTIDSet, err = s.getMasterStatus()
				if err != nil {
					if isAccessDeniedError(err) {
						s.lackOfReplClientPrivilege = true
					}
					log.Errorf("[syncer] get master status error %s", err)
				} else {
					binlogPos.WithLabelValues("master").Set(float64(latestMasterPos.Pos))
					binlogFile.WithLabelValues("master").Set(getBinlogIndex(latestMasterPos.Name))
					masterGTIDGauge(latestmasterGTIDSet, s.fromDB)
				}
			}

			log.Infof("[syncer]total events = %d, total tps = %d, recent tps = %d, master-binlog = %v, master-binlog-gtid=%v, %s",
				total, totalTps, tps, latestMasterPos, latestmasterGTIDSet, s.meta)

			s.lastCount.Set(total)
			s.lastTime = time.Now()
		}
	}
}

func (s *Syncer) getBinlogStreamer() (*replication.BinlogStreamer, bool, error) {
	if s.cfg.EnableGTID {
		gs, err := s.meta.GTID()
		if err != nil {
			return nil, false, errors.Trace(err)
		}

		streamer, err := s.syncer.StartSyncGTID(gs)
		if err != nil {
			log.Errorf("start sync in gtid mode error %v", err)
			return s.startSyncByPosition()
		}

		return streamer, true, err
	}

	return s.startSyncByPosition()
}

func (s *Syncer) createDBs() error {
	var err error
	s.fromDB, err = createDB(s.cfg.From, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	s.toDBs = make([]*sql.DB, 0, s.cfg.WorkerCount)
	s.toDBs, err = createDBs(s.cfg.To, s.cfg.WorkerCount, maxDMLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}
	// db for ddl
	s.ddlDB, err = createDB(s.cfg.To, maxDDLConnectionTimeout)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

// record skip ddl/dml sqls' position
// make newJob's sql argument empty to distinguish normal sql and skips sql
func (s *Syncer) recordSkipSQLsPos(op opType, pos mysql.Position, gtidSet GTIDSet) error {
	job := newJob(op, "", nil, "", false, pos, gtidSet)
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) flushJobs() error {
	log.Infof("flush all jobs meta = %v", s.meta)
	job := newFlushJob()
	err := s.addJob(job)
	return errors.Trace(err)
}

func (s *Syncer) reSyncBinlog(cfg *replication.BinlogSyncerConfig) (*replication.BinlogStreamer, bool, error) {
	err := s.retrySyncGTIDs()
	if err != nil {
		return nil, false, err
	}
	// close still running sync
	return s.reopen(cfg)
}

func (s *Syncer) reopen(cfg *replication.BinlogSyncerConfig) (*replication.BinlogStreamer, bool, error) {
	s.syncer.Close()
	s.syncer = replication.NewBinlogSyncer(cfg)
	return s.getBinlogStreamer()
}

func (s *Syncer) startSyncByPosition() (*replication.BinlogStreamer, bool, error) {
	streamer, err := s.syncer.StartSync(s.meta.Pos())
	return streamer, false, errors.Trace(err)
}

// the result contains [source TableNames, target TableNames]
// the detail of TableNames refs `parserDDLTableNames()`
func (s *Syncer) fetchDDLTableNames(sql string, schema string) ([][]*TableName, error) {
	tableNames, err := parserDDLTableNames(sql)
	if err != nil {
		return nil, errors.Trace(err)
	}

	var targetTableNames []*TableName
	for i := range tableNames {
		if tableNames[i].Schema == "" {
			tableNames[i].Schema = schema
		}
		schema, table := s.renameShardingSchema(tableNames[i].Schema, tableNames[i].Name)
		tableName := &TableName{
			Schema: schema,
			Name:   table,
		}
		targetTableNames = append(targetTableNames, tableName)
	}

	return [][]*TableName{tableNames, targetTableNames}, nil
}

func (s *Syncer) renameShardingSchema(schema, table string) (string, string) {
	if schema == "" {
		return schema, table
	}
	schemaL := strings.ToLower(schema)
	tableL := strings.ToLower(table)
	targetSchema, targetTable := s.tableRouter.Match(schemaL, tableL)
	if targetSchema == "" {
		return schema, table
	}
	if targetTable == "" {
		targetTable = table
	}

	return targetSchema, targetTable
}

func (s *Syncer) isClosed() bool {
	return s.closed.Get()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed() {
		return
	}

	s.cancel()

	<-s.done

	closeJobChans(s.jobs)

	s.wg.Wait()

	closeDBs(s.fromDB)
	closeDBs(s.toDBs...)
	closeDBs(s.ddlDB)

	if s.syncer != nil {
		s.syncer.Close()
		s.syncer = nil
	}

	s.closed.Set(true)
}

func (s *Syncer) needResync() bool {
	if s.lackOfReplClientPrivilege {
		return false
	}
	masterPos, _, err := s.getMasterStatus()
	if err != nil {
		log.Errorf("get master status err:%s", err)
		return false
	}

	// Why 190 ?
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | Log_name         | Pos | Event_type     | Server_id | End_log_pos | Info                                                              |
	// +------------------+-----+----------------+-----------+-------------+-------------------------------------------------------------------+
	// | mysql-bin.000002 |   4 | Format_desc    |         1 |         123 | Server ver: 5.7.18-log, Binlog ver: 4                             |
	// | mysql-bin.000002 | 123 | Previous_gtids |         1 |         194 | 00020393-1111-1111-1111-111111111111:1-7
	//
	// Currently, syncer doesn't handle Format_desc and Previous_gtids events. When binlog rotate to new file with only two events like above,
	// syncer won't save pos to 194. Actually it save pos 4 to meta file. So We got a experience value of 194 - 4 = 190.
	// If (mpos.Pos - spos.Pos) > 190, we could say that syncer is not up-to-date.
	return compareBinlogPos(masterPos, s.meta.Pos(), 190) == 1
}
