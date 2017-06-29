package main

import (
	"bytes"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"hash/crc32"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	tmysql "github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

// safeMode makes syncer reentrant.
// we make each operator reentrant to make syncer reentrant.
// `replace` and `delete` are naturally reentrant.
// use `delete`+`replace` to represent `update` can make `update`  reentrant.
// but there are no ways to make `update` idempotent,
// if we start syncer at an early position, database must bear a period of inconsistent state,
// it's eventual consistency.
var safeMode bool

type column struct {
	idx      int
	name     string
	unsigned bool
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns map[string][]*column
}

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func columnValue(value interface{}, unsigned bool) string {
	castValue := castUnsigned(value, unsigned)

	var data string
	switch v := castValue.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findColumn(columns []*column, indexColumn string) *column {
	for _, column := range columns {
		if column.name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*column, indexColumns map[string][]string) map[string][]*column {
	result := make(map[string][]*column)

	for keyName, indexCols := range indexColumns {
		cols := make([]*column, 0, len(indexCols))
		for _, name := range indexCols {
			column := findColumn(columns, name)
			if column != nil {
				cols = append(cols, column)
			}
		}
		result[keyName] = cols
	}

	return result
}

func genColumnList(columns []*column) string {
	var columnList []byte
	for i, column := range columns {
		name := fmt.Sprintf("`%s`", column.name)
		columnList = append(columnList, []byte(name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func genKeyList(columns []*column, dataSeq []interface{}) string {
	values := make([]string, 0, len(dataSeq))
	for i, data := range dataSeq {
		values = append(values, columnValue(data, columns[i].unsigned))
	}

	return strings.Join(values, ",")
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}

func genInsertSQLs(schema string, table string, dataSeq [][]interface{}, columns []*column, indexColumns map[string][]*column) ([]string, [][]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(dataSeq))
	keys := make([][]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	for _, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("insert columns and data mismatch in length: %d vs %d", len(columns), len(data))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		sql := fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schema, table, columnList, columnPlaceholders)
		ks := genMultipleKeys(columns, value, indexColumns)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genMultipleKeys(columns []*column, value []interface{}, indexColumns map[string][]*column) []string {
	var multipleKeys []string
	for _, indexCols := range indexColumns {
		cols, vals := getColumnData(columns, indexCols, value)
		multipleKeys = append(multipleKeys, genKeyList(cols, vals))
	}
	return multipleKeys
}

func getDefaultIndexColumn(indexColumns map[string][]*column) []*column {
	for _, indexCols := range indexColumns {
		if len(indexCols) > 0 {
			return indexCols
		}
	}

	return nil
}

func getColumnData(columns []*column, indexColumns []*column, data []interface{}) ([]*column, []interface{}) {
	cols := make([]*column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, data[column.idx])
	}

	return cols, values
}

func genWhere(columns []*column, data []interface{}) string {
	var kvs bytes.Buffer
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "IS"
		}

		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` %s ?", columns[i].name, kvSplit)
		} else {
			fmt.Fprintf(&kvs, "`%s` %s ? AND ", columns[i].name, kvSplit)
		}
	}

	return kvs.String()
}

func genKVs(columns []*column) string {
	var kvs bytes.Buffer
	for i := range columns {
		if i == len(columns)-1 {
			fmt.Fprintf(&kvs, "`%s` = ?", columns[i].name)
		} else {
			fmt.Fprintf(&kvs, "`%s` = ?, ", columns[i].name)
		}
	}

	return kvs.String()
}

func genUpdateSQLs(schema string, table string, data [][]interface{}, columns []*column, indexColumns map[string][]*column) ([]string, [][]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(data)/2)
	keys := make([][]string, 0, len(data)/2)
	values := make([][]interface{}, 0, len(data)/2)
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	defaultIndexColumn := getDefaultIndexColumn(indexColumns)
	for i := 0; i < len(data); i += 2 {
		oldData := data[i]
		changedData := data[i+1]
		if len(oldData) != len(changedData) {
			return nil, nil, nil, errors.Errorf("update data mismatch in length: %d vs %d", len(oldData), len(changedData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, errors.Errorf("update columns and data mismatch in length: %d vs %d", len(columns), len(oldData))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		for i := range oldData {
			oldValues = append(oldValues, castUnsigned(oldData[i], columns[i].unsigned))
		}
		changedValues := make([]interface{}, 0, len(changedData))
		for i := range changedData {
			changedValues = append(changedValues, castUnsigned(changedData[i], columns[i].unsigned))
		}

		ks := genMultipleKeys(columns, oldValues, indexColumns)
		ks = append(ks, genMultipleKeys(columns, changedValues, indexColumns)...)

		if safeMode {
			// generate delete sql from old data
			sql, value := genDeleteSQL(schema, table, oldValues, columns, defaultIndexColumn)
			sqls = append(sqls, sql)
			values = append(values, value)
			keys = append(keys, ks)
			// generate replace sql from new data
			sql = fmt.Sprintf("REPLACE INTO `%s`.`%s` (%s) VALUES (%s);", schema, table, columnList, columnPlaceholders)
			sqls = append(sqls, sql)
			values = append(values, changedValues)
			keys = append(keys, ks)
			continue
		}

		updateColumns := make([]*column, 0, len(defaultIndexColumn))
		updateValues := make([]interface{}, 0, len(defaultIndexColumn))
		for j := range oldValues {
			if reflect.DeepEqual(oldValues[j], changedValues[j]) {
				continue
			}
			updateColumns = append(updateColumns, columns[j])
			updateValues = append(updateValues, changedValues[j])
		}

		// ignore no changed sql
		if len(updateColumns) == 0 {
			continue
		}

		value := make([]interface{}, 0, len(oldData))
		kvs := genKVs(updateColumns)
		value = append(value, updateValues...)

		whereColumns, whereValues := columns, oldValues
		if len(defaultIndexColumn) > 0 {
			whereColumns, whereValues = getColumnData(columns, defaultIndexColumn, oldValues)
		}

		where := genWhere(whereColumns, whereValues)
		value = append(value, whereValues...)

		sql := fmt.Sprintf("UPDATE `%s`.`%s` SET %s WHERE %s LIMIT 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genDeleteSQLs(schema string, table string, dataSeq [][]interface{}, columns []*column, indexColumns map[string][]*column) ([]string, [][]string, [][]interface{}, error) {
	sqls := make([]string, 0, len(dataSeq))
	keys := make([][]string, 0, len(dataSeq))
	values := make([][]interface{}, 0, len(dataSeq))
	defaultIndexColumn := getDefaultIndexColumn(indexColumns)
	for _, data := range dataSeq {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("delete columns and data mismatch in length: %d vs %d", len(columns), len(data))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		ks := genMultipleKeys(columns, value, indexColumns)

		sql, value := genDeleteSQL(schema, table, value, columns, defaultIndexColumn)
		sqls = append(sqls, sql)
		values = append(values, value)
		keys = append(keys, ks)
	}

	return sqls, keys, values, nil
}

func genDeleteSQL(schema string, table string, value []interface{}, columns []*column, indexColumns []*column) (string, []interface{}) {
	whereColumns, whereValues := columns, value
	if len(indexColumns) > 0 {
		whereColumns, whereValues = getColumnData(columns, indexColumns, value)
	}

	where := genWhere(whereColumns, whereValues)
	sql := fmt.Sprintf("DELETE FROM `%s`.`%s` WHERE %s LIMIT 1;", schema, table, where)

	return sql, whereValues
}

func ignoreDDLError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	case tmysql.ErrDupKeyName:
		return true
	default:
		return false
	}
}

func isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Errorf("[sql]%s[error]%v", sql, err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	return isDDL, nil
}

// resolveDDLSQL resolve to one ddl sql
// example: drop table test.a,test2.b -> drop table test.a; drop table test2.b;
func resolveDDLSQL(sql string) (sqls []string, err error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		log.Errorf("error while parsing sql: %s, err:%s", sql, err)
		return nil, errors.Trace(err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	if !isDDL {
		// let sqls be empty
		return
	}

	switch v := stmt.(type) {
	case *ast.DropTableStmt:
		var ex string
		if v.IfExists {
			ex = "IF EXISTS "
		}
		for _, t := range v.Tables {
			var db string
			if t.Schema.O != "" {
				db = fmt.Sprintf("`%s`.", t.Schema.O)
			}
			s := fmt.Sprintf("DROP TABLE %s%s`%s`", ex, db, t.Name.O)
			sqls = append(sqls, s)
		}
	case *ast.AlterTableStmt:
		tempSpecs := v.Specs
		if len(tempSpecs) <= 1 {
			sqls = append(sqls, sql)
			break
		}

		newTable := &ast.TableName{}
		log.Warnf("will split alter table statement: %v", sql)
		for i := range tempSpecs {
			v.Specs = tempSpecs[i : i+1]
			sql1 := alterTableStmtToSQL(v, newTable)
			log.Warnf("splitted alter table statement: %s", sql1)
			sqls = append(sqls, sql1)
		}
	default:
		sqls = append(sqls, sql)
	}
	return sqls, nil
}

// todo: fix the ugly code, use ast to rename table
func genDDLSQL(sql string, originTableNames []*TableName, targetTableNames []*TableName) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	switch stmt.(type) {
	case *ast.CreateDatabaseStmt:
		if originTableNames[0].Schema == targetTableNames[0].Schema {
			return sql, nil
		}
		sqlPrefix := createDatabaseRegex.FindString(sql)
		index := findLastWord(sqlPrefix)
		return createDatabaseRegex.ReplaceAllString(sql, fmt.Sprintf("%s`%s`", sqlPrefix[:index], targetTableNames[0].Schema)), nil
	case *ast.DropDatabaseStmt:
		if originTableNames[0].Schema == targetTableNames[0].Schema {
			return sql, nil
		}
		sqlPrefix := dropDatabaseRegex.FindString(sql)
		index := findLastWord(sqlPrefix)
		return dropDatabaseRegex.ReplaceAllString(sql, fmt.Sprintf("%s`%s`", sqlPrefix[:index], targetTableNames[0].Schema)), nil
	case *ast.CreateTableStmt:
		var (
			sqlPrefix string
			index     int
		)
		// replace `like schema.table` section
		if len(originTableNames) == 2 {
			sqlPrefix = createTableLikeRegex.FindString(sql)
			index = findLastWord(sqlPrefix)
			endChars := ""
			if sqlPrefix[len(sqlPrefix)-1] == ')' {
				endChars = ")"
			}
			sql = createTableLikeRegex.ReplaceAllString(sql, fmt.Sprintf("%s`%s`.`%s`%s", sqlPrefix[:index], targetTableNames[1].Schema, targetTableNames[1].Name, endChars))
			fmt.Println(sql)
		}
		// replce `create table schame.table` section
		sqlPrefix = createTableRegex.FindString(sql)
		index = findLastWord(sqlPrefix)
		endChars := findTableDefineIndex(sqlPrefix[index:])
		return createTableRegex.ReplaceAllString(sql, fmt.Sprintf("%s`%s`.`%s`%s", sqlPrefix[:index], targetTableNames[0].Schema, targetTableNames[0].Name, endChars)), nil
	case *ast.DropTableStmt:
		sqlPrefix := dropTableRegex.FindString(sql)
		index := findLastWord(sqlPrefix)
		sql = dropTableRegex.ReplaceAllString(sql, fmt.Sprintf("%s`%s`", sqlPrefix[:index], targetTableNames[0].Name))
	case *ast.TruncateTableStmt:
		sql = fmt.Sprintf("TRUNCATE TABLE `%s`", targetTableNames[0].Name)
	case *ast.AlterTableStmt:
		// RENAME [TO|AS] new_tbl_name
		if len(originTableNames) == 2 {
			index := findLastWord(sql)
			sql = fmt.Sprintf("%s`%s`.`%s`", sql[:index], targetTableNames[1].Schema, targetTableNames[1].Name)
		}
		sql = alterTableRegex.ReplaceAllString(sql, fmt.Sprintf("ALTER TABLE `%s`", targetTableNames[0].Name))
	case *ast.RenameTableStmt:
		return fmt.Sprintf("RENAME TABLE `%s`.`%s` TO `%s`.`%s`", targetTableNames[0].Schema, targetTableNames[0].Name,
			targetTableNames[1].Schema, targetTableNames[1].Name), nil
	case *ast.CreateIndexStmt, *ast.DropIndexStmt:
		sql = indexDDLRegex.ReplaceAllString(sql, fmt.Sprintf("ON `%s`", targetTableNames[0].Name))
	default:
		return "", errors.Errorf("unkown type ddl %s", sql)
	}

	return fmt.Sprintf("use `%s`; %s;", targetTableNames[0].Schema, sql), nil
}

func findTableDefineIndex(literal string) string {
	for i := range literal {
		if literal[i] == '(' {
			return literal[i:]
		}
	}
	return ""
}

func genTableName(schema string, table string) *TableName {
	return &TableName{Schema: schema, Name: table}

}

// the result contains [tableName] excepted create table like and rename table
// for `create table like` DDL, result contains [sourceTableName, sourceRefTableName]
// for rename table ddl, result contains [targetOldTableName, sourceNewTableName]
func parserDDLTableNames(sql string) ([]*TableName, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return nil, errors.Trace(err)
	}

	var res []*TableName
	switch v := stmt.(type) {
	case *ast.CreateDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
	case *ast.DropDatabaseStmt:
		res = append(res, genTableName(v.Name, ""))
	case *ast.CreateTableStmt:
		res = append(res, genTableName(v.Table.Schema.L, v.Table.Name.L))
		if v.ReferTable != nil {
			res = append(res, genTableName(v.ReferTable.Schema.L, v.ReferTable.Name.L))
		}
	case *ast.DropTableStmt:
		if len(v.Tables) != 1 {
			return res, errors.Errorf("drop table with multiple tables, may resovle ddl sql failed")
		}
		res = append(res, genTableName(v.Tables[0].Schema.L, v.Tables[0].Name.L))
	case *ast.TruncateTableStmt:
		res = append(res, genTableName(v.Table.Schema.L, v.Table.Name.L))
	case *ast.AlterTableStmt:
		res = append(res, genTableName(v.Table.Schema.L, v.Table.Name.L))
		if v.Specs[0].NewTable != nil {
			res = append(res, genTableName(v.Specs[0].NewTable.Schema.L, v.Specs[0].NewTable.Name.L))
		}
	case *ast.RenameTableStmt:
		res = append(res, genTableName(v.OldTable.Schema.L, v.OldTable.Name.L))
		res = append(res, genTableName(v.NewTable.Schema.L, v.NewTable.Name.L))
	case *ast.CreateIndexStmt:
		res = append(res, genTableName(v.Table.Schema.L, v.Table.Name.L))
	case *ast.DropIndexStmt:
		res = append(res, genTableName(v.Table.Schema.L, v.Table.Name.L))
	default:
		return res, errors.Errorf("unkown type ddl %s", sql)
	}

	return res, nil
}

func isRetryableError(err error) bool {
	if err == driver.ErrBadConn {
		return true
	}
	var e error
	for {
		e = errors.Cause(err)
		if err == e {
			break
		}
		err = e
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	if ok {
		if mysqlErr.Number == tmysql.ErrUnknown {
			return true
		}
		return false
	}

	return true
}

func isBinlogPurgedError(err error) bool {
	mysqlErr, ok := errors.Cause(err).(*gmysql.MyError)
	if !ok {
		return false
	}
	errCode := terror.ErrCode(mysqlErr.Code)
	if errCode == gmysql.ER_MASTER_FATAL_ERROR_READING_BINLOG {
		return true
	}
	return false
}

func isAccessDeniedError(err error) bool {
	return isError(err, tmysql.ErrSpecificAccessDenied)
}

func isError(err error, code uint16) bool {
	// get origin error
	var e error
	for {
		e = errors.Cause(err)
		if err == e {
			break
		}
		err = e
	}
	mysqlErr, ok := err.(*mysql.MySQLError)
	return ok && mysqlErr.Number == code
}

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("type", "query").Add(1)
			log.Warnf("sql query retry %d: %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Debugf("[query][sql]%s", query)

		rows, err = db.Query(query)
		if err != nil {
			if !isRetryableError(err) {
				return rows, errors.Trace(err)
			}
			log.Warnf("[query][sql]%s[error]%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

func executeSQL(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

	retryCount := 1
	if retry {
		retryCount = maxRetryCount
	}

LOOP:
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			sqlRetriesTotal.WithLabelValues("stmt_exec").Add(1)
			log.Warnf("sql stmt_exec retry %d: %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		for i := range sqls {
			log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				if !isRetryableError(err) {
					rerr := txn.Rollback()
					if rerr != nil {
						log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
					}
					break LOOP
				}

				log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
				}
				continue LOOP
			}
		}
		err = txn.Commit()
		if err != nil {
			log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("exec sqls[%v]args[%v]failed %v", sqls, args, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return errors.Errorf("exec sqls[%v] failed", sqls)
}

func findLastWord(literal string) int {
	index := len(literal) - 1
	for index >= 0 && literal[index-1] == ' ' {
		index--
	}

	for index >= 0 {
		if literal[index-1] == ' ' {
			return index
		}
		index--
	}
	return index
}

func createDB(cfg DBConfig, timeout string) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8&interpolateParams=true&readTimeout=%s", cfg.User, cfg.Password, cfg.Host, cfg.Port, timeout)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}

func createDBs(cfg DBConfig, count int, timeout string) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := createDB(cfg, timeout)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeDBs(dbs ...*sql.DB) {
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Errorf("close db failed: %v", err)
		}
	}
}

func getServerUUID(db *sql.DB) (string, error) {
	var masterUUID string
	rows, err := db.Query(`select @@server_uuid;`)
	if err != nil {
		return "", errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
	   MySQL [test]> SHOW MASTER STATUS;
	   +--------------------------------------+
	   | @@server_uuid                        |
	   +--------------------------------------+
	   | 53ea0ed1-9bf8-11e6-8bea-64006a897c73 |
	   +--------------------------------------+
	*/
	for rows.Next() {
		err = rows.Scan(&masterUUID)
		if err != nil {
			return "", errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return "", errors.Trace(rows.Err())
	}
	return masterUUID, nil
}

func getMasterStatus(db *sql.DB) (gmysql.Position, GTIDSet, error) {
	var (
		binlogPos gmysql.Position
		gs        GTIDSet
	)
	rows, err := db.Query(`SHOW MASTER STATUS`)
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return binlogPos, gs, errors.Trace(err)
	}

	// Show an example.
	/*
		MySQL [test]> SHOW MASTER STATUS;
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| File      | Position | Binlog_Do_DB | Binlog_Ignore_DB | Executed_Gtid_Set                          |
		+-----------+----------+--------------+------------------+--------------------------------------------+
		| ON.000001 |     4822 |              |                  | 85ab69d1-b21f-11e6-9c5e-64006a8978d2:1-46
		+-----------+----------+--------------+------------------+--------------------------------------------+
	*/
	var (
		gtid       string
		binlogName string
		pos        uint32
		nullPtr    interface{}
	)
	for rows.Next() {
		if len(rowColumns) == 5 {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr, &gtid)
		} else {
			err = rows.Scan(&binlogName, &pos, &nullPtr, &nullPtr)
		}
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}

		binlogPos = gmysql.Position{
			Name: binlogName,
			Pos:  pos,
		}

		gs, err = parseGTIDSet(gtid)
		if err != nil {
			return binlogPos, gs, errors.Trace(err)
		}
	}
	if rows.Err() != nil {
		return binlogPos, gs, errors.Trace(rows.Err())
	}

	return binlogPos, gs, nil
}
