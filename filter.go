package main

import (
	"regexp"
	"strings"
)

/*
CREATE [TEMPORARY] TABLE [IF NOT EXISTS] tbl_name
    { LIKE old_tbl_name | (LIKE old_tbl_name) }
*/
var (
	defaultIgnoreDB = "mysql"
	// https://dev.mysql.com/doc/refman/5.7/en/create-database.html
	createDatabaseRegex = regexp.MustCompile("(?i)CREATE\\s+(DATABASE|SCHEMA)\\s+(IF NOT EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-database.html
	dropDatabaseRegex = regexp.MustCompile("(?i)DROP\\s+(DATABASE|SCHEMA)\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-index.html
	// https://dev.mysql.com/doc/refman/5.7/en/drop-index.html
	indexDDLRegex = regexp.MustCompile("(?i)ON\\s+\\S*")
	// https://dev.mysql.com/doc/refman/5.7/en/create-table.html
	createTableRegex     = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+")
	createTableLikeRegex = regexp.MustCompile("(?i)^CREATE\\s+(TEMPORARY\\s+)?TABLE\\s+(IF NOT EXISTS\\s+)?\\S+\\s*\\(?\\s*LIKE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/drop-table.html
	dropTableRegex = regexp.MustCompile("^(?i)DROP\\s+(TEMPORARY\\s+)?TABLE\\s+(IF EXISTS\\s+)?\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
	alterTableRegex = regexp.MustCompile("^(?i)ALTER\\s+TABLE\\s+\\S+")
	// https://dev.mysql.com/doc/refman/5.7/en/create-trigger.html
	triggerRegex = regexp.MustCompile(`(^(?i)CREATE (DEFINER=\S+ )*TRIGGER)`)
	skipSQLs     = []string{
		// For mariadb, for query event, like `# Dumm`
		// But i don't know what is the meaning of this event.
		"^#",

		// transaction
		"^SAVEPOINT",

		// skip all flush sqls
		"^FLUSH",

		// table maintenance
		"^OPTIMIZE\\s+TABLE",
		"^ANALYZE\\s+TABLE",
		"^REPAIR\\s+TABLE",

		//trigger
		"^CREATE\\s+TRIGGER",
		"^DROP\\s+TRIGGER",

		//procedure
		"^DROP\\s+PROCEDURE",
		"^CREATE\\s+PROCEDURE",
		"^ALTER\\s+PROCEDURE",

		//view
		"^CREATE\\s+VIEW",
		"^DROP\\s+VIEW",
		"^ALTER\\s+VIEW",

		//function
		"^CREATE\\s+FUNCTION",
		"^ALTER\\s+FUNCTION",
		"^DROP\\s+FUNCTION",

		// account management
		"^GRANT",
		"^REVOKE",
		"^CREATE\\s+USER",
		"^ALTER\\s+USER",
		"^RENAME\\s+USER",
		"^DROP\\s+USER",
		"^SET\\s+PASSWORD",
	}
	skipPatterns *regexp.Regexp
)

func init() {
	skipPatterns = regexp.MustCompile("(?i)" + strings.Join(skipSQLs, "|"))
}

// whiteFilter whitelist filtering
func (s *Syncer) whiteFilter(stbs []*TableName) []*TableName {
	var tbs []*TableName
	if len(s.cfg.DoTables) == 0 && len(s.cfg.DoDBs) == 0 {
		return stbs
	}
	for _, tb := range stbs {
		if s.matchTable(s.cfg.DoTables, tb) {
			tbs = append(tbs, tb)
		}
		if s.matchDB(s.cfg.DoDBs, tb.Schema) {
			tbs = append(tbs, tb)
		}
	}
	return tbs
}

// blackFilter blacklist filtering
func (s *Syncer) blackFilter(stbs []*TableName) []*TableName {
	var tbs []*TableName
	for _, tb := range stbs {
		if s.matchTable(s.cfg.IgnoreTables, tb) {
			continue
		}
		if s.matchDB(s.cfg.IgnoreDBs, tb.Schema) {
			continue
		}
		tbs = append(tbs, tb)
	}
	return tbs
}

func (s *Syncer) skipQueryEvent(sql string) bool {
	if skipPatterns.FindStringIndex(sql) != nil {
		return true
	}

	for _, skipSQL := range s.cfg.SkipSQLs {
		if strings.HasPrefix(strings.ToUpper(sql), strings.ToUpper(skipSQL)) {
			return true
		}
	}

	if triggerRegex.FindStringIndex(sql) != nil {
		return true
	}

	return false
}

// skipRowEvent first whitelist filtering and then blacklist filtering
func (s *Syncer) skipRowEvent(schema string, table string) bool {
	if schema == defaultIgnoreDB {
		return true
	}
	tbs := []*TableName{
		{
			Schema: strings.ToLower(schema),
			Name:   strings.ToLower(table),
		},
	}
	tbs = s.whiteFilter(tbs)
	tbs = s.blackFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

// skipQueryDDL first whitelist filtering and then blacklist filtering
func (s *Syncer) skipQueryDDL(sql string, tbs []*TableName) bool {
	for i := range tbs {
		if tbs[i].Schema == defaultIgnoreDB {
			return true
		}
	}
	tbs = s.whiteFilter(tbs)
	tbs = s.blackFilter(tbs)
	if len(tbs) == 0 {
		return true
	}
	return false
}

func (s *Syncer) matchString(pattern string, t string) bool {
	if re, ok := s.patternMap[pattern]; ok {
		return re.MatchString(t)
	}
	return pattern == t
}

func (s *Syncer) matchDB(patternDBS []string, a string) bool {
	for _, b := range patternDBS {
		if s.matchString(b, a) {
			return true
		}
	}
	return false
}

func (s *Syncer) matchTable(patternTBS []*TableName, tb *TableName) bool {
	for _, ptb := range patternTBS {
		if s.matchString(ptb.Name, tb.Name) && s.matchString(ptb.Schema, tb.Schema) {
			return true
		}

		// create database or drop database
		if tb.Name == "" {
			if s.matchString(tb.Schema, ptb.Schema) {
				return true
			}
		}
	}

	return false
}
