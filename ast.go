package main

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/util/types"
)

func defaultValueToSQL(opt *ast.ColumnOption) string {
	sql := " DEFAULT "
	datum := opt.Expr.GetDatum()
	switch datum.Kind() {
	case types.KindNull:
		expr, ok := opt.Expr.(*ast.FuncCallExpr)
		if ok {
			sql += expr.FnName.O
		} else {
			sql += "NULL"
		}

	case types.KindInt64:
		sql += strconv.FormatInt(datum.GetInt64(), 10)

	case types.KindString:
		sql += formatStringValue(datum.GetString())

	default:
		panic("not implemented yet")
	}

	return sql
}

func formatStringValue(s string) string {
	if s == "" {
		return "''"
	}
	return fmt.Sprintf("'%s'", s)
}

func fieldTypeToSQL(ft *types.FieldType) string {
	strs := []string{ft.CompactStr()}
	if mysql.HasUnsignedFlag(ft.Flag) {
		strs = append(strs, "UNSIGNED")
	}
	if mysql.HasZerofillFlag(ft.Flag) {
		strs = append(strs, "ZEROFILL")
	}
	if mysql.HasBinaryFlag(ft.Flag) {
		strs = append(strs, "BINARY")
	}

	return strings.Join(strs, " ")
}

// FIXME: tidb's AST is error-some to handle more condition
func columnOptionsToSQL(options []*ast.ColumnOption) string {
	sql := ""
	for _, opt := range options {
		switch opt.Tp {
		case ast.ColumnOptionNotNull:
			sql += " NOT NULL"
		case ast.ColumnOptionNull:
			sql += " NULL"
		case ast.ColumnOptionDefaultValue:
			sql += defaultValueToSQL(opt)
		case ast.ColumnOptionAutoIncrement:
			sql += " AUTO_INCREMENT"
		case ast.ColumnOptionUniqKey:
			sql += " UNIQUE KEY"
		case ast.ColumnOptionKey:
			sql += " KEY"
		case ast.ColumnOptionUniq:
			sql += " UNIQUE"
		case ast.ColumnOptionIndex:
			sql += " INDEX"
		case ast.ColumnOptionUniqIndex:
			sql += " UNIQUE INDEX"
		case ast.ColumnOptionPrimaryKey:
			sql += " PRIMARY KEY"
		case ast.ColumnOptionComment:
			sql += fmt.Sprintf(" COMMENT '%v'", opt.Expr.GetValue())
		case ast.ColumnOptionOnUpdate: // For Timestamp and Datetime only.
			sql += " ON UPDATE CURRENT_TIMESTAMP"
		case ast.ColumnOptionFulltext:
			panic("not implemented yet")
		default:
			panic("not implemented yet")
		}
	}

	return sql
}

func escapeName(name string) string {
	return strings.Replace(name, "`", "``", -1)
}

func tableNameToSQL(tbl *ast.TableName) string {
	sql := ""
	if tbl.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", tbl.Schema.O)
	}
	sql += fmt.Sprintf("`%s`", tbl.Name.O)
	return sql
}

func columnNameToSQL(name *ast.ColumnName) string {
	sql := ""
	if name.Schema.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Schema.O))
	}
	if name.Table.O != "" {
		sql += fmt.Sprintf("`%s`.", escapeName(name.Table.O))
	}
	sql += fmt.Sprintf("`%s`", escapeName(name.Name.O))
	return sql
}

func indexColNameToSQL(name *ast.IndexColName) string {
	sql := columnNameToSQL(name.Column)
	if name.Length != types.UnspecifiedLength {
		sql += fmt.Sprintf(" (%d)", name.Length)
	}
	return sql
}

func constraintKeysToSQL(keys []*ast.IndexColName) string {
	if len(keys) == 0 {
		panic("unreachable")
	}
	sql := ""
	for i, indexColName := range keys {
		if i == 0 {
			sql += "("
		}
		sql += indexColNameToSQL(indexColName)
		if i != len(keys)-1 {
			sql += ", "
		}
	}
	sql += ")"
	return sql
}

func referenceDefToSQL(refer *ast.ReferenceDef) string {
	sql := fmt.Sprintf("%s ", tableNameToSQL(refer.Table))
	sql += constraintKeysToSQL(refer.IndexColNames)
	if refer.OnDelete != nil && refer.OnDelete.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON DELETE %s", refer.OnDelete.ReferOpt)
	}
	if refer.OnUpdate != nil && refer.OnUpdate.ReferOpt != ast.ReferOptionNoOption {
		sql += fmt.Sprintf(" ON UPDATE %s", refer.OnUpdate.ReferOpt)
	}
	return sql
}

func indexTypeToSQL(opt *ast.IndexOption) string {
	// opt can be nil.
	if opt == nil {
		return ""
	}
	switch opt.Tp {
	case model.IndexTypeBtree:
		return "USING BTREE "
	case model.IndexTypeHash:
		return "USING HASH "
	default:
		// nothing to do
		return ""
	}
}

func constraintToSQL(constraint *ast.Constraint) string {
	sql := ""
	switch constraint.Tp {
	case ast.ConstraintKey, ast.ConstraintIndex:
		sql += "ADD INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintUniq, ast.ConstraintUniqKey, ast.ConstraintUniqIndex:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "UNIQUE INDEX "
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintForeignKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "FOREIGN KEY "
		sql += constraintKeysToSQL(constraint.Keys)
		sql += " REFERENCES "
		sql += referenceDefToSQL(constraint.Refer)

	case ast.ConstraintPrimaryKey:
		sql += "ADD CONSTRAINT "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += "PRIMARY KEY "
		sql += indexTypeToSQL(constraint.Option)
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	case ast.ConstraintFulltext:
		sql += "ADD FULLTEXT INDEX "
		if constraint.Name != "" {
			sql += fmt.Sprintf("`%s` ", escapeName(constraint.Name))
		}
		sql += constraintKeysToSQL(constraint.Keys)
		sql += indexOptionToSQL(constraint.Option)

	default:
		panic("not implemented yet")
	}
	return sql
}

func positionToSQL(pos *ast.ColumnPosition) string {
	var sql string
	switch pos.Tp {
	case ast.ColumnPositionNone:
	case ast.ColumnPositionFirst:
		sql = " FIRST"
	case ast.ColumnPositionAfter:
		colName := pos.RelativeColumn.Name.O
		sql = fmt.Sprintf(" AFTER `%s`", escapeName(colName))
	default:
		panic("unreachable")
	}
	return sql
}

// Convert constraint indexoption to sql. Currently only support comment.
func indexOptionToSQL(option *ast.IndexOption) string {
	if option == nil {
		return ""
	}

	if option.Comment != "" {
		return fmt.Sprintf(" COMMENT '%s'", option.Comment)
	}

	return ""
}

func tableOptionsToSQL(options []*ast.TableOption) string {
	sql := ""
	if len(options) == 0 {
		return sql
	}

	for _, opt := range options {
		switch opt.Tp {
		case ast.TableOptionEngine:
			sql += fmt.Sprintf(" ENGINE = %s", opt.StrValue)

		case ast.TableOptionCharset:
			sql += fmt.Sprintf(" CHARACTER SET = %s", opt.StrValue)

		case ast.TableOptionCollate:
			sql += fmt.Sprintf(" COLLATE = %s", opt.StrValue)

		case ast.TableOptionAutoIncrement:
			sql += fmt.Sprintf(" AUTO_INCREMENT = %d", opt.UintValue)

		case ast.TableOptionComment:
			sql += fmt.Sprintf(" COMMENT '%s'", opt.StrValue)

		case ast.TableOptionAvgRowLength:
			sql += fmt.Sprintf(" AVG_ROW_LENGTH = %d", opt.UintValue)

		case ast.TableOptionCheckSum:
			sql += fmt.Sprintf(" CHECKSUM = %d", opt.UintValue)

		case ast.TableOptionCompression:
			// In TiDB parser.y, the rule is "COMPRESSION" EqOpt Identifier. No single quote here.
			sql += fmt.Sprintf(" COMPRESSION = '%s'", opt.StrValue)

		case ast.TableOptionConnection:
			sql += fmt.Sprintf(" CONNECTION = '%s'", opt.StrValue)

		case ast.TableOptionPassword:
			sql += fmt.Sprintf(" PASSWORD = '%s'", opt.StrValue)

		case ast.TableOptionKeyBlockSize:
			sql += fmt.Sprintf(" KEY_BLOCK_SIZE = %d", opt.UintValue)

		case ast.TableOptionMaxRows:
			sql += fmt.Sprintf(" MAX_ROWS = %d", opt.UintValue)

		case ast.TableOptionMinRows:
			sql += fmt.Sprintf(" MIN_ROWS = %d", opt.UintValue)

		case ast.TableOptionDelayKeyWrite:
			sql += fmt.Sprintf(" DELAY_KEY_WRITE = %d", opt.UintValue)

		case ast.TableOptionRowFormat:
			sql += fmt.Sprintf(" ROW_FORMAT = %s", formatRowFormat(opt.UintValue))

		case ast.TableOptionStatsPersistent:
			// Since TiDB doesn't support this feature, we just give a default value.
			sql += " STATS_PERSISTENT = DEFAULT"
		default:
			panic("unreachable")
		}

	}

	// trim prefix space
	sql = strings.TrimPrefix(sql, " ")
	return sql
}

func formatRowFormat(rf uint64) string {
	var s string
	switch rf {
	case ast.RowFormatDefault:
		s = "DEFAULT"
	case ast.RowFormatDynamic:
		s = "DYNAMIC"
	case ast.RowFormatFixed:
		s = "FIXED"
	case ast.RowFormatCompressed:
		s = "COMPRESSED"
	case ast.RowFormatRedundant:
		s = "REDUNDANT"
	case ast.RowFormatCompact:
		s = "COMPACT"
	default:
		panic("unreachable")
	}
	return s
}

func columnToSQL(typeDef string, newColumn *ast.ColumnDef) string {
	return fmt.Sprintf("%s %s%s", columnNameToSQL(newColumn.Name), typeDef, columnOptionsToSQL(newColumn.Options))
}

func alterTableSpecToSQL(spec *ast.AlterTableSpec, ntable *ast.TableName) string {
	sql := ""
	log.Debugf("spec.Tp: %d", spec.Tp)

	switch spec.Tp {
	case ast.AlterTableOption:
		sql += tableOptionsToSQL(spec.Options)

	case ast.AlterTableAddColumn:
		typeDef := spec.NewColumn.Tp.String()
		sql += fmt.Sprintf("ADD COLUMN %s", columnToSQL(typeDef, spec.NewColumn))
		sql += positionToSQL(spec.Position)

	case ast.AlterTableDropColumn:
		sql += fmt.Sprintf("DROP COLUMN %s", columnNameToSQL(spec.OldColumnName))

	case ast.AlterTableDropIndex:
		sql += fmt.Sprintf("DROP INDEX `%s`", escapeName(spec.Name))

	case ast.AlterTableAddConstraint:
		sql += constraintToSQL(spec.Constraint)

	case ast.AlterTableDropForeignKey:
		sql += fmt.Sprintf("DROP FOREIGN KEY `%s`", escapeName(spec.Name))

	case ast.AlterTableModifyColumn:
		// TiDB doesn't support alter table modify column charset and collation.
		typeDef := fieldTypeToSQL(spec.NewColumn.Tp)
		sql += fmt.Sprintf("MODIFY COLUMN %s", columnToSQL(typeDef, spec.NewColumn))
		sql += positionToSQL(spec.Position)

	// FIXME: should support [FIRST|AFTER col_name], but tidb parser not support this currently.
	case ast.AlterTableChangeColumn:
		// TiDB doesn't support alter table change column charset and collation.
		typeDef := fieldTypeToSQL(spec.NewColumn.Tp)
		sql += "CHANGE COLUMN "
		sql += fmt.Sprintf("%s %s",
			columnNameToSQL(spec.OldColumnName),
			columnToSQL(typeDef, spec.NewColumn))

	case ast.AlterTableRenameTable:
		*ntable = *spec.NewTable
		sql += fmt.Sprintf("RENAME TO %s", tableNameToSQL(spec.NewTable))

	case ast.AlterTableAlterColumn:
		sql += fmt.Sprintf("ALTER COLUMN %s ", columnNameToSQL(spec.NewColumn.Name))
		if options := spec.NewColumn.Options; options != nil {
			sql += fmt.Sprintf("SET DEFAULT %v", options[0].Expr.GetValue())
		} else {
			sql += "DROP DEFAULT"
		}

	case ast.AlterTableDropPrimaryKey:
		sql += "DROP PRIMARY KEY"

	case ast.AlterTableLock:
		// just ignore it
	default:
	}
	return sql
}

func alterTableStmtToSQL(stmt *ast.AlterTableStmt, ntable *ast.TableName) string {
	var sql string
	if ntable.Name.O != "" {
		sql = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(ntable))
	} else {
		sql = fmt.Sprintf("ALTER TABLE %s ", tableNameToSQL(stmt.Table))
	}
	for i, spec := range stmt.Specs {
		if i != 0 {
			sql += ", "
		}
		sql += alterTableSpecToSQL(spec, ntable)
	}

	log.Debugf("alter table stmt to sql:%s", sql)
	return sql
}
