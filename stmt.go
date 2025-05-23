package godynamo

import (
	"database/sql/driver"
	"fmt"
	"io"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/btnguyen2k/consu/reddo"
)

const (
	field       = `([\w\-]+)`
	ifNotExists = `(\s+IF\s+NOT\s+EXISTS)?`
	ifExists    = `(\s+IF\s+EXISTS)?`
	with        = `(\s+WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+)((\s+|\s*,\s+|\s+,\s*)WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+))*)?`
)

var (
	reCreateTable   = regexp.MustCompile(`(?im)^CREATE\s+TABLE` + ifNotExists + `\s+` + field + with + `$`)
	reListTables    = regexp.MustCompile(`(?im)^LIST\s+TABLES?$`)
	reDescribeTable = regexp.MustCompile(`(?im)^DESCRIBE\s+TABLE\s+` + field + `$`)
	reAlterTable    = regexp.MustCompile(`(?im)^ALTER\s+TABLE\s+` + field + with + `$`)
	reDropTable     = regexp.MustCompile(`(?im)^(DROP|DELETE)\s+TABLE` + ifExists + `\s+` + field + `$`)

	reDescribeLSI = regexp.MustCompile(`(?im)^DESCRIBE\s+LSI\s+` + field + `\s+ON\s+` + field + `$`)
	reCreateGSI   = regexp.MustCompile(`(?im)^CREATE\s+GSI` + ifNotExists + `\s+` + field + `\s+ON\s+` + field + with + `$`)
	reDescribeGSI = regexp.MustCompile(`(?im)^DESCRIBE\s+GSI\s+` + field + `\s+ON\s+` + field + `$`)
	reAlterGSI    = regexp.MustCompile(`(?im)^ALTER\s+GSI\s+` + field + `\s+ON\s+` + field + with + `$`)
	reDropGSI     = regexp.MustCompile(`(?im)^(DROP|DELETE)\s+GSI` + ifExists + `\s+` + field + `\s+ON\s+` + field + `$`)

	reInsert = regexp.MustCompile(`(?im)^INSERT\s+INTO\s+`)
	reSelect = regexp.MustCompile(`(?im)^SELECT\s+.*?` + with + `$`)
	reUpdate = regexp.MustCompile(`(?im)^UPDATE\s+`)
	reDelete = regexp.MustCompile(`(?im)^DELETE\s+FROM\s+`)
)

func parseQuery(c *Conn, query string) (driver.Stmt, error) {
	query = strings.TrimSpace(query)
	if re := reCreateTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateTable{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
			tableName:   strings.TrimSpace(groups[0][2]),
			withOptsStr: " " + strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reListTables; re.MatchString(query) {
		stmt := &StmtListTables{
			Stmt: &Stmt{query: query, conn: c, numInput: 0},
		}
		return stmt, stmt.validate()
	}
	if re := reDescribeTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeTable{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}
	if re := reAlterTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterTable{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			tableName:   strings.TrimSpace(groups[0][1]),
			withOptsStr: " " + strings.TrimSpace(groups[0][2]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropTable; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropTable{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][3]),
			ifExists:  strings.TrimSpace(groups[0][2]) != "",
		}
		return stmt, stmt.validate()
	}

	if re := reDescribeLSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeLSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][2]),
			indexName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}

	if re := reCreateGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtCreateGSI{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			ifNotExists: strings.TrimSpace(groups[0][1]) != "",
			indexName:   strings.TrimSpace(groups[0][2]),
			tableName:   strings.TrimSpace(groups[0][3]),
			withOptsStr: " " + strings.TrimSpace(groups[0][4]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDescribeGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDescribeGSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][2]),
			indexName: strings.TrimSpace(groups[0][1]),
		}
		return stmt, stmt.validate()
	}
	if re := reAlterGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtAlterGSI{
			Stmt:        &Stmt{query: query, conn: c, numInput: 0},
			indexName:   strings.TrimSpace(groups[0][1]),
			tableName:   strings.TrimSpace(groups[0][2]),
			withOptsStr: " " + strings.TrimSpace(groups[0][3]),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDropGSI; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		stmt := &StmtDropGSI{
			Stmt:      &Stmt{query: query, conn: c, numInput: 0},
			tableName: strings.TrimSpace(groups[0][4]),
			indexName: strings.TrimSpace(groups[0][3]),
			ifExists:  strings.TrimSpace(groups[0][2]) != "",
		}
		return stmt, stmt.validate()
	}

	if re := reInsert; re.MatchString(query) {
		stmt := &StmtInsert{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reSelect; re.MatchString(query) {
		groups := re.FindAllStringSubmatch(query, -1)
		withOptsStr := groups[0][1]
		query = query[0 : len(query)-len(withOptsStr)]
		stmt := &StmtSelect{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
			withOptsStr:    " " + strings.TrimSpace(withOptsStr),
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reUpdate; re.MatchString(query) {
		stmt := &StmtUpdate{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}
	if re := reDelete; re.MatchString(query) {
		stmt := &StmtDelete{
			StmtExecutable: &StmtExecutable{Stmt: &Stmt{query: query, conn: c, numInput: 0}},
		}
		if err := stmt.parse(); err != nil {
			return nil, err
		}
		return stmt, stmt.validate()
	}

	return nil, fmt.Errorf("invalid query: %s", query)
}

type OptStrings []string

func (s OptStrings) StringAt(i int) string {
	if len(s) > i {
		return s[i]
	}
	return ""
}

func (s OptStrings) BoolAt(i int) bool {
	val, _ := reddo.ToBool(s.StringAt(i))
	return val
}

func (s OptStrings) FirstString() string {
	return s.StringAt(0)
}

func (s OptStrings) FirstBool() bool {
	return s.BoolAt(0)
}

// Stmt is AWS DynamoDB abstract implementation of driver.Stmt.
type Stmt struct {
	query    string // the SQL query
	conn     *Conn  // the connection that this prepared statement is bound to
	numInput int    // number of placeholder parameters
	limit    *int32 // limit for SELECT statement
	withOpts map[string]OptStrings
}

var reWithOpts = regexp.MustCompile(`(?im)^(\s+|\s*,\s+|\s+,\s*)WITH\s+` + field + `\s*=\s*([\w/\.\*,;:'"-]+)`)

// parseWithOpts parses "WITH..." clause and store result in withOpts map.
// This function returns no error. Sub-implementations may override this behavior.
func (s *Stmt) parseWithOpts(withOptsStr string) error {
	s.withOpts = make(map[string]OptStrings)
	for {
		matches := reWithOpts.FindStringSubmatch(withOptsStr)
		if matches == nil {
			break
		}
		k := strings.TrimSpace(strings.ToUpper(matches[2]))
		s.withOpts[k] = append(s.withOpts[k], strings.TrimSuffix(strings.TrimSpace(matches[3]), ","))
		withOptsStr = withOptsStr[len(matches[0]):]
	}
	return nil
}

// Close implements driver.Stmt/Close.
func (s *Stmt) Close() error {
	return nil
}

// NumInput implements driver.Stmt/NumInput.
func (s *Stmt) NumInput() int {
	return s.numInput
}

// ResultNoResultSet captures the result from statements that do not expect a ResultSet to be returned.
type ResultNoResultSet struct {
	err          error
	affectedRows int64
}

// LastInsertId implements driver.Result/LastInsertId.
func (r *ResultNoResultSet) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("this operation is not supported")
}

// RowsAffected implements driver.Result/RowsAffected.
func (r *ResultNoResultSet) RowsAffected() (int64, error) {
	return r.affectedRows, r.err
}

// ResultResultSet captures the result from statements that expect a ResultSet to be returned.
type ResultResultSet struct {
	err               error
	columnList        []string
	columnTypes       map[string]reflect.Type
	columnSourceTypes map[string]string
	mu                sync.Mutex // protects the following fields
	stmt              *statement
	read              int32
	items             []map[string]types.AttributeValue
}

func (r *ResultResultSet) init() *ResultResultSet {
	if r.stmt == nil {
		return r
	}
	if r.columnTypes == nil {
		r.columnTypes = make(map[string]reflect.Type)
	}
	if r.columnSourceTypes == nil {
		r.columnSourceTypes = make(map[string]string)
	}
	if r.stmt.output == nil {
		return r
	}

	r.items = r.stmt.output.Items

	// pre-calculate column types
	colMap := make(map[string]bool)
	for _, item := range r.stmt.output.Items {
		for col, av := range item {
			colMap[col] = true
			if r.columnTypes[col] == nil {
				var value interface{}
				_ = attributevalue.Unmarshal(av, &value)
				r.columnTypes[col] = reflect.TypeOf(value)
				r.columnSourceTypes[col] = nameFromAttributeValue(av)
			}
		}
	}

	if len(r.columnList) > 0 {
		// #146: if column list was provided in the SELECT statement, keep the order as specified
		return r
	}

	// save column names, sorted
	r.columnList = make([]string, 0, len(colMap))
	for col := range colMap {
		r.columnList = append(r.columnList, col)
	}
	sort.Strings(r.columnList)

	return r
}

func (r *ResultResultSet) fetchNext() (err error) {
	if r.stmt.output.NextToken == nil {
		return io.EOF
	}
	r.stmt.input.NextToken = r.stmt.output.NextToken
	r.stmt.output, err = r.stmt.client.ExecuteStatement(r.stmt.ctx, r.stmt.input)
	if err == nil && r.stmt.output != nil {
		r.items = r.stmt.output.Items
	}
	return err
}

// Columns implements driver.Rows/Columns.
func (r *ResultResultSet) Columns() []string {
	return r.columnList
}

// ColumnTypeScanType implements driver.RowsColumnTypeScanType/ColumnTypeScanType
func (r *ResultResultSet) ColumnTypeScanType(index int) reflect.Type {
	return r.columnTypes[r.columnList[index]]
}

// ColumnTypeDatabaseTypeName implements driver.RowsColumnTypeDatabaseTypeName/ColumnTypeDatabaseTypeName
//
// @since v0.3.0 ColumnTypeDatabaseTypeName returns DynamoDB's native data types (e.g. B, N, S, SS, NS, BS, BOOL, L, M, NULL).
func (r *ResultResultSet) ColumnTypeDatabaseTypeName(index int) string {
	return r.columnSourceTypes[r.columnList[index]]
}

// Close implements driver.Rows/Close.
func (r *ResultResultSet) Close() error {
	return r.err
}

// Next implements driver.Rows/Next.
func (r *ResultResultSet) Next(dest []driver.Value) error {
	if r.err != nil {
		return r.err
	}
	if r.stmt.limit > 0 && r.read >= r.stmt.limit {
		return io.EOF
	}

	r.mu.Lock()
	if len(r.items) == 0 {
		r.err = r.fetchNext()
		if r.err != nil {
			r.mu.Unlock()
			return r.err
		}
		r.mu.Unlock()
		return r.Next(dest)
	}
	rowData := r.items[0]
	r.items = r.items[1:]
	r.mu.Unlock()
	r.read++

	for i, colName := range r.columnList {
		var value interface{}
		_ = attributevalue.Unmarshal(rowData[colName], &value)
		dest[i] = value
	}
	return nil
}
