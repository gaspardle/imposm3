package sqlserver

import (
	"database/sql"
	"encoding/hex"
	"fmt"
	"github.com/gaspardle/go-mssqlclrgeo"
	"strings"
	"sync"
)

type TableTx interface {
	Begin(*sql.Tx) error
	Insert(row []interface{}) error
	Delete(id int64) error
	End()
	Commit() error
	Rollback()
}

type bulkTableTx struct {
	Pg         *Mssql
	Tx         *sql.Tx
	Table      string
	Spec       *TableSpec
	InsertStmt *sql.Stmt
	InsertSql  string
	wg         *sync.WaitGroup
	rows       chan []interface{}
}

func NewBulkTableTx(mssql *Mssql, spec *TableSpec) TableTx {
	tt := &bulkTableTx{
		Pg:    mssql,
		Table: spec.FullName,
		Spec:  spec,
		wg:    &sync.WaitGroup{},
		rows:  make(chan []interface{}, 64),
	}
	tt.wg.Add(1)
	go tt.loop()
	return tt
}

func (tt *bulkTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	_, err = tx.Exec(fmt.Sprintf(`TRUNCATE TABLE %s.%s`, tt.Pg.Config.ImportSchema, tt.Table))
	if err != nil {
		return err
	}

	tt.InsertSql = tt.Spec.CopySQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSql)
	if err != nil {
		return &SQLError{tt.InsertSql, err}
	}
	tt.InsertStmt = stmt

	return nil
}

func (tt *bulkTableTx) Insert(row []interface{}) error {

	for idx, col := range tt.Spec.Columns {

		//geometryType
		if col.FieldType.Name == "geometry" || col.FieldType.Name == "validated_geometry" {
			wkb, _ := hex.DecodeString(row[idx].(string))
			udt, err := mssqlclrgeo.WkbToUdtGeo(wkb, false)
			if err != nil {
				return err
			}
			row[idx] = udt
		}
		//XXX hstore to json
		if col.FieldType.Name == "hstore_tags" {
			value := row[idx].(string)
			value = strings.Replace(value, "=>", ":", -1)
			row[idx] = "{" + value + "}"
		}
	}

	tt.rows <- row
	return nil
}

func (tt *bulkTableTx) loop() {
	for row := range tt.rows {
		_, err := tt.InsertStmt.Exec(row...)
		if err != nil {
			// TODO
			log.Fatal(&SQLInsertError{SQLError{tt.InsertSql, err}, row})
		}
	}
	tt.wg.Done()
}

func (tt *bulkTableTx) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *bulkTableTx) End() {
	close(tt.rows)
	tt.wg.Wait()
}

func (tt *bulkTableTx) Commit() error {
	tt.End()
	if tt.InsertStmt != nil {
		_, err := tt.InsertStmt.Exec()
		if err != nil {
			return err
		}
	}
	err := tt.Tx.Commit()
	if err != nil {
		return err
	}
	tt.Tx = nil
	return nil
}

func (tt *bulkTableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}

type syncTableTx struct {
	Pg         *Mssql
	Tx         *sql.Tx
	Table      string
	Spec       tableSpec
	Spec2      *TableSpec
	InsertStmt *sql.Stmt
	DeleteStmt *sql.Stmt
	InsertSql  string
	DeleteSql  string
}

type tableSpec interface {
	InsertSQL() string
	DeleteSQL() string
}

func NewSynchronousTableTx(mssql *Mssql, tableName string, spec tableSpec) TableTx {
	tt := &syncTableTx{
		Pg:    mssql,
		Table: tableName,
		Spec:  spec,
	}
	return tt
}
func NewSynchronousTableTxWithColumns(mssql *Mssql, tableName string, spec *TableSpec) TableTx {
	tt := &syncTableTx{
		Pg:    mssql,
		Table: tableName,
		Spec:  spec,
		Spec2: spec,
	}
	return tt
}

func (tt *syncTableTx) Begin(tx *sql.Tx) error {
	var err error
	if tx == nil {
		tx, err = tt.Pg.Db.Begin()
		if err != nil {
			return err
		}
	}
	tt.Tx = tx

	tt.InsertSql = tt.Spec.InsertSQL()

	stmt, err := tt.Tx.Prepare(tt.InsertSql)
	if err != nil {
		return &SQLError{tt.InsertSql, err}
	}
	tt.InsertStmt = stmt

	tt.DeleteSql = tt.Spec.DeleteSQL()
	stmt, err = tt.Tx.Prepare(tt.DeleteSql)
	if err != nil {
		return &SQLError{tt.DeleteSql, err}
	}
	tt.DeleteStmt = stmt

	return nil
}

func (tt *syncTableTx) Insert(row []interface{}) error {

	if tt.Spec2 != nil {
		for idx, col := range tt.Spec2.Columns {
			//geometryType
			if col.FieldType.Name == "geometry" || col.FieldType.Name == "validated_geometry" {
				wkb, _ := hex.DecodeString(row[idx].(string))
				udt, err := mssqlclrgeo.WkbToUdtGeo(wkb, false)
				if err != nil {
					return err
				}
				row[idx] = udt
			}
			//xxx hstore to json
			if col.FieldType.Name == "hstore_tags" {
				value := row[idx].(string)
				value = strings.Replace(value, "=>", ":", -1)
				row[idx] = "{" + value + "}"
			}

		}
	}
	_, err := tt.InsertStmt.Exec(row...)
	if err != nil {
		return &SQLInsertError{SQLError{tt.InsertSql, err}, row}
	}
	return nil
}

func (tt *syncTableTx) Delete(id int64) error {
	_, err := tt.DeleteStmt.Exec(id)
	if err != nil {
		return &SQLInsertError{SQLError{tt.DeleteSql, err}, id}
	}
	return nil
}

func (tt *syncTableTx) End() {
}

func (tt *syncTableTx) Commit() error {
	err := tt.Tx.Commit()
	if err != nil {
		return err
	}
	tt.Tx = nil
	return nil
}

func (tt *syncTableTx) Rollback() {
	rollbackIfTx(&tt.Tx)
}
