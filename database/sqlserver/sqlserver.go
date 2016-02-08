package sqlserver

import (
	"database/sql"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"

	_ "github.com/gaspardle/go-mssqldb"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/element"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/logging"
	"github.com/omniscale/imposm3/mapping"
)

var log = logging.NewLogger("SqlServer")

type SQLError struct {
	query         string
	originalError error
}

func (e *SQLError) Error() string {
	return fmt.Sprintf("SQL Error: %s in query %s", e.originalError.Error(), e.query)
}

type SQLInsertError struct {
	SQLError
	data interface{}
}

func (e *SQLInsertError) Error() string {
	return fmt.Sprintf("SQL Error: %s in query %s (%+v)", e.originalError.Error(), e.query, e.data)
}

func createTable(tx *sql.Tx, spec TableSpec) error {
	var sql string
	var err error

	err = dropTableIfExists(tx, spec.Schema, spec.FullName)
	if err != nil {
		return err
	}

	sql = spec.CreateTableSQL()
	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = addGeometryColumn(tx, spec.FullName, spec)
	if err != nil {
		return err
	}
	return nil
}

func addGeometryColumn(tx *sql.Tx, tableName string, spec TableSpec) error {
	colName := ""
	for _, col := range spec.Columns {
		if col.Type.Name() == "GEOMETRY" {
			colName = col.Name
			break
		}
	}

	if colName == "" {
		return nil
	}

	geomType := strings.ToUpper(spec.GeometryType)
	if geomType == "POLYGON" {
		geomType = "GEOMETRY" // for multipolygon support
	}
	sql := fmt.Sprintf("ALTER TABLE %s.%s ADD [%s] geometry NULL",
		spec.Schema, tableName, colName)
	_, err := tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	//sql := fmt.Sprintf("SELECT AddGeometryColumn('%s', '%s', 'geometry', '%d', '%s', 2);",
	//	spec.Schema, tableName, spec.Srid, geomType)
	//row := tx.QueryRow(sql)
	//var void interface{}
	//err := row.Scan(&void)
	//if err != nil {
	//	return &SQLError{sql, err}
	//}
	return nil
}

func populateGeometryColumn(tx *sql.Tx, tableName string, spec TableSpec) error {
	//Add SRID Constraint
	/*
		sql = fmt.Sprintf(`ALTER TABLE %s.%s
			ADD CONSTRAINT [enforce_srid_%s_%s]
			CHECK ([geometry] IS NULL OR [geometry].[STSrid] = '%d')`,
			spec.Schema, tableName, spec.Schema, tableName, spec.Srid)
		_, err = tx.Exec(sql)
		if err != nil {
			return &SQLError{sql, err}
		}
		//Add GeometryType Constraint
		sql = fmt.Sprintf(`ALTER TABLE %s.%s
			ADD CONSTRAINT [enforce_type_%s_%s]
			CHECK ([geometry] IS NULL OR [geometry].STGeometryType() = '%s')`,
			spec.Schema, tableName, spec.Schema, tableName, geomType)
		_, err = tx.Exec(sql)
		if err != nil {
			return &SQLError{sql, err}
		}
	*/

	/*sql := fmt.Sprintf("SELECT Populate_Geometry_Columns('%s.%s'::regclass);",
		spec.Schema, tableName)
	row := tx.QueryRow(sql)
	var void interface{}
	err := row.Scan(&void)
	if err != nil {
		return &SQLError{sql, err}
	}*/
	return nil
}

func (mssql *Mssql) createSchema(schema string) error {
	var sql string
	var err error

	if schema == "public" {
		//we cant create a schema named public on sql server...
		return fmt.Errorf("Can't use a schema named 'public'")
	}

	sql = fmt.Sprintf("SELECT CAST("+
		"CASE WHEN EXISTS(SELECT schema_name FROM information_schema.schemata WHERE schema_name = '%s')"+
		"THEN 1  ELSE 0  END AS BIT)",
		schema)

	row := mssql.Db.QueryRow(sql)
	var exists bool
	err = row.Scan(&exists)
	if err != nil {
		return &SQLError{sql, err}
	}
	if exists {
		return nil
	}

	sql = fmt.Sprintf("CREATE SCHEMA [%s]", schema)
	_, err = mssql.Db.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}
	return nil
}

// Init creates schema and tables, drops existing data.
func (mssql *Mssql) Init() error {

	if err := mssql.createSchema(mssql.Config.ImportSchema); err != nil {
		fmt.Printf("on err\n\n")
		return err
	}

	tx, err := mssql.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)
	for _, spec := range mssql.Tables {
		if err := createTable(tx, *spec); err != nil {
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil
	return nil
}

// Finish creates spatial indices on all tables.
func (mssql *Mssql) Finish() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating geometry indices")))

	worker := int(runtime.GOMAXPROCS(0))
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(mssql.Tables)+len(mssql.GeneralizedTables))
	for _, tbl := range mssql.Tables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return createIndex(mssql, tableName, table.Columns)
		}
	}

	for _, tbl := range mssql.GeneralizedTables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return createIndex(mssql, tableName, table.Source.Columns)
		}
	}

	err := p.wait()
	if err != nil {
		return err
	}

	return nil
}

func createIndex(mssql *Mssql, tableName string, columns []ColumnSpec) error {
	sql := fmt.Sprintf(`ALTER TABLE [%s].[%s]  ADD CONSTRAINT "PK_%s_id" PRIMARY KEY CLUSTERED (id) ON [PRIMARY]`,
		mssql.Config.ImportSchema, tableName, tableName)
	step := log.StartStep(fmt.Sprintf("Creating Primary key id index on %s", tableName))
	_, err := mssql.Db.Exec(sql)
	log.StopStep(step)
	if err != nil {
		log.StopStep(sql)
		return err
	}

	for _, col := range columns {
		if col.FieldType.Name == "id" {
			sql := fmt.Sprintf(`CREATE INDEX "%s_%s_idx" ON [%s].[%s](%s) ON [PRIMARY]`,
				tableName, col.Name, mssql.Config.ImportSchema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating OSM id index on %s", tableName))
			_, err := mssql.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				log.StopStep(sql)
				return err
			}
		}
		if col.Type.Name() == "GEOMETRY" {
			sql := fmt.Sprintf(`CREATE SPATIAL INDEX %s_geom ON %s.%s(%s) USING GEOMETRY_AUTO_GRID
			WITH( BOUNDING_BOX  = ( xmin  = -20037508.34, ymin  = -20037508.34, xmax  = 20037508.34, ymax  = 20037508.34), CELLS_PER_OBJECT  = 16, STATISTICS_NORECOMPUTE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON)`,
				tableName, mssql.Config.ImportSchema, tableName, col.Name)
			step := log.StartStep(fmt.Sprintf("Creating geometry index on %s", tableName))
			_, err := mssql.Db.Exec(sql)
			log.StopStep(step)
			if err != nil {
				log.StopStep(sql)
				return err
			}
		}

	}
	return nil
}

func (mssql *Mssql) GeneralizeUpdates() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Updating generalized tables")))
	for _, table := range mssql.sortedGeneralizedTables() {
		if ids, ok := mssql.updatedIds[table]; ok {
			for _, id := range ids {
				mssql.txRouter.Insert(table, []interface{}{id})
			}
		}
	}
	return nil
}

func (mssql *Mssql) Generalize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Creating generalized tables")))

	worker := int(runtime.GOMAXPROCS(0))
	if worker < 1 {
		worker = 1
	}
	// generalized tables can depend on other generalized tables
	// create tables with non-generalized sources first
	p := newWorkerPool(worker, len(mssql.GeneralizedTables))
	for _, table := range mssql.GeneralizedTables {
		if table.SourceGeneralized == nil {
			tbl := table // for following closure
			p.in <- func() error {
				if err := mssql.generalizeTable(tbl); err != nil {
					return err
				}
				tbl.created = true
				return nil
			}
		}
	}
	err := p.wait()
	if err != nil {
		return err
	}

	// next create tables with created generalized sources until
	// no new source is created
	created := int32(1)
	for created == 1 {
		created = 0

		p := newWorkerPool(worker, len(mssql.GeneralizedTables))
		for _, table := range mssql.GeneralizedTables {
			if !table.created && table.SourceGeneralized.created {
				tbl := table // for following closure
				p.in <- func() error {
					if err := mssql.generalizeTable(tbl); err != nil {
						return err
					}
					tbl.created = true
					atomic.StoreInt32(&created, 1)
					return nil
				}
			}
		}
		err := p.wait()
		if err != nil {
			return err
		}
	}
	return nil
}

func (mssql *Mssql) generalizeTable(table *GeneralizedTableSpec) error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Generalizing %s into %s",
		table.Source.FullName, table.FullName)))

	tx, err := mssql.Db.Begin()
	if err != nil {
		return err
	}
	defer rollbackIfTx(&tx)

	var where string
	if table.Where != "" {
		where = " WHERE " + table.Where
	}
	var cols []string

	for _, col := range table.Source.Columns {
		cols = append(cols, col.Type.GeneralizeSql(&col, table))
	}

	if err := dropTableIfExists(tx, mssql.Config.ImportSchema, table.FullName); err != nil {
		return err
	}

	columnSQL := strings.Join(cols, ",\n")

	var sourceTable string
	if table.SourceGeneralized != nil {
		sourceTable = table.SourceGeneralized.FullName
	} else {
		sourceTable = table.Source.FullName
	}

	sql := fmt.Sprintf(`SELECT id, %s INTO [%s].[%s] FROM [%s].[%s] %s`,
		columnSQL, mssql.Config.ImportSchema, table.FullName, mssql.Config.ImportSchema,
		sourceTable, where)

	_, err = tx.Exec(sql)
	if err != nil {
		return &SQLError{sql, err}
	}

	err = populateGeometryColumn(tx, table.FullName, *table.Source)
	if err != nil {
		return err
	}

	err = tx.Commit()
	if err != nil {
		return err
	}
	tx = nil // set nil to prevent rollback
	return nil
}

// Optimize clusters tables on new GeoHash index.
func (mssql *Mssql) Optimize() error {
	defer log.StopStep(log.StartStep(fmt.Sprintf("Clustering on geometry")))

	worker := int(runtime.GOMAXPROCS(0))
	if worker < 1 {
		worker = 1
	}

	p := newWorkerPool(worker, len(mssql.Tables)+len(mssql.GeneralizedTables))

	for _, tbl := range mssql.Tables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return clusterTable(mssql, tableName, table.Srid, table.Columns)
		}
	}
	for _, tbl := range mssql.GeneralizedTables {
		tableName := tbl.FullName
		table := tbl
		p.in <- func() error {
			return clusterTable(mssql, tableName, table.Source.Srid, table.Source.Columns)
		}
	}

	err := p.wait()
	if err != nil {
		return err
	}

	return nil
}

func clusterTable(mssql *Mssql, tableName string, srid int, columns []ColumnSpec) error {
	log.Print("mssql: clusterTable is not implemented")
	//sql := fmt.Sprintf(`UPDATE STATISTICS [%s].[%s]`, mssql.Config.ImportSchema, sourceTable)

	return nil
}

type Mssql struct {
	Db *sql.DB
	//Params                  string
	Config                  database.Config
	Tables                  map[string]*TableSpec
	GeneralizedTables       map[string]*GeneralizedTableSpec
	Prefix                  string
	txRouter                *TxRouter
	updateGeneralizedTables bool
	updatedIds              map[string][]int64
}

func (mssql *Mssql) Open() error {
	var err error

	mssql.Db, err = sql.Open("mssql", mssql.Config.ConnectionParams)
	if err != nil {
		return err
	}
	// check that the connection actually works
	err = mssql.Db.Ping()
	if err != nil {
		return err
	}
	return nil
}

func (mssql *Mssql) InsertPoint(elem element.OSMElem, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := mssql.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (mssql *Mssql) InsertLineString(elem element.OSMElem, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := mssql.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if mssql.updateGeneralizedTables {
		for _, generalizedTable := range mssql.generalizedFromMatches(matches) {
			mssql.updatedIds[generalizedTable.Name] = append(mssql.updatedIds[generalizedTable.Name], elem.Id)
		}
	}
	return nil
}

func (mssql *Mssql) InsertPolygon(elem element.OSMElem, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := mssql.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if mssql.updateGeneralizedTables {
		for _, generalizedTable := range mssql.generalizedFromMatches(matches) {
			mssql.updatedIds[generalizedTable.Name] = append(mssql.updatedIds[generalizedTable.Name], elem.Id)
		}
	}
	return nil
}

func (mssql *Mssql) InsertRelationMember(rel element.Relation, m element.Member, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.MemberRow(&rel, &m, &geom)
		if err := mssql.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (mssql *Mssql) Delete(id int64, matches interface{}) error {
	if matches, ok := matches.([]mapping.Match); ok {
		for _, match := range matches {
			mssql.txRouter.Delete(match.Table.Name, id)
		}
		if mssql.updateGeneralizedTables {
			for _, generalizedTable := range mssql.generalizedFromMatches(matches) {
				mssql.txRouter.Delete(generalizedTable.Name, id)
			}
		}
	}
	return nil
}

func (mssql *Mssql) DeleteElem(elem element.OSMElem) error {
	// handle deletes of geometries that did not match in ProbeXxx.
	// we have to handle multipolygon relations that took the tags of the
	// main-member. those tags are not avail. during delete. just try to
	// delete from each polygon/relation table.
	if _, ok := elem.Tags["type"]; ok {
		for _, tableSpec := range mssql.Tables {
			if tableSpec.GeometryType != "polygon" && tableSpec.GeometryType != "geometry" && tableSpec.GeometryType != "relation" {
				continue
			}
			mssql.txRouter.Delete(tableSpec.Name, elem.Id)
			if mssql.updateGeneralizedTables {
				for _, genTable := range tableSpec.Generalizations {
					mssql.txRouter.Delete(genTable.Name, elem.Id)
				}
			}
		}
	}
	return nil
}

func (mssql *Mssql) generalizedFromMatches(matches []mapping.Match) []*GeneralizedTableSpec {
	generalizedTables := []*GeneralizedTableSpec{}
	for _, match := range matches {
		tbl := mssql.Tables[match.Table.Name]
		generalizedTables = append(generalizedTables, tbl.Generalizations...)
	}
	return generalizedTables
}

func (mssql *Mssql) sortedGeneralizedTables() []string {
	added := map[string]bool{}
	sorted := []string{}

	for len(mssql.GeneralizedTables) > len(sorted) {
		for _, tbl := range mssql.GeneralizedTables {
			if _, ok := added[tbl.Name]; !ok {
				if tbl.Source != nil || added[tbl.SourceGeneralized.Name] {
					added[tbl.Name] = true
					sorted = append(sorted, tbl.Name)
				}
			}
		}
	}
	return sorted
}

func (mssql *Mssql) EnableGeneralizeUpdates() {
	mssql.updateGeneralizedTables = true
	mssql.updatedIds = make(map[string][]int64)
}

func (mssql *Mssql) Begin() error {
	var err error
	mssql.txRouter, err = newTxRouter(mssql, false)
	return err
}

func (mssql *Mssql) BeginBulk() error {
	var err error
	mssql.txRouter, err = newTxRouter(mssql, true)
	return err
}

func (mssql *Mssql) Abort() error {
	return mssql.txRouter.Abort()
}

func (mssql *Mssql) End() error {
	return mssql.txRouter.End()
}

func (mssql *Mssql) Close() error {
	return mssql.Db.Close()
}

func New(conf database.Config, m *mapping.Mapping) (database.DB, error) {
	db := &Mssql{}

	db.Tables = make(map[string]*TableSpec)
	db.GeneralizedTables = make(map[string]*GeneralizedTableSpec)

	db.Config = conf

	if strings.HasPrefix(db.Config.ConnectionParams, "mssql://") {
		db.Config.ConnectionParams = strings.Replace(
			db.Config.ConnectionParams,
			"mssql://", "", 1,
		)
	}

	db.Prefix = prefixFromConnectionParams(db.Config.ConnectionParams)

	for name, table := range m.Tables {
		db.Tables[name] = NewTableSpec(db, table)
	}
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = NewGeneralizedTableSpec(db, table)
	}
	db.prepareGeneralizedTableSources()
	db.prepareGeneralizations()

	err := db.Open()
	if err != nil {
		return nil, err
	}
	return db, nil
}

// prepareGeneralizedTableSources checks if all generalized table have an
// existing source and sets .Source to the original source (works even
// when source is allready generalized).
func (mssql *Mssql) prepareGeneralizedTableSources() {
	for name, table := range mssql.GeneralizedTables {
		if source, ok := mssql.Tables[table.SourceName]; ok {
			table.Source = source
		} else if source, ok := mssql.GeneralizedTables[table.SourceName]; ok {
			table.SourceGeneralized = source
		} else {
			log.Printf("missing source '%s' for generalized table '%s'\n",
				table.SourceName, name)
		}
	}

	// set source table until all generalized tables have a source
	for filled := true; filled; {
		filled = false
		for _, table := range mssql.GeneralizedTables {
			if table.Source == nil {
				if source, ok := mssql.GeneralizedTables[table.SourceName]; ok && source.Source != nil {
					table.Source = source.Source
				}
				filled = true
			}
		}
	}
}

func (mssql *Mssql) prepareGeneralizations() {
	for _, table := range mssql.GeneralizedTables {
		table.Source.Generalizations = append(table.Source.Generalizations, table)
		if source, ok := mssql.GeneralizedTables[table.SourceName]; ok {
			source.Generalizations = append(source.Generalizations, table)
		}
	}
}

func init() {
	database.Register("mssql", New)
	database.Register("mssql", New)
}
