package sqlserver

import (
	"fmt"
)

type ColumnType interface {
	Name() string
	PrepareInsertSql(i int,
		spec *TableSpec) string
	GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string
}

type simpleColumnType struct {
	name string
}

func (t *simpleColumnType) Name() string {
	return t.name
}

func (t *simpleColumnType) PrepareInsertSql(i int, spec *TableSpec) string {
	return fmt.Sprintf("$%d", i)
}

func (t *simpleColumnType) GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string {
	return "\"" + colSpec.Name + "\""
}

type geometryType struct {
	name string
}

func (t *geometryType) Name() string {
	return t.name
}

func (t *geometryType) PrepareInsertSql(i int, spec *TableSpec) string {

	return fmt.Sprintf("cast(convert(varbinary(max), $%d, 2) as geometry)",
		i,
	)
}

func (t *geometryType) GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string {
	//return fmt.Sprintf(`ST_SimplifyPreserveTopology("%s", %f) as "%s"`,
	return fmt.Sprintf(`[%s].MakeValid().Reduce(%f) as "%s"`,
		colSpec.Name, spec.Tolerance, colSpec.Name,
	)
}

type validatedGeometryType struct {
	geometryType
}

func (t *validatedGeometryType) GeneralizeSql(colSpec *ColumnSpec, spec *GeneralizedTableSpec) string {
	//return fmt.Sprintf(`ST_Buffer(ST_SimplifyPreserveTopology("%s", %f), 0) as "%s"`,
	if spec.Source.GeometryType != "polygon" {
		// TODO return warning earlier
		log.Warnf("validated_geometry column returns polygon geometries for %s", spec.FullName)
	}
	return fmt.Sprintf(`[%s].Reduce(%f).STBuffer(0) as "%s"`,
		colSpec.Name, spec.Tolerance, colSpec.Name,
	)
}

var mssqlTypes map[string]ColumnType

func init() {
	mssqlTypes = map[string]ColumnType{
		"string":             &simpleColumnType{"NVARCHAR(255)"},
		"bool":               &simpleColumnType{"BIT"},
		"int8":               &simpleColumnType{"SMALLINT"},
		"int32":              &simpleColumnType{"INT"},
		"int64":              &simpleColumnType{"BIGINT"},
		"float32":            &simpleColumnType{"REAL"},
		"hstore_string":      &simpleColumnType{"NVARCHAR(max)"},
		"geometry":           &geometryType{"GEOMETRY"},
		"validated_geometry": &validatedGeometryType{geometryType{"GEOMETRY"}},
	}
}
