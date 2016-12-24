package test

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"os"
	"strings"
	"testing"

	"github.com/omniscale/imposm3/element"

	"github.com/omniscale/imposm3/cache"

	"github.com/omniscale/imposm3/geom/geos"
	"github.com/omniscale/imposm3/update"

	"github.com/omniscale/imposm3/config"
	"github.com/omniscale/imposm3/import_"
)

const (
	dbschemaImport     = "imposm3testimport"
	dbschemaProduction = "imposm3testproduction"
	dbschemaBackup     = "imposm3testbackup"
)

type importConfig struct {
	connection      string
	osmFileName     string
	mappingFileName string
	cacheDir        string
	verbose         bool
	expireTileDir   string
}

type importTestSuite struct {
	dir    string
	config importConfig
	db     *sql.DB
	g      *geos.Geos
}

const Missing = ""

func getTestConnectionString() string {
	addr := os.Getenv("SQLHOST")
	instance := os.Getenv("SQLINSTANCE")
	user := os.Getenv("SQLUSER")
	password := os.Getenv("SQLPASSWORD")
	database := os.Getenv("SQLDATABASE")

	if len(strings.TrimSpace(instance)) > 0 {
		instance = "\\" + instance
	}
	return fmt.Sprintf(
		"Server=%s%s;User Id=%s;Password=%s;Database=%s;",
		addr, instance, user, password, database)

}
func (s *importTestSuite) importOsm(t *testing.T) {
	importArgs := []string{
		"-connection", s.config.connection,
		"-read", s.config.osmFileName,
		"-write",
		"-cachedir", s.config.cacheDir,
		"-diff",
		"-overwritecache",
		"-dbschema-import", dbschemaImport,
		// "-optimize",
		"-mapping", s.config.mappingFileName,
		"-quiet",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup=false",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) deployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction",
		"-removebackup=false",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-deployproduction",
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) revertDeployOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-revertdeploy",
		"-deployproduction=false",
		"-removebackup=false",
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) cache(t *testing.T) *cache.OSMCache {
	c := cache.NewOSMCache(s.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *importTestSuite) diffCache(t *testing.T) *cache.DiffCache {
	c := cache.NewDiffCache(s.config.cacheDir)
	if err := c.Open(); err != nil {
		t.Fatal(err)
	}
	return c
}

func (s *importTestSuite) removeBackupOsm(t *testing.T) {
	importArgs := []string{
		"-read=", // overwrite previous options
		"-write=false",
		"-optimize=false",
		"-revertdeploy=false",
		"-deployproduction=false",
		"-removebackup",
		"-connection", s.config.connection,
		"-dbschema-import", dbschemaImport,
		"-dbschema-production", dbschemaProduction,
		"-dbschema-backup", dbschemaBackup,
		"-mapping", s.config.mappingFileName,
		"-quiet",
	}

	config.ParseImport(importArgs)
	import_.Import()
}

func (s *importTestSuite) updateOsm(t *testing.T, diffFile string) {
	args := []string{
		"-connection", s.config.connection,
		"-cachedir", s.config.cacheDir,
		"-limitto", "clipping.geojson",
		"-dbschema-production", dbschemaProduction,
		"-mapping", s.config.mappingFileName,
	}
	if s.config.expireTileDir != "" {
		args = append(args, "-expiretiles-dir", s.config.expireTileDir)
	}
	args = append(args, diffFile)
	config.ParseDiffImport(args)
	update.Diff()
}

func (s *importTestSuite) dropSchemas() {
	dropSchemaSql := `EXEC sp_MSforeachtable "drop table ?", @whereand = ' And Object_id In (Select Object_id From sys.TABLES
Where schema_id = (select schema_id from sys.schemas where name = ''%s''))'
`
	var err error
	_, err = s.db.Exec(fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s') `+dropSchemaSql, dbschemaImport, dbschemaImport))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.db.Exec(fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s') `+dropSchemaSql, dbschemaProduction, dbschemaProduction))
	if err != nil {
		log.Fatal(err)
	}
	_, err = s.db.Exec(fmt.Sprintf(`IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s') `+dropSchemaSql, dbschemaBackup, dbschemaBackup))
	if err != nil {
		log.Fatal(err)
	}
}

func (s *importTestSuite) tableExists(t *testing.T, schema, table string) bool {
	row := s.db.QueryRow(fmt.Sprintf(`SELECT CAST(COUNT(*) AS BIT) FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s'`, table, schema))
	var exists bool
	if err := row.Scan(&exists); err != nil {
		t.Error(err)
		return false
	}
	return exists
}

type record struct {
	id      int
	name    string
	osmType string
	wkt     string
	missing bool
	tags    map[string]string
}

func (s *importTestSuite) query(t *testing.T, table string, id int64, keys []string) record {
	kv := make([]string, len(keys))

	for i, k := range keys {
		kv[i] = "\"" + k + "\": \"' + cast(coalesce(" + k + ", '') as nvarchar(max)) + '\""
	}
	columns := strings.Join(kv, ", ")
	if columns == "" {
		columns = "'{}'"
	} else {
		columns = "'{" + columns + "}'"
	}

	//columns = "(select " + strings.Join(kv, ", ") + " for json path)"
	stmt := fmt.Sprintf(`SELECT osm_id, name, type, geometry.STAsText(), %s as json FROM "%s"."%s" WHERE osm_id=$1`, columns, dbschemaProduction, table)

	row := s.db.QueryRow(stmt, id)
	r := record{}

	var col_wkt, col_name, col_osmtype, col_json []byte

	if err := row.Scan(&r.id, &col_name, &col_osmtype, &col_wkt, &col_json); err != nil {
		if err == sql.ErrNoRows {
			r.missing = true
		} else {
			t.Fatal(err)
		}
	}

	r.name = string(col_name)
	r.osmType = string(col_osmtype)
	r.wkt = string(col_wkt)

	//json
	r.tags = make(map[string]string)
	if !r.missing {
		tags_map := make(map[string]string)
		err := json.Unmarshal(col_json, &tags_map)
		if err != nil {
			t.Fatal(err)
		}
		r.tags = tags_map
	}
	return r
}

func (s *importTestSuite) queryTags(t *testing.T, table string, id int64) record {
	stmt := fmt.Sprintf(`SELECT osm_id, tags FROM "%s"."%s" WHERE osm_id=$1`, dbschemaProduction, table)
	row := s.db.QueryRow(stmt, id)
	r := record{}

	var col_json []byte

	if err := row.Scan(&r.id, &col_json); err != nil {
		if err == sql.ErrNoRows {
			r.missing = true
		} else {
			t.Fatal(err)
		}
	}

	r.tags = make(map[string]string)
	if !r.missing {
		tags_map := make(map[string]string)
		err := json.Unmarshal(col_json, &tags_map)
		if err != nil {
			t.Fatal(err)
		}
		r.tags = tags_map
	}
	return r
}

func (s *importTestSuite) queryRows(t *testing.T, table string, id int64) []record {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT osm_id, name, type, geometry.STAsText() FROM "%s"."%s" WHERE osm_id=$1 ORDER BY type, name, geometry.STGeometryType()`, dbschemaProduction, table), id)
	if err != nil {
		t.Fatal(err)
	}
	rs := []record{}
	for rows.Next() {
		var r record
		if err = rows.Scan(&r.id, &r.name, &r.osmType, &r.wkt); err != nil {
			t.Fatal(err)
		}
		rs = append(rs, r)
	}
	return rs
}

func (s *importTestSuite) queryRowsTags(t *testing.T, table string, id int64) []record {
	rows, err := s.db.Query(fmt.Sprintf(`SELECT osm_id, geometry.STAsText(), tags FROM "%s"."%s" WHERE osm_id=$1 ORDER BY geometry.STGeometryType()`, dbschemaProduction, table), id)
	if err != nil {
		t.Fatal(err)
	}
	rs := []record{}
	for rows.Next() {
		var r record
		var col_json []byte
		if err = rows.Scan(&r.id, &r.wkt, &col_json); err != nil {
			t.Fatal(err)
		}

		r.tags = make(map[string]string)
		tags_map := make(map[string]string)
		err = json.Unmarshal(col_json, &tags_map)
		if err != nil {
			t.Fatal(err)
		}
		r.tags = tags_map

		rs = append(rs, r)
	}
	return rs
}

func (s *importTestSuite) queryGeom(t *testing.T, table string, id int64) *geos.Geom {
	stmt := fmt.Sprintf(`SELECT osm_id, geometry.STAsText() FROM "%s"."%s" WHERE osm_id=$1`, dbschemaProduction, table)
	row := s.db.QueryRow(stmt, id)
	r := record{}
	if err := row.Scan(&r.id, &r.wkt); err != nil {
		if err == sql.ErrNoRows {
			r.missing = true
		} else {
			t.Fatal(err)
		}
	}
	g := geos.NewGeos()
	defer g.Finish()
	geom := g.FromWkt(r.wkt)
	if geom == nil {
		t.Fatalf("unable to read WKT for %d", id)
	}
	return geom
}

func (s *importTestSuite) queryDynamic(t *testing.T, table, where string) []map[string]string {

	//XXX
	keys := map[string]string{
		"wkt":  "geometry.STAsText()",
		"name": "name",
		"role": "role",
	}
	kv := make([]string, len(keys))

	i := 0
	for key, value := range keys {
		kv[i] = "\"" + key + "\": \"' + cast(coalesce(" + value + ", '') as nvarchar(max)) + '\""
		i = i + 1
	}
	columns := strings.Join(kv, ", ")
	columns = "'{" + columns + "}'"

	stmt := fmt.Sprintf(`SELECT %s FROM "%s"."%s" WHERE %s`, columns, dbschemaProduction, table, where)
	rows, err := s.db.Query(stmt)
	if err != nil {
		t.Fatal(err)
	}
	results := []map[string]string{}
	for rows.Next() {
		var col_json []byte
		if err := rows.Scan(&col_json); err != nil {
			t.Fatal(err)
		}

		r := make(map[string]string)
		err = json.Unmarshal(col_json, &r)
		if err != nil {
			t.Fatal(err)
		}

		results = append(results, r)
	}
	return results

}

type checkElem struct {
	table   string
	id      int64
	osmType string
	tags    map[string]string
}

func assertRecords(t *testing.T, elems []checkElem) {
	for _, e := range elems {
		keys := make([]string, 0, len(e.tags))
		for k, _ := range e.tags {
			keys = append(keys, k)
		}
		r := ts.query(t, e.table, e.id, keys)
		if e.osmType == "" {
			if r.missing {
				continue
			}
			t.Errorf("got unexpected record %d", r.id)
		}
		if r.osmType != e.osmType {
			t.Errorf("got unexpected type %s != %s for %d in %s", r.osmType, e.osmType, e.id, e.table)
		}
		for k, v := range e.tags {
			if r.tags[k] != v {
				t.Errorf("%s does not match for %d %s != %s", k, e.id, r.tags[k], v)
			}
		}
	}
}

func assertHstore(t *testing.T, elems []checkElem) {
	for _, e := range elems {
		r := ts.queryTags(t, e.table, e.id)
		if e.osmType == "" {
			if r.missing {
				continue
			}
			t.Errorf("got unexpected record %d", r.id)
		}
		if len(e.tags) != len(r.tags) {
			t.Errorf("tags for %d differ %v != %v", e.id, r.tags, e.tags)
		}
		for k, v := range e.tags {
			if r.tags[k] != v {
				t.Errorf("%s does not match for %d %s != %s", k, e.id, r.tags[k], v)
			}
		}
	}
}

func assertGeomValid(t *testing.T, e checkElem) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
}

func assertGeomArea(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Area()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func assertGeomLength(t *testing.T, e checkElem, expect float64) {
	geom := ts.queryGeom(t, e.table, e.id)
	if !ts.g.IsValid(geom) {
		t.Fatalf("geometry of %d is invalid", e.id)
	}
	actual := geom.Length()
	if math.Abs(expect-actual) > 1 {
		t.Errorf("unexpected size of %d %f!=%f", e.id, actual, expect)
	}
}

func assertGeomType(t *testing.T, e checkElem, expect string) {
	actual := ts.g.Type(ts.queryGeom(t, e.table, e.id))
	if actual != expect {
		t.Errorf("expected %s geometry for %d, got %s", expect, e.id, actual)
	}
}

func assertCachedWay(t *testing.T, c *cache.OSMCache, id int64) *element.Way {
	way, err := c.Ways.GetWay(id)
	if err == cache.NotFound {
		t.Errorf("missing way %d", id)
	} else if err != nil {
		t.Fatal(err)
	}
	if way.Id != id {
		t.Errorf("cached way contains invalid id, %d != %d", way.Id, id)
	}
	return way
}

func assertCachedNode(t *testing.T, c *cache.OSMCache, id int64) *element.Node {
	node, err := c.Nodes.GetNode(id)
	if err == cache.NotFound {
		node, err = c.Coords.GetCoord(id)
		if err == cache.NotFound {
			t.Errorf("missing node %d", id)
			return nil
		}
	} else if err != nil {
		t.Fatal(err)
	}
	if node.Id != id {
		t.Errorf("cached node contains invalid id, %d != %d", node.Id, id)
	}
	return node
}
