import math
import tempfile
import shutil
import subprocess
import os
#import psycopg2
#import psycopg2.extras
import pytds
import json
from  shapely.wkb import loads as wkb_loads
from shapely.geometry import Point
import binascii

import unittest

__all__ = [
    "assert_almost_equal",
    "query_row",
    "cache_query",
    "merc_point",
    "imposm3_import",
    "imposm3_deploy",
    "imposm3_update",
    "imposm3_revert_deploy",
    "imposm3_remove_backups",
    "table_exists",
    "drop_schemas",
    "TEST_SCHEMA_IMPORT",
    "TEST_SCHEMA_PRODUCTION",
    "TEST_SCHEMA_BACKUP",
    "db_conf",
    "assert_missing_node",
    "assert_cached_node",
    "assert_cached_way",
]

class Dummy(unittest.TestCase):
    def nop():
        pass
_t = Dummy('nop')
assert_almost_equal = _t.assertAlmostEqual

db_conf = {
    'server': '(local)\SQL2014',
    'as_dict': True,
    'database': 'osm',
    'user': 'osm',
    'password': 'osm'
}

TEST_SCHEMA_IMPORT = "imposm3testimport"
TEST_SCHEMA_PRODUCTION = "imposm3testpublic"
TEST_SCHEMA_BACKUP = "imposm3testbackup"

def pg_db_url(db_conf):
    return 'mssql://server=%(server)s;id=%(user)s;password=%(password)s;database=%(database)s;' % db_conf

def query_row(db_conf, table, osmid):
    conn = _test_connection(db_conf)
    #cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute('select * from %s.%s where osm_id = %%s' % (TEST_SCHEMA_PRODUCTION, table), [osmid])
    results = []
    for row in cur.fetchall():
        create_geom_in_row(row)
        results.append(row)
    cur.close()

    if not results:
        return None
    if len(results) == 1:
        return results[0]
    return results

def query_duplicates(db_conf, table):
    conn = _test_connection(db_conf)
    #cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur = conn.cursor()
    cur.execute('select osm_id, count(osm_id) from %s.%s group by osm_id having count(osm_id) > 1' % (TEST_SCHEMA_PRODUCTION, table))
    results = []
    for row in cur.fetchall():
        results.append(row)
    cur.close()
    return results

def _test_connection(db_conf):
    if '_connection' in db_conf:
        return db_conf['_connection']
    db_conf['_connection'] = pytds.connect(**db_conf)
    return db_conf['_connection']

def _close_test_connection(db_conf):
    if '_connection' in db_conf:
        db_conf['_connection'].close()
        del db_conf['_connection']

def table_exists(table, schema=TEST_SCHEMA_IMPORT):
    conn = _test_connection(db_conf)
    cur = conn.cursor()
    cur.execute("SELECT CAST(COUNT(*) AS BIT) FROM information_schema.tables WHERE table_name='%s' AND table_schema='%s'"
        % (table, schema))

    exists = cur.fetchone()[0]
    cur.close()
    return exists

def drop_schemas():
    conn = _test_connection(db_conf)
    cur = conn.cursor()
    cur.execute("IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')	DROP SCHEMA %s" % (TEST_SCHEMA_IMPORT, TEST_SCHEMA_IMPORT))
    cur.execute("IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')	DROP SCHEMA %s" % (TEST_SCHEMA_PRODUCTION,TEST_SCHEMA_PRODUCTION))
    cur.execute("IF EXISTS (SELECT 1 FROM sys.schemas WHERE name = '%s')	DROP SCHEMA %s" % (TEST_SCHEMA_BACKUP,TEST_SCHEMA_BACKUP))
    conn.commit()

