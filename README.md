Imposm 3 for SQL Server
=======================
[![Build status](https://ci.appveyor.com/api/projects/status/fesj0xxehhuk4wne/branch/sqlserver?svg=true)](https://ci.appveyor.com/project/gaspardle/imposm3/branch/sqlserver)

Import OpenStreetMap data into a SQL Server 2012/2014/2016 database.

Uses the go-mssqldb driver. Check https://github.com/denisenkom/go-mssqldb for the list of accepted connection parameters.

Support almost all features, such as: Custom schemas, diff, bulk import.

### Usage ###

    imposm3 import -connection mssql://Server=host\instance;User Id=user;Password=password;Database=database; \
        -mapping mapping.json -read /path/to/osm.pbf -write

### Other considerations ###
Hstore columns are stored in JSON in `nvarchar(max)` column.

Geometries are stored in a `geometry` column.

Make sure to use functions supported by SQL Server in your mapping.
Ex. `ST_Area(geometry)` is `geometry.STArea()`

You can still import into a PostGIS database with `postgis://`.

### Tests ###
Refer to the [Test](#test) section below. Set the database with `SQLHOST`,`SQLINSTANCE`, `SQLDATABASE`, `SQLUSER` and `SQLPASSWORD` environment variables.

Imposm 3
========

Imposm is an importer for OpenStreetMap data. It reads PBF files and
imports the data into PostgreSQL/PostGIS. It can also update the
DB from diff files.

It is designed to create databases that are optimized for rendering (i.e. generating tiles or for WMS services).

Imposm 3 is written in Go and it is a complete rewrite of the previous Python implementation.
Configurations/mappings and cache files are not compatible with Imposm 2, but they share a similar architecture.

The development of Imposm 3 was sponsored by [Omniscale](http://omniscale.com/). There are [commercial licenses available for Imposm](http://omniscale.com/opensource/soss) to support the long-term development of Imposm.
There is also commercial support available from Omniscale.


Features
--------

* High-performance
* Diff support
* Custom database schemas
* Generalized geometries


### In detail


- High performance:
  Parallel from the ground up. It distributes parsing and processing to all available CPU cores.

- Custom database schemas:
  Creates tables for different data types. This allows easier styling and better performance for rendering in WMS or tile services.

- Unify values:
  For example, the boolean values `1`, `on`, `true` and `yes` all become ``TRUE``.

- Filter by tags and values:
  Only import data you are going to render/use.

- Efficient nodes cache:
  It is necessary to store all nodes to build ways and relations. Imposm uses a file-based key-value database to cache this data.

- Generalized tables:
  Automatically creates tables with lower spatial resolutions, perfect for rendering large road networks in low resolutions.

- Limit to polygons:
  Limit imported geometries to polygons from Shapefiles or GeoJSON, for city/state/country imports.

- Easy deployment:
  Single binary with only runtime dependencies to common libs (GEOS, SQLite and LevelDB)

- Support for table namespace (PostgreSQL schema)


Performance
-----------

Imposm 3 is much faster than Imposm 2 and osm2pgsql:

* Makes full use of all available CPU cores
* Bulk inserts into PostgreSQL with `COPY FROM`
* Efficient intermediate cache for reduced IO load during ways and relations building


Some import times from a Hetzner EX 4S server (Intel i7-2600 CPU @ 3.40GHz, 32GB RAM and 2TB software RAID1 (2x2TB 7200rpm SATA disks)) for imports of a 20.5GB planet PBF (2013-06-14) with generalized tables:

* 6:30h in normal-mode
* 13h in diff-mode

osm2pgsql required between 2-8 days in a [similar benchmark (slide 7)](http://www.geofabrik.de/media/2012-09-08-osm2pgsql-performance.pdf) with a smaller planet PBF file (~15GB).

Benchmarks with SSD are TBD.

Import of Europe 11GB PBF with generalized tables:

* 2:20h in normal-mode


Current status
--------------

Imposm 3 is used in production but there is no official 3.0 release yet.

### Planned features ###

There are a few features we like to see in Imposm 3:

* Automatic download and import of differential files
* Support for other projections than EPSG:3857 or EPSG:4326
* Support for route relations
* Improved integration with tile servers (expiration of updated tiles)
* Custom field/filter functions
* Official releases with binaries for more platforms

There is no roadmap however, as the implementation of these features largely depends on external funding. There are [commercial licenses available for Imposm](http://omniscale.com/opensource/soss) if you like to help with this development.

Installation
------------

### Binary

There are no official releases, but you find development builds at <http://imposm.org/static/rel/>.
These builds are for x86 64bit Linux and require *no* further dependencies. Download, untar and start `imposm3`.
(Note: These binaries require glibc >= 2.15 at the moment. Ubuntu 12.04 is recent enough, Debian 7 not.)

### Source

There are some dependencies:

#### Compiler

You need [Go >=1.1](http://golang.org).

#### C/C++ libraries

Other dependencies are [libleveldb][], [libgeos][] and [protobuf][].
Imposm 3 was tested with recent versions of these libraries, but you might succeed with older versions.
GEOS >=3.2 is recommended, since it became much more robust when handling invalid geometries.
For best performance use [HyperLevelDB][libhyperleveldb] as an in-place replacement for libleveldb.


[libleveldb]: https://code.google.com/p/leveldb/
[libhyperleveldb]: https://github.com/rescrv/HyperLevelDB
[libgeos]: http://trac.osgeo.org/geos/
[protobuf]: https://code.google.com/p/protobuf/

#### Go libraries

Imposm3 uses the following libraries.

- <https://github.com/jmhodges/levigo>
- <https://github.com/golang/protobuf/proto>
- <https://github.com/golang/protobuf/protoc-gen-go>
- <https://github.com/lib/pq>

`go get` will fetch these, but you can also use [godep][] to use a provided (vendorized) set of these dependencies.

[godep]: https://github.com/tools/godep


#### Other

Fetching Imposm and the Go libraries requires [mercurial][] and [git][].

[mercurial]: http://mercurial.selenic.com/
[git]: http://git-scm.com/


#### Compile

Create a new [Go workspace](http://golang.org/doc/code.html):

    mkdir imposm
    cd imposm
    export GOPATH=`pwd`

Get Imposm 3 and all dependencies:

    go get github.com/omniscale/imposm3
    go install github.com/omniscale/imposm3

Done. You should now have an imposm3 binary in `$GOPATH/bin`.

Go compiles to static binaries and so Imposm 3 has no runtime dependencies to Go.
Just copy the `imposm3` binary to your server for deployment. The C/C++ libraries listed above are still required though.

##### Godep

Imposm contains a fixed set of the dependencies that are known to work. You need to install Imposm with [godep][] to compile with this set.

    git clone https://github.com/omniscale/imposm3 src/github.com/omniscale/imposm3
    cd src/github.com/omniscale/imposm3
    godep go install ./


Usage
-----

`imposm3` has multiple subcommands. Use `imposm3 import` for basic imports.

For a simple import:

    imposm3 import -connection postgis://user:password@host/database \
        -mapping mapping.json -read /path/to/osm.pbf -write

You need a JSON file with the target database mapping. See `example-mapping.json` to get an idea what is possible with the mapping.

Imposm creates all new tables inside the `import` table schema. So you'll have `import.osm_roads` etc. You can change the tables to the `public` schema:

    imposm3 import -connection postgis://user:passwd@host/database \
        -mapping mapping.json -deployproduction


You can write some options into a JSON configuration file:

    {
        "cachedir": "/var/local/imposm3",
        "mapping": "mapping.json",
        "connection": "postgis://user:password@localhost:port/database"
    }

To use that config:

    imposm3 import -config config.json [args...]

For more options see:

    imposm3 import -help


Note: TLS/SSL support is disabled by default due to the lack of renegotiation support in Go's TLS implementation. You can re-enable encryption by setting the `PGSSLMODE` environment variable or the `sslmode` connection option to `require` or `verify-full`, eg: `-connect postgis://host/dbname?sslmode=require`. You will need to disable renegotiation support on your server to prevent connection errors on larger imports. You can do this by setting `ssl_renegotiation_limit` to 0 in your PostgreSQL server configuration.


Documentation
-------------

The latest documentation can be found here: <http://imposm.org/docs/imposm3/latest/>

Support
-------

There is a [mailing list at Google Groups](http://groups.google.com/group/imposm) for all questions. You can subscribe by sending an email to: `imposm+subscribe@googlegroups.com`

For commercial support [contact Omniscale](http://omniscale.com/contact).

Development
-----------

The source code is available at: <https://github.com/omniscale/imposm3/>

You can report any issues at: <https://github.com/omniscale/imposm3/issues>

License
-------

Imposm 3 is released as open source under the Apache License 2.0. See LICENSE.

All dependencies included as source code are released under a BSD-ish license except the YAML package.
The YAML package is released as LGPL3 with an exception that permits static linking. See LICENSE.deps.

All dependencies included in binary releases are released under a BSD-ish license except the GEOS package.
The GEOS package is released as LGPL3 and is linked dynamically. See LICENSE.bin.


### Test ###

#### Unit tests ####

To run all unit tests:

    make test-unit

Or:

    godep go test ./...


#### System tests ####

There are system test that import and update OSM data and verify the database content.
You need `osmosis` to create the test PBF files.
There is a Makefile that creates all test files if necessary and then runs the test itself.

    make test

Call `make test-system` to skip the unit tests.

WARNING: It uses your local PostgeSQL database (`imposm3testimport`, `imposm3testproduction` and `imposm3testbackup` schema). Change the database with the standard `PGDATABASE`, `PGHOST`, etc. environment variables.
