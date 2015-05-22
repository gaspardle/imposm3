package mssqlclrgeo

import (
	"bytes"

	"fmt"
)

// http://edndoc.esri.com/arcsde/9.1/general_topics/wkb_representation.htm
// https://github.com/postgis/postgis/blob/2.1.0/doc/ZMSgeoms.txt
// http://www.opengeospatial.org/standards/sfs

type wkbByteOrder uint8

const (
	wkbXDR wkbByteOrder = 0 // Big Endian
	wkbNDR wkbByteOrder = 1 // Little Endian
)

const wkbZ uint32 = 0x80000000
const wkbM uint32 = 0x40000000
const wkbSRID uint32 = 0x20000000

type wkbGeometryType uint32

const (
	typeWkbPoint              wkbGeometryType = 1
	typeWkbLineString         wkbGeometryType = 2
	typeWkbPolygon            wkbGeometryType = 3
	typeWkbMultiPoint         wkbGeometryType = 4
	typeWkbMultiLineString    wkbGeometryType = 5
	typeWkbMultiPolygon       wkbGeometryType = 6
	typeWkbGeometryCollection wkbGeometryType = 7
	typeWkbCircularString     wkbGeometryType = 8
	typeWkbCompoundCurve      wkbGeometryType = 9
	typeWkbCurvePolygon       wkbGeometryType = 10
	typeWkbMultiCurve         wkbGeometryType = 11
)

type linearRing struct {
	numPoints uint32
	points    []Point
}
type wkbPoint struct {
	wkbGeometry WkbGeometry
	point       Point
}
type wkbLineString struct {
	wkbGeometry WkbGeometry
	numPoints   uint32
	points      []Point
}
type wkbPolygon struct {
	wkbGeometry WkbGeometry
	numRings    uint32
	rings       []linearRing
}
type wkbMultiPoint struct {
	wkbGeometry   WkbGeometry
	num_wkbPoints uint32
	wkbPoints     []wkbPoint
}
type wkbMultiLineString struct {
	wkbGeometry        WkbGeometry
	num_wkbLineStrings uint32
	wkbLineStrings     []wkbLineString
}
type wkbMultiPolygon struct {
	wkbGeometry     WkbGeometry
	num_wkbPolygons uint32
	wkbPolygons     []wkbPolygon
}
type wkbGeometryCollection struct {
	wkbGeometry       WkbGeometry
	num_wkbGeometries uint32
	wkbGeometries     []interface{}
}
type wkbCircularString struct {
	wkbGeometry WkbGeometry
	numPoints   uint32
	points      []Point
}

type WkbGeometry struct {
	byteOrder wkbByteOrder
	wkbType   wkbGeometryType
	srid      uint32
	hasZ      bool
	hasM      bool
	hasSRID   bool
}

func ParseWkb(data []byte) (g interface{}, err error) {

	var buffer = bytes.NewBuffer(data[0:])
	geom, err := readWKBGeometry(buffer)

	return geom, err

}

func WkbToUdtGeo(data_wkb []byte) (data_udt []byte, err error) {

	geom, err := ParseWkb(data_wkb)
	if err != nil {
		return nil, err
	}

	b := NewBuilder()

	err = buildType(geom, b)
	if err != nil {
		return nil, err
	}

	return b.Generate()
}

func buildType(geom interface{}, b *Builder) (err error) {

	switch wkbGeom := geom.(type) {

	case *wkbPoint:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_POINT)
		b.AddFeature(FIGURE_STROKE)
		b.AddPoint(wkbGeom.point.X, wkbGeom.point.Y, wkbGeom.point.Z, wkbGeom.point.M)
		b.CloseShape()

	case *wkbLineString:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_LINESTRING)
		b.AddFeature(FIGURE_STROKE)
		for _, point := range wkbGeom.points {
			b.AddPoint(point.X, point.Y, point.Z, point.M)
		}
		b.CloseShape()

	case *wkbPolygon:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_POLYGON)
		for idx, ring := range wkbGeom.rings {
			if idx == 0 {
				b.AddFeature(FIGURE_EXTERIOR_RING)
			} else {
				b.AddFeature(FIGURE_INTERIOR_RING)
			}
			for _, point := range ring.points {
				b.AddPoint(point.X, point.Y, point.Z, point.M)
			}
		}
		b.CloseShape()

	case *wkbMultiPoint:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_MULTIPOINT)
		for _, p := range wkbGeom.wkbPoints {
			b.AddShape(SHAPE_POINT)
			b.AddFeature(FIGURE_STROKE)
			b.AddPoint(p.point.X, p.point.Y, p.point.Z, p.point.M)
			b.CloseShape()
		}
		b.CloseShape()

	case *wkbMultiLineString:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_MULTILINESTRING)
		for _, linestring := range wkbGeom.wkbLineStrings {
			b.AddShape(SHAPE_LINESTRING)
			b.AddFeature(FIGURE_STROKE)
			for _, point := range linestring.points {
				b.AddPoint(point.X, point.Y, point.Z, point.M)
			}
			b.CloseShape()
		}
		b.CloseShape()

	case *wkbMultiPolygon:

		b.AddShape(SHAPE_MULTIPOLYGON)
		for _, polygon := range wkbGeom.wkbPolygons {
			buildType(&polygon, b)
		}
		b.CloseShape()
		b.Srid = wkbGeom.wkbGeometry.srid

	case *wkbGeometryCollection:
		b.AddShape(SHAPE_GEOMETRY_COLLECTION)
		for _, coll_geom := range wkbGeom.wkbGeometries {
			err := buildType(coll_geom, b)
			if err != nil {
				return err
			}
		}
		b.CloseShape()
		b.Srid = wkbGeom.wkbGeometry.srid

	case *wkbCircularString:
		b.Srid = wkbGeom.wkbGeometry.srid
		b.AddShape(SHAPE_CIRCULAR_STRING)

		b.AddFeature(FIGURE_V2_ARC)
		for _, point := range wkbGeom.points {
			b.AddPoint(point.X, point.Y, point.Z, point.M)
		}
		b.CloseShape()

	default:
		return fmt.Errorf("Type not implemented")

	}
	return nil

}
