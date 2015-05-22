package mssqlclrgeo

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func readPoint(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p Point, err error) {

	err = binary.Read(buffer, order, &p.X)
	if err != nil {
		return
	}

	err = binary.Read(buffer, order, &p.Y)
	if err != nil {
		return
	}

	if g.hasZ {
		err = binary.Read(buffer, order, &p.Z)
		if err != nil {
			return
		}
	}
	if g.hasM {
		err = binary.Read(buffer, order, &p.M)
		if err != nil {
			return
		}
	}

	return
}
func readWkbPoint(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbPoint, err error) {
	p = &wkbPoint{wkbGeometry: *g}

	p.point, err = readPoint(buffer, order, g)
	if err != nil {
		return nil, err
	}
	return
}
func readLineString(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbLineString, err error) {
	p = &wkbLineString{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.numPoints)
	if err != nil {
		return nil, err
	}

	p.points = make([]Point, p.numPoints)
	for i := 0; i < int(p.numPoints); i++ {
		point, err := readPoint(buffer, order, g)
		if err != nil {
			return nil, err
		}

		p.points[i] = point
	}
	return
}
func readCircularString(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbCircularString, err error) {
	p = &wkbCircularString{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.numPoints)
	if err != nil {
		return nil, err
	}

	p.points = make([]Point, p.numPoints)
	for i := 0; i < int(p.numPoints); i++ {
		point, err := readPoint(buffer, order, g)
		if err != nil {
			return nil, err
		}

		p.points[i] = point
	}
	return
}
func readMultiPoint(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbMultiPoint, err error) {
	p = &wkbMultiPoint{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.num_wkbPoints)
	if err != nil {
		return
	}

	p.wkbPoints = make([]wkbPoint, p.num_wkbPoints)
	for i := 0; i < int(p.num_wkbPoints); i++ {
		point, err := readWKBGeometry(buffer)
		if err != nil {
			return nil, err
		}
		p.wkbPoints[i] = *(point.(*wkbPoint))
	}
	return
}
func readMultiLineString(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbMultiLineString, err error) {
	p = &wkbMultiLineString{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.num_wkbLineStrings)
	if err != nil {
		return
	}

	p.wkbLineStrings = make([]wkbLineString, p.num_wkbLineStrings)
	for i := 0; i < int(p.num_wkbLineStrings); i++ {
		linestring, err := readWKBGeometry(buffer)
		if err != nil {
			return nil, err
		}
		p.wkbLineStrings[i] = *(linestring.(*wkbLineString))
	}
	return
}

func readMultiPolygon(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbMultiPolygon, err error) {
	p = &wkbMultiPolygon{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.num_wkbPolygons)
	if err != nil {
		return
	}

	p.wkbPolygons = make([]wkbPolygon, p.num_wkbPolygons)
	for i := 0; i < int(p.num_wkbPolygons); i++ {
		polygon, err := readWKBGeometry(buffer)
		if err != nil {
			return nil, err
		}
		p.wkbPolygons[i] = *(polygon.(*wkbPolygon))
	}
	return
}

func readGeometryCollection(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbGeometryCollection, err error) {
	p = &wkbGeometryCollection{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.num_wkbGeometries)
	if err != nil {
		return
	}

	p.wkbGeometries = make([]interface{}, p.num_wkbGeometries)
	for i := 0; i < int(p.num_wkbGeometries); i++ {
		geom, err := readWKBGeometry(buffer)
		if err != nil {
			return nil, err
		}
		p.wkbGeometries[i] = (geom)
	}
	return

}

func readPolygon(buffer *bytes.Buffer, order binary.ByteOrder, g *WkbGeometry) (p *wkbPolygon, err error) {
	p = &wkbPolygon{wkbGeometry: *g}

	err = binary.Read(buffer, order, &p.numRings)
	if err != nil {
		return nil, err
	}

	p.rings = make([]linearRing, p.numRings)

	for i := 0; i < int(p.numRings); i++ {

		var ring linearRing
		err = binary.Read(buffer, order, &ring.numPoints)
		if err != nil {
			return nil, err
		}

		ring.points = make([]Point, ring.numPoints)
		for j := 0; j < int(ring.numPoints); j++ {
			point, err := readPoint(buffer, order, g)
			if err != nil {
				return nil, err
			}
			ring.points[j] = point
			//points = append(points, point)
		}
		p.rings[i] = ring
	}
	return p, nil
}

func readHeader(buffer *bytes.Buffer) (g WkbGeometry, order binary.ByteOrder, err error) {

	err = binary.Read(buffer, binary.LittleEndian, &g.byteOrder)
	if err != nil {
		return g, nil, err
	}
	if g.byteOrder == wkbNDR {
		order = binary.LittleEndian
	} else if g.byteOrder == wkbXDR {
		order = binary.BigEndian
	} else {
		return g, nil, fmt.Errorf("Invalid byte order value")
	}

	//Geometry type
	err = binary.Read(buffer, order, &g.wkbType)
	if err != nil {
		return g, nil, err
	}

	g.hasZ = (uint32(g.wkbType) & wkbZ) != 0
	g.hasM = (uint32(g.wkbType) & wkbM) != 0
	g.hasSRID = (uint32(g.wkbType) & wkbSRID) != 0
	g.wkbType = wkbGeometryType(uint32(g.wkbType) &^ (wkbM | wkbZ | wkbSRID))

	if g.hasSRID {
		err = binary.Read(buffer, order, &g.srid)
		if err != nil {
			return g, nil, err
		}
	}

	/*fmt.Printf("geometryType: %d\n", g.wkbType)
	fmt.Printf("hasSRID: %t\n", g.hasSRID)
	fmt.Printf("hasZ: %t\n", g.hasZ)
	fmt.Printf("hasM: %t\n", g.hasM)
	fmt.Printf("SRID: : %d\n", g.srid)*/
	return
}

func readWKBGeometry(buffer *bytes.Buffer) (geom interface{}, err error) {
	g, order, err := readHeader(buffer)
	if err != nil {
		return g, err
	}

	switch g.wkbType {
	case typeWkbPoint:
		geom, _ = readWkbPoint(buffer, order, &g)
	case typeWkbLineString:
		geom, _ = readLineString(buffer, order, &g)
	case typeWkbPolygon:
		geom, _ = readPolygon(buffer, order, &g)
	case typeWkbMultiPoint:
		geom, _ = readMultiPoint(buffer, order, &g)
	case typeWkbMultiLineString:
		geom, _ = readMultiLineString(buffer, order, &g)
	case typeWkbMultiPolygon:
		geom, _ = readMultiPolygon(buffer, order, &g)
	case typeWkbGeometryCollection:
		geom, _ = readGeometryCollection(buffer, order, &g)
	case typeWkbCircularString:
		geom, _ = readCircularString(buffer, order, &g)
	}

	return
}
