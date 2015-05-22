package mssqlclrgeo

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

func readPointsZ(buffer *bytes.Buffer, points []Point) (err error) {

	if len(points) < 1 {
		return
	}
	for _, point := range points {
		err := binary.Read(buffer, binary.LittleEndian, &point.Z)
		if err != nil {
			return err
		}
	}
	return nil
}

func readPointsM(buffer *bytes.Buffer, points []Point) (err error) {

	if len(points) < 1 {
		return
	}
	for _, point := range points {
		err := binary.Read(buffer, binary.LittleEndian, &point.M)
		if err != nil {
			return err
		}
	}
	return nil
}
func readPoints(buffer *bytes.Buffer, count uint32) (points []Point, err error) {

	if count < 1 {
		return points, nil
	}

	for i := 0; i < int(count); i++ {

		var point Point
		err = binary.Read(buffer, binary.LittleEndian, &point.X)
		if err != nil {
			return nil, err
		}
		err = binary.Read(buffer, binary.LittleEndian, &point.Y)
		if err != nil {
			return nil, err
		}
		points = append(points, point)
	}

	return points, nil
}

func readFigures(buffer *bytes.Buffer, count uint32, properties SerializationProperties) (figures []Figure, err error) {

	if count < 1 {
		return figures, nil
	}

	if properties.P || properties.L {
		figures = append(figures, Figure{Attribute: FIGURE_STROKE, Offset: 0})
	} else {
		for i := 0; i < int(count); i++ {

			var f Figure
			err = binary.Read(buffer, binary.LittleEndian, &f)
			if err != nil {
				return nil, err
			}

			figures = append(figures, f)
		}
	}
	return figures, nil
}
func readShapes(buffer *bytes.Buffer, count uint32, properties SerializationProperties) (shapes []Shape, err error) {

	if count < 1 {
		return shapes, nil
	}

	if properties.P {
		shapes = append(shapes, Shape{
			ParentOffset: -1,
			FigureOffset: 0,
			OpenGisType:  SHAPE_POINT})
	} else if properties.L {
		shapes = append(shapes, Shape{
			ParentOffset: -1,
			FigureOffset: 0,
			OpenGisType:  SHAPE_LINESTRING})
	} else {
		for i := 0; i < int(count); i++ {
			var s Shape
			err = binary.Read(buffer, binary.LittleEndian, &s.ParentOffset)
			err = binary.Read(buffer, binary.LittleEndian, &s.FigureOffset)
			err = binary.Read(buffer, binary.LittleEndian, &s.OpenGisType)
			s.index = i
			if err != nil {
				return nil, err
			}

			shapes = append(shapes, s)
		}
	}
	return shapes, nil
}
func readSegments(buffer *bytes.Buffer, count uint32) (segments []Segment, err error) {

	if count < 1 {
		return
	}

	for i := 0; i < int(count); i++ {
		var s Segment
		err = binary.Read(buffer, binary.LittleEndian, &s.Type)
		if err != nil {
			return nil, err
		}
		segments = append(segments, s)
	}

	return segments, nil
}

func ReadGeography(data []byte) (g *Geometry, err error) {
	return parseGeometry(data, true)
}

func ReadGeometry(data []byte) (g *Geometry, err error) {
	return parseGeometry(data, false)
}

func parseGeometry(data []byte, isGeography bool) (g *Geometry, err error) {
	g = &Geometry{}

	var numberOfPoints uint32
	var numberOfFigures uint32
	var numberOfShapes uint32
	var numberOfSegments uint32

	var buffer = bytes.NewBuffer(data[0:])

	err = binary.Read(buffer, binary.LittleEndian, &g.SRID)
	if err != nil {
		return nil, err
	}

	if isGeography == true {
		if g.SRID == -1 {
			return
		} else if g.SRID < 4210 || g.SRID > 4999 {
			return nil, fmt.Errorf("Invalid SRID for geography")
		}
	}

	//version
	err = binary.Read(buffer, binary.LittleEndian, &g.Version)
	if err != nil {
		return nil, err
	}
	if g.Version > 2 {
		return nil, fmt.Errorf("Version %d is not supported", g.Version)
	}

	//flags
	var flags uint8 = 0
	err = binary.Read(buffer, binary.LittleEndian, &flags)
	if err != nil {
		return nil, err
	}
	g.Properties.Z = (flags & (1 << 0)) != 0
	g.Properties.M = (flags & (1 << 1)) != 0
	g.Properties.V = (flags & (1 << 2)) != 0
	g.Properties.P = (flags & (1 << 3)) != 0
	g.Properties.L = (flags & (1 << 4)) != 0

	if g.Version == 2 {
		g.Properties.H = (flags & (1 << 5)) != 0
	}
	if g.Properties.P && g.Properties.L {
		return nil, fmt.Errorf("geometry data is invalid")
	}

	//points
	if g.Properties.P {
		numberOfPoints = 1
	} else if g.Properties.L {
		numberOfPoints = 2
	} else {
		err = binary.Read(buffer, binary.LittleEndian, &numberOfPoints)
		if err != nil {
			return nil, err
		}
	}
	g.Points, err = readPoints(buffer, numberOfPoints)
	if err != nil {
		return nil, err
	}

	if g.Properties.Z {
		err = readPointsZ(buffer, g.Points)
		if err != nil {
			return nil, err
		}
	}
	if g.Properties.M {
		err = readPointsM(buffer, g.Points)
		if err != nil {
			return nil, err
		}
	}

	//figures
	if g.Properties.P || g.Properties.L {
		numberOfFigures = 1
	} else {
		err = binary.Read(buffer, binary.LittleEndian, &numberOfFigures)
		if err != nil {
			return nil, err
		}
	}
	g.Figures, err = readFigures(buffer, numberOfFigures, g.Properties)
	if err != nil {
		return nil, err
	}

	//shapes
	if g.Properties.P || g.Properties.L {
		numberOfShapes = 1
	} else {
		err = binary.Read(buffer, binary.LittleEndian, &numberOfShapes)
		if err != nil {
			return nil, err
		}
	}
	g.Shapes, err = readShapes(buffer, numberOfShapes, g.Properties)
	if err != nil {
		return nil, err
	}

	//segments
	if g.Version == 2 {
		err = binary.Read(buffer, binary.LittleEndian, &numberOfSegments)
		if err != nil && err != io.EOF {
			return nil, err
		}
		g.Segments, err = readSegments(buffer, numberOfSegments)
		if err != nil {
			return nil, err
		}

	}

	return g, nil
}
