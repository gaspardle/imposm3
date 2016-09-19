package mssqlclrgeo

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

func btou(b bool) uint8 {
	if b {
		return 1
	}
	return 0
}

func writePoints(buffer *bytes.Buffer, points []Point, isGeography bool) (err error) {

	for _, point := range points {
		if isGeography {
			err = binary.Write(buffer, binary.LittleEndian, &point.Y)
			if err != nil {
				return err
			}
			err = binary.Write(buffer, binary.LittleEndian, &point.X)
			if err != nil {
				return err
			}

		} else {
			err = binary.Write(buffer, binary.LittleEndian, &point.X)
			if err != nil {
				return err
			}
			err = binary.Write(buffer, binary.LittleEndian, &point.Y)
			if err != nil {
				return err
			}
		}

	}
	return
}
func writePointsZ(buffer *bytes.Buffer, points []Point) (err error) {

	for _, point := range points {
		err := binary.Write(buffer, binary.LittleEndian, &point.Z)
		if err != nil {
			return err
		}
	}
	return nil
}

func writePointsM(buffer *bytes.Buffer, points []Point) (err error) {

	for _, point := range points {
		err := binary.Write(buffer, binary.LittleEndian, &point.M)
		if err != nil {
			return err
		}
	}
	return nil
}

func writeFigures(buffer *bytes.Buffer, figures []Figure, properties SerializationProperties) (err error) {

	if properties.P || properties.L {

	} else {
		for _, f := range figures {
			err = binary.Write(buffer, binary.LittleEndian, &f)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeShapes(buffer *bytes.Buffer, shapes []Shape, properties SerializationProperties) (err error) {

	if properties.P || properties.L {

	} else {
		for _, s := range shapes {

			err = binary.Write(buffer, binary.LittleEndian, &s.ParentOffset)
			err = binary.Write(buffer, binary.LittleEndian, &s.FigureOffset)
			err = binary.Write(buffer, binary.LittleEndian, &s.OpenGisType)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func writeSegments(buffer *bytes.Buffer, segments []Segment) (err error) {

	for _, s := range segments {
		err = binary.Write(buffer, binary.LittleEndian, &s.Type)
		if err != nil {
			return err
		}
	}

	return nil
}

func WriteGeometry(g Geometry, isGeography bool) (data []byte, err error) {

	var numberOfPoints uint32 = 0
	var numberOfFigures uint32 = 0
	var numberOfShapes uint32 = 0
	var numberOfSegments uint32 = 0

	numberOfFigures = uint32(len(g.Figures))
	numberOfShapes = uint32(len(g.Shapes))
	numberOfPoints = uint32(len(g.Points))

	var buffer = bytes.NewBuffer(make([]byte, 0))

	err = binary.Write(buffer, binary.LittleEndian, &g.SRID)
	if err != nil {
		return nil, err
	}

	if isGeography == true {
		if g.SRID == -1 {
			return
		} else if g.SRID < 4210 || g.SRID > 4999 {
			return nil, fmt.Errorf("Invalid Srid for geography")
		}
	}

	//version
	err = binary.Write(buffer, binary.LittleEndian, &g.Version)
	if err != nil {
		return nil, err
	}

	//flags
	if numberOfPoints == 1 && numberOfFigures == 1 && numberOfShapes == 1 {
		g.Properties.P = true
	}
	if numberOfPoints == 2 && numberOfFigures == 1 && numberOfShapes == 1 {
		g.Properties.L = true
	}
	var flags uint8 = 0
	flags = flags | (btou(g.Properties.Z) << 0)
	flags = flags | (btou(g.Properties.M) << 1)
	flags = flags | (btou(g.Properties.V) << 2)
	flags = flags | (btou(g.Properties.P) << 3)
	flags = flags | (btou(g.Properties.L) << 4)

	if g.Version == 2 {
		flags = flags | (btou(g.Properties.H) << 5)
	}
	err = binary.Write(buffer, binary.LittleEndian, &flags)
	if err != nil {
		return nil, err
	}

	//points
	if g.Properties.P == false && g.Properties.L == false {
		err = binary.Write(buffer, binary.LittleEndian, &numberOfPoints)
		if err != nil {
			return nil, err
		}
	}

	err = writePoints(buffer, g.Points, isGeography)
	if err != nil {
		return nil, err
	}

	if g.Properties.Z {
		err = writePointsZ(buffer, g.Points)
		if err != nil {
			return nil, err
		}
	}
	if g.Properties.M {
		err = writePointsM(buffer, g.Points)
		if err != nil {
			return nil, err
		}
	}

	//figures
	if g.Properties.P == false && g.Properties.L == false {
		err = binary.Write(buffer, binary.LittleEndian, &numberOfFigures)
		if err != nil {
			return nil, err
		}

		err = writeFigures(buffer, g.Figures, g.Properties)
		if err != nil {
			return nil, err
		}
	}

	//shapes
	if g.Properties.P == false && g.Properties.L == false {
		err = binary.Write(buffer, binary.LittleEndian, &numberOfShapes)
		if err != nil {
			return nil, err
		}
		err = writeShapes(buffer, g.Shapes, g.Properties)
		if err != nil {
			return nil, err
		}
	}

	//segments
	if g.Version == 2 {
		numberOfSegments = uint32(len(g.Segments))
		if numberOfSegments > 0 {
			err = binary.Write(buffer, binary.LittleEndian, &numberOfSegments)
			if err != nil {
				return nil, err
			}
			err = writeSegments(buffer, g.Segments)
			if err != nil {
				return nil, err
			}
		}
	}

	return buffer.Bytes(), nil
}
