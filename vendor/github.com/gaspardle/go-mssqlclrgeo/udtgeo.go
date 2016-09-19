package mssqlclrgeo

type FIGURE uint8

const (
	FIGURE_INTERIOR_RING      FIGURE = 0x00
	FIGURE_STROKE             FIGURE = 0x01
	FIGURE_EXTERIOR_RING      FIGURE = 0x02
	FIGURE_V2_POINT           FIGURE = 0x00
	FIGURE_V2_LINE            FIGURE = 0x01
	FIGURE_V2_ARC             FIGURE = 0x02
	FIGURE_V2_COMPOSITE_CURVE FIGURE = 0x03
)

type SHAPE uint8

const (
	SHAPE_POINT               SHAPE = 0x01
	SHAPE_LINESTRING          SHAPE = 0x02
	SHAPE_POLYGON             SHAPE = 0x03
	SHAPE_MULTIPOINT          SHAPE = 0x04
	SHAPE_MULTILINESTRING     SHAPE = 0x05
	SHAPE_MULTIPOLYGON        SHAPE = 0x06
	SHAPE_GEOMETRY_COLLECTION SHAPE = 0x07
	//V2
	SHAPE_CIRCULAR_STRING SHAPE = 0x08
	SHAPE_COMPOUND_CURVE  SHAPE = 0x09
	SHAPE_CURVE_POLYGON   SHAPE = 0x0A
	SHAPE_FULL_GLOBE      SHAPE = 0x0B
)

type SEGMENT uint8

const (
	SEGMENT_LINE       SEGMENT = 0x00
	SEGMENT_ARC        SEGMENT = 0x01
	SEGMENT_FIRST_LINE SEGMENT = 0x02
	SEGMENT_FIRST_ARC  SEGMENT = 0x03
)

type Figure struct {
	Attribute FIGURE
	Offset    uint32
}

type Shape struct {
	ParentOffset int32
	FigureOffset int32
	OpenGisType  SHAPE

	index int
}

type Point struct {
	X float64
	Y float64
	Z float64
	M float64
}
type Segment struct {
	Type SEGMENT
}

type SerializationProperties struct {
	H bool //V2
	L bool
	P bool
	V bool
	M bool
	Z bool
}

type Geometry struct {
	SRID       int32
	Version    uint8
	Properties SerializationProperties

	Points   []Point
	Figures  []Figure
	Shapes   []Shape
	Segments []Segment
}

func (s *Shape) GetFigures(g Geometry) (figures []Figure) {
	offset := s.FigureOffset

	if len(g.Shapes) > s.index+1 {
		offsetEnd := g.Shapes[s.index+1].FigureOffset
		figures = g.Figures[offset:offsetEnd]

	} else {
		figures = g.Figures[offset:]
	}
	return
}

func (g *Geometry) GetShapes() (shapes []Shape) {
	return g.Shapes
}

func (g *Figure) getPoints() (points []Point) {

	return
}

func (s *Shape) getParent(g Geometry) (parent *Shape) {
	offset := s.ParentOffset

	if offset == 0 {
		return nil
	}

	parent = &g.Shapes[offset]

	return
}
func (g *Geometry) getFiguresFromShape(shapeIdx int) (figures []Figure) {
	offset := g.Shapes[shapeIdx].FigureOffset

	if len(g.Shapes) > shapeIdx+1 {
		offsetEnd := g.Shapes[shapeIdx+1].FigureOffset
		figures = g.Figures[offset:offsetEnd]

	} else {
		figures = g.Figures[offset:]
	}
	return
}
func (g *Geometry) getPointsFromFigure(figureIdx int) (points []Point) {
	offset := g.Figures[figureIdx].Offset

	if len(g.Figures) > figureIdx+1 {
		offsetEnd := g.Figures[figureIdx+1].Offset
		points = g.Points[offset:offsetEnd]
	} else {
		points = g.Points[offset:]
	}
	return
}
