package mssqlclrgeo

type Builder struct {
	g          Geometry
	Srid       uint32
	stackShape stack
}

type stack []*Shape

func NewBuilder() *Builder {
	b := &Builder{}
	b.g.Version = 1
	b.g.Properties.V = true
	return b
}

func (b *Builder) AddShape(shape_type SHAPE) {
	shape := &Shape{OpenGisType: shape_type}
	shape.FigureOffset = int32(len(b.g.Figures))

	if len(b.stackShape) == 0 {
		shape.ParentOffset = -1
	} else {
		shape.ParentOffset = int32(len(b.stackShape) - 1)
	}
	b.stackShape.Push(shape)
	b.g.Shapes = append(b.g.Shapes, *shape)

	if shape.OpenGisType > 7 {
		b.g.Version = 2
	}

}
func (b *Builder) CloseShape() {
	b.stackShape.Pop()
}

func (b *Builder) AddFeature(figureType FIGURE) {
	figure := &Figure{Attribute: figureType}

	figure.Offset = uint32(len(b.g.Points))
	b.g.Figures = append(b.g.Figures, *figure)
}

func (b *Builder) AddPoint(x float64, y float64, z float64, m float64) {
	point := &Point{X: x, Y: y, Z: z, M: m}
	b.g.Points = append(b.g.Points, *point)
}

func (b *Builder) Generate() (data []byte, err error) {
	b.g.SRID = int32(b.Srid)
	return WriteGeometry(b.g, false)
}

func (s stack) Empty() bool    { return len(s) == 0 }
func (s stack) Peek() *Shape   { return s[len(s)-1] }
func (s *stack) Push(i *Shape) { (*s) = append((*s), i) }
func (s *stack) Pop() *Shape {
	d := (*s)[len(*s)-1]
	(*s) = (*s)[:len(*s)-1]
	return d
}
