package mssqlclrgeo

type Builder struct {
	g           Geometry
	Srid        uint32
	stackShape  stack
	isGeography bool
}

type stack []*Shape

func NewBuilder(isGeography bool) *Builder {
	b := &Builder{}
	b.g.Version = 1
	b.g.Properties.V = true
	b.isGeography = isGeography
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
func isClockwise(vertices []Point) bool {
	if len(vertices) < 3 {
		return false
	}

	var area float64

	for i := 0; i < len(vertices); i++ {
		j := (i + 1) % len(vertices)
		area += (vertices[j].X - vertices[i].X) * (vertices[j].Y + vertices[i].Y)
	}

	return (area > 0)
}
func (b *Builder) AddFigure(figureType FIGURE) {
	figure := &Figure{Attribute: figureType}
	figure.Offset = uint32(len(b.g.Points))
	b.g.Figures = append(b.g.Figures, *figure)
}

func (b *Builder) EndFigure() {
	if b.isGeography && true {
		points_fig := b.g.Points[b.g.Figures[len(b.g.Figures)-1].Offset:]
		if isClockwise(points_fig) {
			//reverse points
			for i := len(points_fig)/2 - 1; i >= 0; i-- {
				opp := len(points_fig) - 1 - i
				points_fig[i], points_fig[opp] = points_fig[opp], points_fig[i]
			}
		}
		//TODO check for self intersection
	}
}
func (b *Builder) AddPoint(x float64, y float64, z float64, m float64) {
	point := &Point{X: x, Y: y, Z: z, M: m}
	b.g.Points = append(b.g.Points, *point)
}

func (b *Builder) Generate() (data []byte, err error) {
	b.g.SRID = int32(b.Srid)
	return WriteGeometry(b.g, b.isGeography)
}

func (s stack) Empty() bool    { return len(s) == 0 }
func (s stack) Peek() *Shape   { return s[len(s)-1] }
func (s *stack) Push(i *Shape) { (*s) = append((*s), i) }
func (s *stack) Pop() *Shape {
	d := (*s)[len(*s)-1]
	(*s) = (*s)[:len(*s)-1]
	return d
}
