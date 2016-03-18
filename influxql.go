package influxql

type Builder interface {
	Build() (string, error)
}

type compilable interface {
	Compile() (string, error)
}
