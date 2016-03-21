package influxql

// Builder represents any struct that can be compiled into InfluxQL.
type Builder interface {
	Build() (string, error)
}
