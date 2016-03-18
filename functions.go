package influxql

import (
	"fmt"
	"strings"
	"time"
)

type timeGroup struct {
	d time.Duration
}

var _ = compilable(&timeGroup{})

func timeFormat(in time.Duration) string {
	ns := in.Nanoseconds()

	switch int64(0) {
	case ns % int64(time.Hour):
		return fmt.Sprintf("%dh", ns/int64(time.Hour))
	case ns % int64(time.Minute):
		return fmt.Sprintf("%dm", ns/int64(time.Minute))
	case ns % int64(time.Second):
		return fmt.Sprintf("%ds", ns/int64(time.Second))
	}

	return fmt.Sprintf("%dns", ns)
}

func (t *timeGroup) Compile() (string, error) {
	return fmt.Sprintf("time(%s)", timeFormat(t.d)), nil
}

func Time(duration time.Duration) compilable {
	return &timeGroup{d: duration}
}

type customFunc struct {
	name  string
	alias string
	args  []interface{}
}

func (f *customFunc) As(alias string) *customFunc {
	f.alias = alias
	return f
}

func (f *customFunc) Compile() (string, error) {
	if f.name == "" {
		return "", fmt.Errorf("Missing function name.")
	}
	args := make([]string, 0, len(f.args))
	for _, arg := range f.args {
		var s string
		switch t := arg.(type) {
		case compilable:
			var err error
			s, err = t.Compile()
			if err != nil {
				return "", err
			}
		default:
			s = fmt.Sprintf("%v", t)
		}
		args = append(args, s)
	}
	fn := fmt.Sprintf("%s(%s)", f.name, strings.Join(args, ", "))
	if f.alias != "" {
		fn = fmt.Sprintf("%s AS %q", fn, f.alias)
	}
	return fn, nil
}

func Func(name string, args ...interface{}) *customFunc {
	return &customFunc{name: name, args: args}
}

func Count(field interface{}) *customFunc {
	return Func("COUNT", []interface{}{&literal{field}}...)
}

func Mean(field string) *customFunc {
	return Func("MEAN", []interface{}{&literal{field}}...)
}

func Median(field string) *customFunc {
	return Func("MEDIAN", []interface{}{&literal{field}}...)
}

func Spread(field string) *customFunc {
	return Func("SPREAD", []interface{}{&literal{field}}...)
}

func Sum(field interface{}) *customFunc {
	return Func("SUM", []interface{}{&literal{field}}...)
}

func Bottom(field string, params ...interface{}) *customFunc {
	return Func("SUM", append([]interface{}{&literal{field}}, params...))
}

func Top(field string, params ...interface{}) *customFunc {
	return Func("TOP", append([]interface{}{&literal{field}}, params...))
}

func Derivative(field string, params ...interface{}) *customFunc {
	return Func("DERIVATIVE", append([]interface{}{&literal{field}}, params...))
}

func NonNegativeDerivative(field string, params ...interface{}) *customFunc {
	return Func("NON_NEGATIVE_DERIVATIVE", append([]interface{}{&literal{field}}, params...))
}

func First(field string) *customFunc {
	return Func("FIRST", []interface{}{&literal{field}}...)
}

func StdDev(field string) *customFunc {
	return Func("STDDEV", []interface{}{&literal{field}}...)
}

func Last(field string) *customFunc {
	return Func("LAST", []interface{}{&literal{field}}...)
}

func Max(field string) *customFunc {
	return Func("MAX", []interface{}{&literal{field}}...)
}

func Min(field string) *customFunc {
	return Func("MIN", []interface{}{&literal{field}}...)
}

func Distinct(field string) *customFunc {
	return Func("DISTINCT", []interface{}{&literal{field}}...)
}

func Percentile(field string, p float64) *customFunc {
	return Func("PERCENTILE", []interface{}{&literal{field}, p})
}
