package influxql

import (
	"fmt"
	"strings"
	"time"
)

type timeGroup struct {
	d time.Duration
}

var _ = Builder(&timeGroup{})

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

// Build satisfies Builder.
func (t *timeGroup) Build() (string, error) {
	return fmt.Sprintf("time(%s)", timeFormat(t.d)), nil
}

// Time represents a time(duration) function.
func Time(duration time.Duration) Builder {
	return &timeGroup{d: duration}
}

// F represents a function.
type F struct {
	name  string
	alias string
	args  []interface{}
}

// As is like calling "F() AS alias"
func (f *F) As(alias string) *F {
	f.alias = alias
	return f
}

// Build satisfies Builder.
func (f *F) Build() (string, error) {
	if f.name == "" {
		return "", fmt.Errorf("Missing function name.")
	}
	args := make([]string, 0, len(f.args))
	for _, arg := range f.args {
		var s string
		switch t := arg.(type) {
		case Builder:
			var err error
			s, err = t.Build()
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

// Func creates a function.
func Func(name string, args ...interface{}) *F {
	return &F{name: name, args: args}
}

// Count represents the COUNT function.
func Count(field interface{}) *F {
	return Func("COUNT", []interface{}{&literal{field}}...)
}

// Mean represents the MEAN function.
func Mean(field interface{}) *F {
	return Func("MEAN", []interface{}{&literal{field}}...)
}

// Median represents the MEDIAN function.
func Median(field interface{}) *F {
	return Func("MEDIAN", []interface{}{&literal{field}}...)
}

// Spread represents the SPREAD function.
func Spread(field interface{}) *F {
	return Func("SPREAD", []interface{}{&literal{field}}...)
}

// Sum represents the SUM function.
func Sum(field interface{}) *F {
	return Func("SUM", []interface{}{&literal{field}}...)
}

// Bottom represents the BOTTOM function.
func Bottom(field interface{}, params ...interface{}) *F {
	return Func("BOTTOM", append([]interface{}{&literal{field}}, params...))
}

// Top represents the TOP function.
func Top(field interface{}, params ...interface{}) *F {
	return Func("TOP", append([]interface{}{&literal{field}}, params...))
}

// Derivative represents the DERIVATIVE function.
func Derivative(field interface{}, params ...interface{}) *F {
	return Func("DERIVATIVE", append([]interface{}{&literal{field}}, params...))
}

// NonNegativeDerivative represents the NON_NEGATIVE_DERIVATIVE function.
func NonNegativeDerivative(field interface{}, params ...interface{}) *F {
	return Func("NON_NEGATIVE_DERIVATIVE", append([]interface{}{&literal{field}}, params...))
}

// First represents the FIRST function.
func First(field interface{}) *F {
	return Func("FIRST", []interface{}{&literal{field}}...)
}

// StdDev represents the STDDEV function.
func StdDev(field interface{}) *F {
	return Func("STDDEV", []interface{}{&literal{field}}...)
}

// Last represents the LAST function.
func Last(field interface{}) *F {
	return Func("LAST", []interface{}{&literal{field}}...)
}

// Max represents the	MAX function.
func Max(field interface{}) *F {
	return Func("MAX", []interface{}{&literal{field}}...)
}

// Min represents the MIN function.
func Min(field interface{}) *F {
	return Func("MIN", []interface{}{&literal{field}}...)
}

// Distinct represents the DISTINCT function.
func Distinct(field interface{}) *F {
	return Func("DISTINCT", []interface{}{&literal{field}}...)
}

// Percentile represents the PERCENTILE function.
func Percentile(field interface{}, p float64) *F {
	return Func("PERCENTILE", []interface{}{&literal{field}, p})
}
