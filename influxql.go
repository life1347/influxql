package influxql

import (
	"bytes"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"
)

const placeholder = "?"

type nullValue struct{}

var (
	reWhiteChars        = regexp.MustCompile(`[\s\t\r\n]+`)
	reSpacesBetweenTags = regexp.MustCompile(`}}[\s\t\r\n]+{{`)
)

func cleanTemplate(s string) string {
	s = reWhiteChars.ReplaceAllString(s, " ")
	s = reSpacesBetweenTags.ReplaceAllString(s, "}}{{")
	return strings.TrimSpace(s)
}

type quoted string

const selectTemplateText = `
	SELECT
		{{.Fields | joinWithCommas}}
	FROM
		{{.Measurement}}
	{{if .Where}}
		WHERE
		 {{.Where | joinWithSpace }}
	{{end}}
	{{if .GroupBy}}
		GROUP BY
		 {{.GroupBy | joinWithCommas }}
	{{end}}
	{{if .Fill}} fill({{.Fill}}){{end}}
`

type selectTemplateValues struct {
	Measurement string
	Fields      []string
	Where       []string
	GroupBy     []string
	Fill        string
}

func joinWithCommas(in []string) string {
	return strings.Join(in, ", ")
}

func joinWithSpace(in []string) string {
	return strings.Join(in, " ")
}

var selectTemplate = template.Must(
	template.New("select").Funcs(
		map[string]interface{}{
			"joinWithCommas": joinWithCommas,
			"joinWithSpace":  joinWithSpace,
		},
	).Parse(cleanTemplate(selectTemplateText)),
)

type timeGroup struct {
	d time.Duration
}

func (t *timeGroup) Compile() (string, error) {
	ns := t.d.Nanoseconds()

	switch int64(0) {
	case ns % int64(time.Hour):
		return fmt.Sprintf("time(%dh)", ns/int64(time.Hour)), nil
	case ns % int64(time.Minute):
		return fmt.Sprintf("time(%dm)", ns/int64(time.Minute)), nil
	case ns % int64(time.Second):
		return fmt.Sprintf("time(%ds)", ns/int64(time.Second)), nil
	}

	return fmt.Sprintf("time(%dns)", ns), nil
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

func Sum(field string) *customFunc {
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

type Builder interface {
	Build() (string, error)
}

type keyword struct {
	v string
}

func (k *keyword) Compile() (string, error) {
	return k.v, nil
}

var (
	orKeyword  = &keyword{"OR"}
	andKeyword = &keyword{"AND"}
)

type compilable interface {
	Compile() (string, error)
}

type Expr struct {
	expr   string
	values []interface{}
}

func (e *Expr) Compile() (string, error) {
	placeholders := strings.Count(e.expr, placeholder)

	if placeholders > 0 {
		// Where("foo = ?", "bar")
		if placeholders != len(e.values) {
			return "", fmt.Errorf("Mismatched number of placeholders (%d) and values (%d)", strings.Count(e.expr, placeholder), len(e.values))
		}
	} else {
		if len(e.values) > 0 {
			parts := strings.Split(strings.TrimSpace(reWhiteChars.ReplaceAllString(e.expr, " ")), " ")
			lparts := len(parts)

			if lparts < 1 {
				return "", fmt.Errorf("Expecting statement.")
			} else if lparts < 2 {
				// Where("foo", "bar")
				if len(e.values) != 1 {
					return "", fmt.Errorf("Expecting exactly one value.")
				}
				e.expr = fmt.Sprintf("%q = ?", parts[0])
			} else if lparts < 3 {
				// Where("foo =", "bar")
				if len(e.values) != 1 {
					return "", fmt.Errorf("Expecting exactly one value.")
				}
				e.expr = fmt.Sprintf("%q %s ?", parts[0], parts[1])
			} else {
				return "", fmt.Errorf("Unsupported expression %q", e.expr)
			}
		}
	}

	compiled := make([]interface{}, 0, len(e.values))
	for i := range e.values {
		lit := &value{e.values[i]}
		c, err := lit.Compile()
		if err != nil {
			return "", err
		}
		compiled = append(compiled, c)
	}

	s := strings.Replace(e.expr, "?", "%s", -1)
	return fmt.Sprintf(s, compiled...), nil
}

type value struct {
	v interface{}
}

func (v *value) Compile() (string, error) {
	switch t := v.v.(type) {
	case string:
		return fmt.Sprintf(`'%s'`, t), nil
	case int:
		return fmt.Sprintf("%d", t), nil
	case uint:
		return fmt.Sprintf("%d", t), nil
	case int64:
		return fmt.Sprintf("%d", t), nil
	case uint64:
		return fmt.Sprintf("%d", t), nil
	case int32:
		return fmt.Sprintf("%d", t), nil
	case uint32:
		return fmt.Sprintf("%d", t), nil
	case int8:
		return fmt.Sprintf("%d", t), nil
	case uint8:
		return fmt.Sprintf("%d", t), nil
	case time.Duration:
		return fmt.Sprintf("%dns", t.Nanoseconds()), nil
	default:
		return fmt.Sprintf(`'%v'`, t), nil
	}
	panic("reached")
}

type literal struct {
	v interface{}
}

func (l *literal) Compile() (string, error) {
	switch v := l.v.(type) {
	case compilable:
		return v.Compile()
	case string:
		return fmt.Sprintf(`"%s"`, v), nil
	default:
		return fmt.Sprintf(`"%v"`, v), nil
	}
	panic("reached")
}

type selectBuilder struct {
	measurement compilable
	fields      []compilable
	where       []compilable
	groupBy     []compilable
	orderBy     []compilable
	limit       int
	offset      int
	slimit      int
	soffset     int
	fill        interface{}
}

func Select(fields ...interface{}) *selectBuilder {
	s := &selectBuilder{}
	for i := range fields {
		s.fields = append(s.fields, &literal{fields[i]})
	}
	return s
}

func (s *selectBuilder) Fill(v interface{}) *selectBuilder {
	if v == nil {
		s.fill = nullValue{}
		return s
	}
	s.fill = v
	return s
}

func (s *selectBuilder) From(measurement string) *selectBuilder {
	s.measurement = &literal{measurement}
	return s
}

func (s *selectBuilder) GroupBy(fields ...interface{}) *selectBuilder {
	for i := range fields {
		s.groupBy = append(s.groupBy, &literal{fields[i]})
	}
	return s
}

func (s *selectBuilder) OrderBy(fields ...interface{}) *selectBuilder {
	for i := range fields {
		s.orderBy = append(s.orderBy, &literal{fields[i]})
	}
	return s
}

func (s *selectBuilder) Where(expr string, values ...interface{}) *selectBuilder {
	s.where = make([]compilable, 0, 1)
	s.where = append(s.where, &Expr{expr: expr, values: values})
	return s
}

func (s *selectBuilder) And(expr string, values ...interface{}) *selectBuilder {
	s.where = append(s.where, andKeyword, &Expr{expr: expr, values: values})
	return s
}

func (s *selectBuilder) Or(expr string, values ...interface{}) *selectBuilder {
	s.where = append(s.where, orKeyword, &Expr{expr: expr, values: values})
	return s
}

func (s *selectBuilder) Offset(offset int) *selectBuilder {
	s.offset = offset
	return s
}

func (s *selectBuilder) Limit(limit int) *selectBuilder {
	s.limit = limit
	return s
}

func (s *selectBuilder) SOffset(soffset int) *selectBuilder {
	s.soffset = soffset
	return s
}

func (s *selectBuilder) SLimit(slimit int) *selectBuilder {
	s.slimit = slimit
	return s
}

func (s *selectBuilder) Build() (string, error) {
	data := selectTemplateValues{}

	if err := compileInto(s.measurement, &data.Measurement); err != nil {
		return "", err
	}

	if err := compileArrayInto(s.fields, &data.Fields); err != nil {
		return "", err
	}

	if err := compileArrayInto(s.where, &data.Where); err != nil {
		return "", err
	}

	if err := compileArrayInto(s.groupBy, &data.GroupBy); err != nil {
		return "", err
	}

	if s.fill != nil {
		switch v := s.fill.(type) {
		case nullValue:
			data.Fill = "null"
		case string:
			data.Fill = fmt.Sprintf("%v", v)
		default:
			data.Fill = fmt.Sprintf("%v", v)
		}
	}

	buf := bytes.NewBuffer(nil)
	err := selectTemplate.Execute(buf, data)
	if err != nil {
		return "", err
	}
	return buf.String(), nil
}

func compileInto(src compilable, dst *string) (err error) {
	*dst, err = src.Compile()
	return
}

func compileArrayInto(src []compilable, dst *[]string) error {
	v := make([]string, 0, len(src))
	for i := range src {
		s, err := src[i].Compile()
		if err != nil {
			return err
		}
		v = append(v, s)
	}
	*dst = v
	return nil
}
