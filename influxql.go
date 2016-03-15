package influxql

import (
	"bytes"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"text/template"
	"time"
)

var reWhiteChars = regexp.MustCompile(`[\s\t\r\n]+`)

func cleanTemplate(s string) string {
	return strings.TrimSpace(reWhiteChars.ReplaceAllString(s, " "))
}

const selectTemplateText = `
	SELECT
		{{.Fields | joinWithCommas}}
	FROM
		{{.Measurement}}
`

type selectTemplateValues struct {
	Measurement string
	Fields      []string
}

func joinWithCommas(in []string) string {
	return strings.Join(in, ", ")
}

var selectTemplate = template.Must(
	template.New("select").Funcs(
		map[string]interface{}{
			"joinWithCommas": joinWithCommas,
		},
	).Parse(cleanTemplate(selectTemplateText)),
)

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
	if strings.Count("?", e.expr) != len(e.values) {
		return "", errors.New("Mismatched number of placeholders and values")
	}

	compiled := make([]interface{}, 0, len(e.values))
	for i := range e.values {
		lit := &literal{e.values[i]}
		c, err := lit.Compile()
		if err != nil {
			return "", err
		}
		compiled = append(compiled, c)
	}

	s := strings.Replace(e.expr, "?", "%s", -1)
	return fmt.Sprintf(s, compiled...), nil
}

type literal struct {
	v interface{}
}

func (l *literal) Compile() (string, error) {
	switch v := l.v.(type) {
	case string:
		return fmt.Sprintf("%q", v), nil
	case int:
		return fmt.Sprintf("%d", v), nil
	case uint:
		return fmt.Sprintf("%d", v), nil
	case int64:
		return fmt.Sprintf("%d", v), nil
	case uint64:
		return fmt.Sprintf("%d", v), nil
	case int32:
		return fmt.Sprintf("%d", v), nil
	case uint32:
		return fmt.Sprintf("%d", v), nil
	case int8:
		return fmt.Sprintf("%d", v), nil
	case uint8:
		return fmt.Sprintf("%d", v), nil
	case time.Duration:
		return fmt.Sprintf("%dns", v.Nanoseconds()), nil
	case compilable:
		return v.Compile()
	default:
		return fmt.Sprintf("%v", v), nil
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
}

func Select(fields ...interface{}) *selectBuilder {
	s := &selectBuilder{}
	for i := range fields {
		s.fields = append(s.fields, &literal{fields[i]})
	}
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
