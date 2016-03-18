package influxql

import (
	"bytes"
	"fmt"
)

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
