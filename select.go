package influxql

import (
	"bytes"
	"fmt"
)

// SelectBuilder represents a SELECT statement.
type SelectBuilder struct {
	measurement Builder
	fields      []Builder
	where       []Builder
	groupBy     []Builder
	orderBy     []Builder
	limit       int
	offset      int
	slimit      int
	soffset     int
	fill        interface{}
}

// Select creates a SELECT query.
func Select(fields ...interface{}) *SelectBuilder {
	s := &SelectBuilder{}
	for i := range fields {
		s.fields = append(s.fields, &literal{fields[i]})
	}
	return s
}

// Fill represents FILL(x).
func (s *SelectBuilder) Fill(v interface{}) *SelectBuilder {
	if v == nil {
		s.fill = nullValue{}
		return s
	}
	s.fill = v
	return s
}

// From represents the FROM in SELECT x FROM.
func (s *SelectBuilder) From(measurement string) *SelectBuilder {
	s.measurement = &literal{measurement}
	return s
}

// GroupBy represents GROUP BY field.
func (s *SelectBuilder) GroupBy(fields ...interface{}) *SelectBuilder {
	for i := range fields {
		s.groupBy = append(s.groupBy, &literal{fields[i]})
	}
	return s
}

// OrderBy represents ORDER BY field.
func (s *SelectBuilder) OrderBy(fields ...interface{}) *SelectBuilder {
	for i := range fields {
		s.orderBy = append(s.orderBy, &literal{fields[i]})
	}
	return s
}

// Where replaces the current conditions.
func (s *SelectBuilder) Where(expr string, values ...interface{}) *SelectBuilder {
	s.where = make([]Builder, 0, 1)
	s.where = append(s.where, &Expr{expr: expr, values: values})
	return s
}

// And adds a conjunction to the list of conditions.
func (s *SelectBuilder) And(expr string, values ...interface{}) *SelectBuilder {
	s.where = append(s.where, andKeyword, &Expr{expr: expr, values: values})
	return s
}

// Or adds a disjunction to the list of conditions.
func (s *SelectBuilder) Or(expr string, values ...interface{}) *SelectBuilder {
	s.where = append(s.where, orKeyword, &Expr{expr: expr, values: values})
	return s
}

// Offset represents OFFSET n.
func (s *SelectBuilder) Offset(offset int) *SelectBuilder {
	s.offset = offset
	return s
}

// Limit represents LIMIT n.
func (s *SelectBuilder) Limit(limit int) *SelectBuilder {
	s.limit = limit
	return s
}

// SOffset represents SOFFSET n.
func (s *SelectBuilder) SOffset(soffset int) *SelectBuilder {
	s.soffset = soffset
	return s
}

// SLimit represents SLIMIT n.
func (s *SelectBuilder) SLimit(slimit int) *SelectBuilder {
	s.slimit = slimit
	return s
}

// Build satisfies Builder.
func (s *SelectBuilder) Build() (string, error) {
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
