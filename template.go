package influxql

import (
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

var (
	orKeyword  = &keyword{"OR"}
	andKeyword = &keyword{"AND"}
)

func cleanTemplate(s string) string {
	s = reWhiteChars.ReplaceAllString(s, " ")
	s = reSpacesBetweenTags.ReplaceAllString(s, "}}{{")
	return strings.TrimSpace(s)
}

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

type keyword struct {
	v string
}

func (k *keyword) Compile() (string, error) {
	return k.v, nil
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
