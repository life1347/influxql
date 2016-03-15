package influxql

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelect(t *testing.T) {
	q := Select("foo").From("bar")

	s, err := q.Build()

	assert.NoError(t, err)
	assert.Equal(t, `SELECT "foo" FROM "bar"`, s)
}
