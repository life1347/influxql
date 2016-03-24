package influxql

import (
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var testSamples = []struct {
	b Builder
	s string
	e bool
}{
	{
		Select("foo").From("bar"),
		`SELECT "foo" FROM "bar"`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location = ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE location = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location != ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE location != 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location != 'Toronto'`),
		`SELECT "foo" FROM "bar" WHERE location != 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`location`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`time >`, time.Date(2015, 8, 18, 0, 0, 0, 0, time.UTC)),
		`SELECT "foo" FROM "bar" WHERE "time" > '2015-08-18T00:00:00Z'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`time > ?`, time.Date(2015, 8, 18, 0, 0, 0, 0, time.UTC)),
		`SELECT "foo" FROM "bar" WHERE time > '2015-08-18T00:00:00Z'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`"location" = ?`, "Toronto"),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto'`,
		false,
	},
	{
		Select("foo").From("bar").Where(`"location" = ? ?`, "Toronto"),
		``,
		true, // Unmatched ? placeholder.
	},
	{
		Select("foo").From("bar").Where(`"location" = ? AND "altitude" >= ?`, "Toronto", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' AND "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").And("altitude >=", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' AND "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").Or("altitude >=", 500),
		`SELECT "foo" FROM "bar" WHERE "location" = 'Toronto' OR "altitude" >= 500`,
		false,
	},
	{
		Select("foo").From("bar").Where("location", "Toronto").And("altitude >= x", 500),
		``,
		true, // Unsupported expression "altitude >= x"
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest"),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest'`,
		false, // Query is invalid, though.
	},
	{
		Select(Sum("value")).From("cpu").Where("region", "uswest"),
		`SELECT SUM("value") FROM "cpu" WHERE "region" = 'uswest'`,
		false, // Query is invalid, though.
	},
	{
		Select(Distinct("level description")).From("h2o_feet"),
		`SELECT DISTINCT("level description") FROM "h2o_feet"`,
		false,
	},
	{
		Select(Min("water_level"), Max("water_level")).From("h2o_feet"),
		`SELECT MIN("water_level"), MAX("water_level") FROM "h2o_feet"`,
		false,
	},
	{
		Select(Mean("water_level").As("dream_name")).From("h2o_feet"),
		`SELECT MEAN("water_level") AS "dream_name" FROM "h2o_feet"`,
		false,
	},
	{
		Select(Min("water_level").As("mwl"), Max("water_level").As("Mwl")).From("h2o_feet"),
		`SELECT MIN("water_level") AS "mwl", MAX("water_level") AS "Mwl" FROM "h2o_feet"`,
		false,
	},
	{
		Select(Count(Distinct("level description"))).From("h2o_feet"),
		`SELECT COUNT(DISTINCT("level description")) FROM "h2o_feet"`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)).Fill(0),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Minute * 10)).Fill(nil),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(null)`,
		false,
	},
	{
		Select(Mean("value")).From("cpu").Where("region", "uswest").GroupBy(Time(time.Hour * 4)).Fill("none"),
		`SELECT MEAN("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(4h) fill(none)`,
		false,
	},
}

func TestSelect(t *testing.T) {
	for _, sample := range testSamples {
		s, err := sample.b.Build()

		if sample.e {
			assert.Error(t, err)
		} else {
			assert.NoError(t, err)
		}

		assert.Equal(t, sample.s, s)
	}
}
