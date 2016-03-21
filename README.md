# InfluxQL

The `influxql` package is a **query** builder for InfluxQL, the SQL-like query
language InfluxDB uses. The purpose of this package is to offer a better tool
to build InfluxQL when writing complex queries. Sometimes string concatenation
is just not the best approach.

This package is still a draft, see some examples below:

```
q = influxql.Select(inql.Distinct("foo")).From("bar")
s, err = q.Build() // SELECT DISTINCT(foo) FROM bar

q = influxql.Select("foo").From("bar").Where("location", "Toronto").And("time >=", dateA).And("time <=", dateB).GroupBy(time.Minute*30).Fill("none")
q.Build() // SELECT foo FROM bar WHERE location = ? AND time >= ? AND time <= ?

q = influxql.Select("foo").From("bar").Where("location = ? AND time >= ? AND time <= ?", "Toronto", dateA, dateB).GroupBy(time.Minute*30).Fill(0)
q.Build() // SELECT foo FROM bar WHERE location = ? AND time >= ? AND time <= ?

q := influxql.Select(inql.Distinct("level description")).From("h2o_feet").GroupBy("location")
q.Build() // SELECT DISTINCT("level description") FROM h2o_feet GROUP BY location
```
