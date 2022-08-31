package bq

import (
	"fmt"
	"reflect"

	"cloud.google.com/go/bigquery"
)

type BigQueryNamedTracker struct {
	Values []bigquery.QueryParameter
}

func NewBigQueryNamedTracker() *BigQueryNamedTracker {
	return &BigQueryNamedTracker{}
}

func (t *BigQueryNamedTracker) AddValue(val interface{}) string {
	for _, v := range t.Values {
		if reflect.DeepEqual(v.Value, val) {
			return "@" + v.Name
		}
	}
	name := fmt.Sprintf("v%dt", len(t.Values))
	t.Values = append(t.Values, bigquery.QueryParameter{Name: name, Value: val})
	return "@" + name
}
