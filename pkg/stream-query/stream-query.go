package streamquery

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/spf13/pflag"

	"github.com/mcluseau/sql2sync/pkg/db"
)

type StreamQuery struct {
	// The query to synchronize
	Query string
	// The key builder to use ("json-object", "json-array", etc.)
	KeyBuilder string
	// The key columns
	KeyColumns []string
	// Group the objects by key
	GroupByKey bool
}

type KeyValue struct {
	Key, Value []byte
}

func (sq *StreamQuery) BindFlags(flags *pflag.FlagSet) {
	flags.StringVar(&sq.KeyBuilder, "key-builder", defaultKeyBuilder, "The key builder to use. Available builders: "+strings.Join(KeyBuilderNames(), ", "))
	flags.StringSliceVar(&sq.KeyColumns, "key", nil, "Add a primary key column")
	flags.BoolVar(&sq.GroupByKey, "group-by-key", false, "Group objects by key (key is not unique)")
}

func (sq *StreamQuery) StreamTo(output chan KeyValue) {
	defer close(output)

	query := sq.Query
	keyBuilder := sq.KeyBuilder
	keyColumns := sq.KeyColumns

	if sq.GroupByKey {
		query = fmt.Sprintf("select * from (%s) order by \"%s\"", query, strings.Join(sq.KeyColumns, "\", \""))
	}

	glog.Info("Running query: ", query)
	rows, err := db.DB.Query(query)
	if err != nil {
		glog.Fatal(err)
	}

	columns, err := rows.Columns()
	if err != nil {
		glog.Fatal(err)
	}

	values := make([]interface{}, len(columns))
	valueRefs := make([]interface{}, len(columns))
	for idx := range columns {
		valueRefs[idx] = &values[idx]
	}

	lastMsg := time.Now()
	scans := 0

	buildKey, ok := keyBuilders[keyBuilder]
	if !ok {
		glog.Fatal("Unknown key builder ", keyBuilder)
	}

	for rows.Next() {
		if err := rows.Scan(valueRefs...); err != nil {
			glog.Fatal(err)
		}

		scans++
		if now := time.Now(); now.Sub(lastMsg) >= 5*time.Second {
			glog.Info("Processing: scanned ", scans, " rows.")
			lastMsg = now
		}

		keyCols := make([]string, 0, len(keyColumns))
		keyVals := make([]interface{}, 0, len(keyColumns))
		value := map[string]interface{}{}

		for idx, col := range columns {
			colValue := values[idx]
			isKey := false
			for _, keyCol := range keyColumns {
				if keyCol == col {
					isKey = true
					break
				}
			}
			if isKey {
				keyCols = append(keyCols, col)
				keyVals = append(keyVals, colValue)
			}
			value[col] = colValue
		}

		output <- KeyValue{
			Key:   buildKey(keyCols, keyVals),
			Value: mustMarshal(value),
		}
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		glog.Error(reflect.TypeOf(err))
		glog.Error(string(mustMarshal(err)))
		glog.Fatal(err)
	}
	glog.Info("Scanned ", scans, " rows.")
}

func mustMarshal(v interface{}) []byte {
	ba, err := json.Marshal(v)
	if err != nil {
		glog.Fatal(err)
	}
	return ba
}
