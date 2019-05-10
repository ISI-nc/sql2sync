package streamquery

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"

	"github.com/golang/glog"
)

// Available key builders.
// Parameters:
// 1. columns []string, the list of columns;
// 2. values []interface{}, the list of values (value for columns[i] is values[i]).
var keyBuilders = map[string]KeyBuilder{
	"json-object": jsonObjectKey,
	"json-array":  jsonArrayKey,
	"value":       value,
}

type KeyBuilder func([]string, []interface{}) []byte

const defaultKeyBuilder = "json-object"

func KeyBuilderNames() []string {
	names := make([]string, 0, len(keyBuilders))
	for key := range keyBuilders {
		names = append(names, key)
	}
	sort.Strings(names)
	return names
}

func jsonObjectKey(columns []string, values []interface{}) []byte {
	kv := map[string]interface{}{}
	for idx, col := range columns {
		kv[col] = values[idx]
	}
	ba, err := json.Marshal(kv)
	if err != nil {
		glog.Fatal(err)
	}
	return ba
}

func jsonArrayKey(columns []string, values []interface{}) []byte {
	ba, err := json.Marshal(values)
	if err != nil {
		glog.Fatal(err)
	}
	return ba
}

func value(columns []string, values []interface{}) []byte {
	if len(columns) > 1 {
		glog.Fatal("Key-Builders value can only be used with 1 value")
	}

	switch v := values[0].(type) {
	case string:
		return []byte(v)
	case int:
		return []byte(strconv.Itoa(v))
	case int64:
		b := make([]byte, 8)
		binary.LittleEndian.PutUint64(b, uint64(v))
		return b
	case float64:
		return []byte(strconv.FormatFloat(v, 'f', -1, 64))
	default:
		fmt.Printf("The key is: %+v\n", reflect.TypeOf(v))
		glog.Fatal("The key is neither a string/int/float")
	}

	// should never happen
	return nil
}
