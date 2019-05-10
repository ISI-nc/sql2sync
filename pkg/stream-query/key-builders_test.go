package streamquery

import (
	"testing"
)

func TestJsonObjectKeyBuilder(t *testing.T) {
	if `{"a":"a"}` != string(jsonObjectKey([]string{"a"}, []interface{}{"a"})) {
		t.Fail()
	}
	if `{"a":"a","b":1,"c":1.2}` != string(jsonObjectKey([]string{"a", "b", "c"}, []interface{}{"a", 1, 1.2})) {
		t.Fail()
	}
}

func TestJsonArrayKeyBuilder(t *testing.T) {
	if `["a"]` != string(jsonArrayKey(nil, []interface{}{"a"})) {
		t.Fail()
	}
	if `["a",1,1.2]` != string(jsonArrayKey(nil, []interface{}{"a", 1, 1.2})) {
		t.Fail()
	}
}

func TestValueKeyBuilder(t *testing.T) {
	if `a` != string(value(nil, []interface{}{"a"})) {
		t.Fail()
	}
	if `1` != string(value(nil, []interface{}{1})) {
		t.Fail()
	}
	if `123.563` != string(value(nil, []interface{}{123.563})) {
		t.Fail()
	}
}
