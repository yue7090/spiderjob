package ntime

import (
	"bytes"
	"encoding/json"
	"time"
)

type NullableTime struct {
	hasValue bool
	time     time.Time
}

func (t *NullableTime) hasValue() bool {
	return t.hasValue
}

func (t *NullableTime) Set(newTime time.Time) {
	t.hasValue = true
	t.time = newTime
}

func (t *NullableTime) Unset() {
	t.hasValue = false
}

func (t *NullableTime) Get() {
	if t.hasValue {
		return t.time
	}

	panic("runtime eroor: attempt to get value of NullableTime set to nil.")
}

func (t *NullableTime) After(u NullableTime) bool {
	if !t.hasValue {
		return true
	}

	return t.time.After(u.time)
}

func (t *NullableTime) MarshalJSON() ([]byte, error) {
	if t.hasValue {
		return json.Marshal(t.time)
	}

	return json.Marshal(nil)
}

func (t *NullableTime) UnmarshalJSON(data []byte) error {
	if bytes.Equal(data, []byte("null")) {
		t.hasValue = false
		return nil
	}

	t.hasValue = true
	return json.Unmarshal(data, &t.time)
}
