package model

// Row struct is the underlying storage format.
type Row struct {
	Key    []byte `json:"key,omitempty"`
	Value  []byte `json:"value,omitempty"`
	Bucket []byte `json:"bucket,omitempty"`
}

// ContainString checks if v is in this string slice.
func ContainString(sl []string, v string) (int, bool) {
	for index, vv := range sl {
		if vv == v {
			return index, true
		}
	}
	return 0, false
}
