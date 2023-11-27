package objectstore

import (
	"testing"
)

func TestZeroLengthRFC1035Label(t *testing.T) {
	res := validateStringIsRFC1035Label("")
	if res == nil {
		t.Fatalf("zero length string validated successfully")
	}
}

func TestTooLongRFC1035Label(t *testing.T) {
	res := validateStringIsRFC1035Label("toolongtoolongtoolongtoolongtoolongtoolongtoolongtoolongtoolongtoolong")
	if res == nil {
		t.Fatalf("too long string validated successfully")
	}
}

func TestBeginWithNumberRFC1035Label(t *testing.T) {
	res := validateStringIsRFC1035Label("1invalid")
	if res == nil {
		t.Fatalf("string beginning with number validates successfully")
	}
}
