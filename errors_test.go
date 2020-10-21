package redis

import "testing"

func TestSetNilFunc(t *testing.T) {
	fTrue := func(_ error) bool { return true }
	fFalse := func(_ error) bool { return false }

	SetNilFunc(fTrue)
	if !IsNil(nil) {
		t.Fatal()
	}

	SetNilFunc(fFalse)
	if IsNil(nil) {
		t.Fatal()
	}
}
