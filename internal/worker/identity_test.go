package worker

import (
	"strings"
	"testing"
)

func TestNewIDIsUnique(t *testing.T) {
	first, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	second, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	if first == second || !strings.Contains(first, "-") {
		t.Errorf("NewID() = %q and %q, want distinct structured IDs", first, second)
	}
}
