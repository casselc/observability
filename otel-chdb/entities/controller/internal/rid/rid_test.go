package rid

import "testing"

// The vector is ClickHouse 26.10's evaluation of sql/resources.sql's
// expression over the same map (empty values dropped).
func TestIDMatchesClickHouse(t *testing.T) {
	got := ID(map[string]string{"k8s.pod.name": "a-1", "k8s.namespace.name": "ns", "empty": ""})
	if want := uint64(18114781823887046220); got != want {
		t.Fatalf("ID = %d, want %d", got, want)
	}
}
