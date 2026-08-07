package tree

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"testing"
)

func TestRewriteLastMetricsRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "metrics.csv")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	writer := csv.NewWriter(file)
	writer.WriteAll([][]string{
		{"Block", "Storage_Breakdown_Valid", "Reachable_Logical_Bytes"},
		{"100000", "false", "0"},
		{"200000", "false", "0"},
	})
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	if err := rewriteLastMetricsRecord(path, map[string]string{
		"Storage_Breakdown_Valid": "true",
		"Reachable_Logical_Bytes": "1234",
	}); err != nil {
		t.Fatal(err)
	}
	file, err = os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	records, err := csv.NewReader(file).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if got := records[1]; got[1] != "false" || got[2] != "0" {
		t.Fatalf("earlier row changed: %v", got)
	}
	if got := records[2]; got[1] != "true" || got[2] != "1234" {
		t.Fatalf("last row not backfilled: %v", got)
	}
}
