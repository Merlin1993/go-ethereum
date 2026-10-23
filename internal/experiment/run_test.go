package experiment

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareFreshDirRequiresExplicitOverwrite(t *testing.T) {
	base := filepath.Join(t.TempDir(), "run")
	if err := os.MkdirAll(base, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(base, "state"), []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareFreshDir(base, false); err == nil {
		t.Fatal("expected non-empty directory rejection")
	}
	if _, err := PrepareFreshDir(base, true); err != nil {
		t.Fatal(err)
	}
	if entries, err := os.ReadDir(base); err != nil || len(entries) != 0 {
		t.Fatalf("directory was not reset: entries=%d err=%v", len(entries), err)
	}
}

func TestRunWritesManifestAndChecksum(t *testing.T) {
	base := t.TempDir()
	run, err := Start(base, "test", map[string]any{"seed": 7})
	if err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(base, "results", "data.csv")
	if err := os.WriteFile(output, []byte("a,b\n1,2\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := run.Finish(false, output); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"manifest.json", "checksums.sha256"} {
		if _, err := os.Stat(filepath.Join(base, "results", name)); err != nil {
			t.Fatal(err)
		}
	}
	checksums, err := os.ReadFile(filepath.Join(base, "results", "checksums.sha256"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"results/data.csv", "results/manifest.json"} {
		if !strings.Contains(string(checksums), want) {
			t.Fatalf("checksum file does not include %s:\n%s", want, checksums)
		}
	}
}
