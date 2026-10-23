package experiment

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/mem"
)

type Manifest struct {
	Experiment  string         `json:"experiment"`
	Status      string         `json:"status"`
	StartedAt   time.Time      `json:"started_at"`
	FinishedAt  *time.Time     `json:"finished_at,omitempty"`
	Command     []string       `json:"command"`
	GoVersion   string         `json:"go_version"`
	GOOS        string         `json:"goos"`
	GOARCH      string         `json:"goarch"`
	NumCPU      int            `json:"num_cpu"`
	CPUModel    string         `json:"cpu_model,omitempty"`
	MemoryBytes uint64         `json:"memory_bytes,omitempty"`
	VCSRevision string         `json:"vcs_revision,omitempty"`
	VCSModified bool           `json:"vcs_modified,omitempty"`
	Config      map[string]any `json:"config"`
	Outputs     []string       `json:"outputs,omitempty"`
}

type Run struct {
	baseDir  string
	manifest Manifest
}

// PrepareFreshDir prevents accidental reuse of experiment state. Existing
// non-empty directories require the caller's explicit allowOverwrite flag.
func PrepareFreshDir(path string, allowOverwrite bool) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("experiment directory is empty")
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	abs = filepath.Clean(abs)
	volumeRoot := filepath.Clean(filepath.VolumeName(abs) + string(os.PathSeparator))
	cwd, _ := os.Getwd()
	home, _ := os.UserHomeDir()
	for _, forbidden := range []string{volumeRoot, filepath.Clean(cwd), filepath.Clean(home)} {
		if forbidden != "." && strings.EqualFold(abs, forbidden) {
			return "", fmt.Errorf("refusing unsafe experiment directory %q", abs)
		}
	}
	entries, readErr := os.ReadDir(abs)
	switch {
	case readErr == nil && len(entries) > 0 && !allowOverwrite:
		return "", fmt.Errorf("experiment directory %q is not empty; choose a fresh path or pass the explicit overwrite flag", abs)
	case readErr == nil && len(entries) > 0:
		if err := os.RemoveAll(abs); err != nil {
			return "", err
		}
	case readErr != nil && !os.IsNotExist(readErr):
		return "", readErr
	}
	if err := os.MkdirAll(abs, 0o755); err != nil {
		return "", err
	}
	return abs, nil
}

func Start(baseDir, name string, config map[string]any) (*Run, error) {
	manifest := Manifest{
		Experiment: name,
		Status:     "running",
		StartedAt:  time.Now().UTC(),
		Command:    append([]string(nil), os.Args...),
		GoVersion:  runtime.Version(),
		GOOS:       runtime.GOOS,
		GOARCH:     runtime.GOARCH,
		NumCPU:     runtime.NumCPU(),
		Config:     config,
	}
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, setting := range info.Settings {
			switch setting.Key {
			case "vcs.revision":
				manifest.VCSRevision = setting.Value
			case "vcs.modified":
				manifest.VCSModified = setting.Value == "true"
			}
		}
	}
	if manifest.VCSRevision == "" {
		manifest.VCSRevision, manifest.VCSModified = gitRevision()
	}
	if info, err := cpu.Info(); err == nil && len(info) > 0 {
		manifest.CPUModel = strings.TrimSpace(info[0].ModelName)
	}
	if info, err := mem.VirtualMemory(); err == nil && info != nil {
		manifest.MemoryBytes = info.Total
	}
	r := &Run{baseDir: baseDir, manifest: manifest}
	if err := r.writeManifest(); err != nil {
		return nil, err
	}
	return r, nil
}

func gitRevision() (string, bool) {
	cwd, err := os.Getwd()
	if err != nil {
		return "", false
	}
	revisionCmd := exec.Command("git", "rev-parse", "HEAD")
	revisionCmd.Dir = cwd
	revision, err := revisionCmd.Output()
	if err != nil {
		return "", false
	}
	statusCmd := exec.Command("git", "status", "--porcelain", "--untracked-files=no")
	statusCmd.Dir = cwd
	status, statusErr := statusCmd.Output()
	return strings.TrimSpace(string(revision)), statusErr == nil && strings.TrimSpace(string(status)) != ""
}

func (r *Run) Finish(failed bool, outputs ...string) error {
	now := time.Now().UTC()
	r.manifest.FinishedAt = &now
	if failed {
		r.manifest.Status = "failed"
	} else {
		r.manifest.Status = "completed"
	}
	r.manifest.Outputs = relativeExistingFiles(r.baseDir, outputs)
	if err := r.writeManifest(); err != nil {
		return err
	}
	manifestPath := filepath.Join(r.baseDir, "results", "manifest.json")
	checksumInputs := append(append([]string(nil), outputs...), manifestPath)
	return r.writeChecksums(checksumInputs)
}

func (r *Run) writeManifest() error {
	resultsDir := filepath.Join(r.baseDir, "results")
	if err := os.MkdirAll(resultsDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(r.manifest, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	return os.WriteFile(filepath.Join(resultsDir, "manifest.json"), data, 0o644)
}

func (r *Run) writeChecksums(outputs []string) error {
	type checksum struct{ path, sum string }
	var sums []checksum
	for _, path := range outputs {
		if path == "" {
			continue
		}
		f, err := os.Open(path)
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return err
		}
		h := sha256.New()
		_, copyErr := io.Copy(h, f)
		closeErr := f.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		rel, err := filepath.Rel(r.baseDir, path)
		if err != nil {
			rel = path
		}
		sums = append(sums, checksum{filepath.ToSlash(rel), hex.EncodeToString(h.Sum(nil))})
	}
	sort.Slice(sums, func(i, j int) bool { return sums[i].path < sums[j].path })
	var b strings.Builder
	for _, item := range sums {
		fmt.Fprintf(&b, "%s  %s\n", item.sum, item.path)
	}
	return os.WriteFile(filepath.Join(r.baseDir, "results", "checksums.sha256"), []byte(b.String()), 0o644)
}

func relativeExistingFiles(baseDir string, paths []string) []string {
	var out []string
	for _, path := range paths {
		if path == "" {
			continue
		}
		if _, err := os.Stat(path); err != nil {
			continue
		}
		rel, err := filepath.Rel(baseDir, path)
		if err != nil {
			rel = path
		}
		out = append(out, filepath.ToSlash(rel))
	}
	return out
}
