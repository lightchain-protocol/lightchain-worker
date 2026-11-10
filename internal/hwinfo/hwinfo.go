// Package hwinfo reports the worker's build version and GPU hardware for the
// health payload, so the operator programme can verify what a worker runs on.
package hwinfo

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"os/exec"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Version is the release version, set at build time:
//
//	go build -ldflags "-X github.com/lightchain/worker/internal/hwinfo.Version=v0.0.6-agents.1"
//
// Left empty, WorkerVersion falls back to the VCS revision of the build.
var Version = ""

// WorkerVersion returns the release version, or "dev+<revision>" for an
// unversioned build, or "dev" when no build information is available.
func WorkerVersion() string {
	if Version != "" {
		return Version
	}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "dev"
	}
	var rev string
	var dirty bool
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			rev = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	if rev == "" {
		return "dev"
	}
	if len(rev) > 12 {
		rev = rev[:12]
	}
	if dirty {
		rev += "-dirty"
	}
	return "dev+" + rev
}

// GPUDevice is one GPU as nvidia-smi reports it.
type GPUDevice struct {
	Name           string `json:"name"`
	MemoryTotalMiB int    `json:"memoryTotalMiB"`
	MemoryUsedMiB  int    `json:"memoryUsedMiB"`
	UtilizationPct int    `json:"utilizationPct"`
}

// GPU report sources.
const (
	SourceNvidiaSMI = "nvidia-smi" // devices read directly from the driver
	SourceOllama    = "ollama"     // no driver access; VRAM used by loaded models only
	SourceNone      = "none"       // neither was available
)

// GPUReport is the GPU part of the health payload.
type GPUReport struct {
	Source  string      `json:"source"`
	Devices []GPUDevice `json:"devices"`
	// VRAMInUseBytes is the VRAM held by the models Ollama has loaded. It is
	// the only figure available when the worker cannot reach the driver.
	VRAMInUseBytes int64 `json:"vramInUseBytes"`
}

// MaxUtilization is the busiest device's utilisation, for the legacy
// single-number gpuUtil heartbeat field.
func (r GPUReport) MaxUtilization() int {
	m := 0
	for _, d := range r.Devices {
		if d.UtilizationPct > m {
			m = d.UtilizationPct
		}
	}
	return m
}

// commandRunner runs nvidia-smi; replaced in tests.
type commandRunner func(ctx context.Context, name string, args ...string) ([]byte, error)

func execRunner(ctx context.Context, name string, args ...string) ([]byte, error) {
	return exec.CommandContext(ctx, name, args...).Output()
}

// Prober reads the GPU report, caching it so a short heartbeat interval does
// not run nvidia-smi on every tick.
type Prober struct {
	ollamaURL string
	client    *http.Client
	run       commandRunner
	ttl       time.Duration

	mu      sync.Mutex
	cached  GPUReport
	fetched time.Time
}

// NewProber returns a Prober that falls back to ollamaURL when nvidia-smi is
// not available. A zero ttl defaults to one minute.
func NewProber(ollamaURL string, ttl time.Duration) *Prober {
	if ttl <= 0 {
		ttl = time.Minute
	}
	return &Prober{
		ollamaURL: strings.TrimRight(ollamaURL, "/"),
		client:    &http.Client{Timeout: 3 * time.Second},
		run:       execRunner,
		ttl:       ttl,
	}
}

// Report returns the cached GPU report, refreshing it once the ttl has passed.
// It never fails: an unreadable GPU is reported as source "none".
func (p *Prober) Report(ctx context.Context) GPUReport {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.fetched.IsZero() && time.Since(p.fetched) < p.ttl {
		return p.cached
	}
	p.cached = p.probe(ctx)
	p.fetched = time.Now()
	return p.cached
}

func (p *Prober) probe(ctx context.Context) GPUReport {
	cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := p.run(cctx, "nvidia-smi",
		"--query-gpu=name,memory.total,memory.used,utilization.gpu",
		"--format=csv,noheader,nounits")
	if err == nil {
		if devices, perr := parseNvidiaSMI(out); perr == nil && len(devices) > 0 {
			return GPUReport{Source: SourceNvidiaSMI, Devices: devices}
		}
	}
	if p.ollamaURL != "" {
		if vram, oerr := p.ollamaVRAM(ctx); oerr == nil {
			return GPUReport{Source: SourceOllama, Devices: []GPUDevice{}, VRAMInUseBytes: vram}
		}
	}
	return GPUReport{Source: SourceNone, Devices: []GPUDevice{}}
}

// parseNvidiaSMI parses `--format=csv,noheader,nounits` output for the
// name, memory.total, memory.used and utilization.gpu columns.
func parseNvidiaSMI(out []byte) ([]GPUDevice, error) {
	r := csv.NewReader(strings.NewReader(string(out)))
	r.TrimLeadingSpace = true
	rows, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse nvidia-smi: %w", err)
	}
	devices := make([]GPUDevice, 0, len(rows))
	for _, row := range rows {
		if len(row) != 4 {
			return nil, fmt.Errorf("parse nvidia-smi: want 4 columns, got %d", len(row))
		}
		total, err1 := atoiField(row[1])
		used, err2 := atoiField(row[2])
		util, err3 := atoiField(row[3])
		if err1 != nil || err2 != nil || err3 != nil {
			return nil, fmt.Errorf("parse nvidia-smi: non-numeric field in %q", strings.Join(row, ", "))
		}
		devices = append(devices, GPUDevice{
			Name:           strings.TrimSpace(row[0]),
			MemoryTotalMiB: total,
			MemoryUsedMiB:  used,
			UtilizationPct: util,
		})
	}
	return devices, nil
}

// atoiField parses a numeric column; nvidia-smi prints "[N/A]" for values a
// device does not report, which count as 0.
func atoiField(s string) (int, error) {
	s = strings.TrimSpace(s)
	if s == "[N/A]" || s == "N/A" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

// ollamaVRAM sums size_vram over the models Ollama currently has loaded.
func (p *Prober) ollamaVRAM(ctx context.Context) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.ollamaURL+"/api/ps", http.NoBody)
	if err != nil {
		return 0, err
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("ollama /api/ps: status %d", resp.StatusCode)
	}
	var body struct {
		Models []struct {
			SizeVRAM int64 `json:"size_vram"`
		} `json:"models"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, fmt.Errorf("ollama /api/ps: %w", err)
	}
	var total int64
	for _, m := range body.Models {
		total += m.SizeVRAM
	}
	return total, nil
}
