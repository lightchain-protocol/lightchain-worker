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

// NoGPU is the report for a worker that reads neither the driver nor Ollama.
func NoGPU() GPUReport { return GPUReport{Source: SourceNone, Devices: []GPUDevice{}} }

// Utilization is the busiest device's utilisation as a fraction (0 to 1), the
// unit of the single-number gpuUtil heartbeat field that worker selection reads.
func (r GPUReport) Utilization() float64 {
	m := 0
	for _, d := range r.Devices {
		if d.UtilizationPct > m {
			m = d.UtilizationPct
		}
	}
	return float64(m) / 100
}

// MemoryMiB sums total and used memory over all devices. Without driver
// access it reports the VRAM Ollama's loaded models hold as used, total 0.
func (r GPUReport) MemoryMiB() (total, used int64) {
	if len(r.Devices) == 0 {
		return 0, r.VRAMInUseBytes >> 20
	}
	for _, d := range r.Devices {
		total += int64(d.MemoryTotalMiB)
		used += int64(d.MemoryUsedMiB)
	}
	return total, used
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
	// firstWait bounds how long Report waits for the very first probe.
	firstWait time.Duration

	mu       sync.Mutex
	cached   GPUReport
	fetched  time.Time
	inflight chan struct{} // closed when the running probe ends; nil when none runs
	called   bool          // Report has run before, so it no longer waits
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
		firstWait: time.Second,
		cached:    NoGPU(),
	}
}

// Report returns the latest GPU report and starts a refresh once the ttl has
// passed. The probe runs in its own goroutine, so a hung nvidia-smi (a wedged
// driver can keep it alive past any kill) never blocks a heartbeat. Only the
// first call waits for a probe, at most firstWait or until ctx ends; later
// calls return the previous report while a refresh runs. It never fails: an
// unreadable GPU is reported as source "none".
func (p *Prober) Report(ctx context.Context) GPUReport {
	p.mu.Lock()
	if p.inflight == nil && time.Since(p.fetched) >= p.ttl {
		p.inflight = make(chan struct{})
		go p.refresh(p.inflight)
	}
	done, first := p.inflight, !p.called
	p.called = true
	p.mu.Unlock()

	if done != nil && first {
		t := time.NewTimer(p.firstWait)
		defer t.Stop()
		select {
		case <-done:
		case <-t.C:
		case <-ctx.Done():
		}
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.cached
}

// refresh probes and stores the result. A probe that never returns leaves
// inflight set: the report keeps its last value and no further nvidia-smi
// processes pile up behind the stuck one. A worker restart clears it.
func (p *Prober) refresh(done chan struct{}) {
	r := p.probe(context.Background())
	p.mu.Lock()
	p.cached, p.fetched, p.inflight = r, time.Now(), nil
	p.mu.Unlock()
	close(done)
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
	return NoGPU()
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
