package hwinfo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestParseNvidiaSMI(t *testing.T) {
	out := "NVIDIA L4, 23034, 6120, 37\nNVIDIA L4, 23034, [N/A], 0\n"
	got, err := parseNvidiaSMI([]byte(out))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("want 2 devices, got %d", len(got))
	}
	want := GPUDevice{Name: "NVIDIA L4", MemoryTotalMiB: 23034, MemoryUsedMiB: 6120, UtilizationPct: 37}
	if got[0] != want {
		t.Fatalf("device 0: want %+v, got %+v", want, got[0])
	}
	if got[1].MemoryUsedMiB != 0 {
		t.Fatalf("[N/A] must parse as 0, got %d", got[1].MemoryUsedMiB)
	}
}

func TestParseNvidiaSMIRejectsBadRows(t *testing.T) {
	for _, out := range []string{"NVIDIA L4, 23034, 6120\n", "NVIDIA L4, lots, 6120, 37\n"} {
		if _, err := parseNvidiaSMI([]byte(out)); err == nil {
			t.Fatalf("want an error for %q", out)
		}
	}
}

func TestReportPrefersNvidiaSMI(t *testing.T) {
	p := NewProber("", time.Minute)
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("NVIDIA A100-SXM4-80GB, 81920, 40000, 92\n"), nil
	}
	r := p.Report(context.Background())
	if r.Source != SourceNvidiaSMI || len(r.Devices) != 1 || r.MaxUtilization() != 92 {
		t.Fatalf("unexpected report %+v", r)
	}
}

func TestReportFallsBackToOllama(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/ps" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"models":[{"name":"llama3:8b","size_vram":5000000000},{"name":"x","size_vram":1000}]}`))
	}))
	defer srv.Close()

	p := NewProber(srv.URL+"/", time.Minute)
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		return nil, errors.New("exec: \"nvidia-smi\": executable file not found in $PATH")
	}
	r := p.Report(context.Background())
	if r.Source != SourceOllama || r.VRAMInUseBytes != 5000001000 {
		t.Fatalf("unexpected report %+v", r)
	}
	if r.Devices == nil {
		t.Fatal("devices must be an empty list, not null, so the JSON shape is stable")
	}
}

func TestReportNoneWhenNothingAnswers(t *testing.T) {
	p := NewProber("http://127.0.0.1:1", time.Minute)
	p.run = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("no driver") }
	if r := p.Report(context.Background()); r.Source != SourceNone {
		t.Fatalf("want source none, got %+v", r)
	}
}

func TestReportIsCached(t *testing.T) {
	calls := 0
	p := NewProber("", time.Hour)
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		calls++
		return []byte("NVIDIA L4, 23034, 1, 1\n"), nil
	}
	p.Report(context.Background())
	p.Report(context.Background())
	if calls != 1 {
		t.Fatalf("want 1 nvidia-smi run within the ttl, got %d", calls)
	}
}

func TestWorkerVersion(t *testing.T) {
	old := Version
	defer func() { Version = old }()

	Version = "v0.0.6-agents.1"
	if got := WorkerVersion(); got != "v0.0.6-agents.1" {
		t.Fatalf("want the ldflags version, got %q", got)
	}
	Version = ""
	if got := WorkerVersion(); got != "dev" && !strings.HasPrefix(got, "dev+") {
		t.Fatalf("want dev or dev+<rev>, got %q", got)
	}
}
