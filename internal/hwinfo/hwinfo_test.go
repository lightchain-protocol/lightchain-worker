package hwinfo

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseNvidiaSMI(t *testing.T) {
	out := "NVIDIA L4, 23034, 6120, 37\nNVIDIA L4, 23034, [N/A], 0\n"
	got, err := parseNvidiaSMI([]byte(out))
	require.NoError(t, err)
	require.Len(t, got, 2)
	assert.Equal(t, GPUDevice{Name: "NVIDIA L4", MemoryTotalMiB: 23034, MemoryUsedMiB: 6120, UtilizationPct: 37}, got[0])
	assert.Zero(t, got[1].MemoryUsedMiB, "[N/A] must parse as 0")
}

func TestParseNvidiaSMIRejectsBadRows(t *testing.T) {
	for _, out := range []string{"NVIDIA L4, 23034, 6120\n", "NVIDIA L4, lots, 6120, 37\n"} {
		_, err := parseNvidiaSMI([]byte(out))
		assert.Error(t, err, "want an error for %q", out)
	}
}

func TestReportPrefersNvidiaSMI(t *testing.T) {
	p := NewProber("", time.Minute)
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		return []byte("NVIDIA A100-SXM4-80GB, 81920, 40000, 92\nNVIDIA A100-SXM4-80GB, 81920, 1000, 10\n"), nil
	}
	r := p.Report(context.Background())
	assert.Equal(t, SourceNvidiaSMI, r.Source)
	assert.Len(t, r.Devices, 2)
	// gpuUtil is a fraction: worker selection clamps it to 1.
	assert.InDelta(t, 0.92, r.Utilization(), 1e-9)
	total, used := r.MemoryMiB()
	assert.Equal(t, int64(163840), total)
	assert.Equal(t, int64(41000), used)
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
	assert.Equal(t, SourceOllama, r.Source)
	assert.Equal(t, int64(5000001000), r.VRAMInUseBytes)
	assert.NotNil(t, r.Devices, "devices must be an empty list, not null, so the JSON shape is stable")
	total, used := r.MemoryMiB()
	assert.Zero(t, total)
	assert.Equal(t, int64(5000001000>>20), used)
}

func TestReportNoneWhenNothingAnswers(t *testing.T) {
	p := NewProber("http://127.0.0.1:1", time.Minute)
	p.run = func(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("no driver") }
	assert.Equal(t, SourceNone, p.Report(context.Background()).Source)
}

func TestReportIsCached(t *testing.T) {
	var calls atomic.Int32
	p := NewProber("", time.Hour)
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		calls.Add(1)
		return []byte("NVIDIA L4, 23034, 1, 1\n"), nil
	}
	p.Report(context.Background())
	p.Report(context.Background())
	assert.Equal(t, int32(1), calls.Load(), "want 1 nvidia-smi run within the ttl")
}

// A wedged GPU driver can leave nvidia-smi running forever. The heartbeat
// must still go out: the first call waits only firstWait, later calls not at
// all, and no second nvidia-smi starts behind the stuck one.
func TestReportDoesNotBlockOnAHungProbe(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var calls atomic.Int32
	p := NewProber("", time.Nanosecond)
	p.firstWait = 50 * time.Millisecond
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		calls.Add(1)
		<-release
		return nil, errors.New("killed")
	}

	start := time.Now()
	r := p.Report(context.Background())
	assert.Less(t, time.Since(start), time.Second)
	assert.Equal(t, NoGPU(), r)

	start = time.Now()
	p.Report(context.Background())
	assert.Less(t, time.Since(start), 20*time.Millisecond, "a later call must not wait for the stuck probe")
	assert.Equal(t, int32(1), calls.Load())
}

// The first heartbeat runs under the startup deadline, so the first probe
// gives up when the caller's context ends.
func TestReportHonoursContextOnFirstCall(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	p := NewProber("", time.Minute)
	p.firstWait = time.Hour
	p.run = func(context.Context, string, ...string) ([]byte, error) {
		<-release
		return nil, errors.New("killed")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	start := time.Now()
	assert.Equal(t, SourceNone, p.Report(ctx).Source)
	assert.Less(t, time.Since(start), time.Second)
}

func TestWorkerVersion(t *testing.T) {
	old := Version
	defer func() { Version = old }()

	Version = "v0.0.6-agents.1"
	assert.Equal(t, "v0.0.6-agents.1", WorkerVersion())
	Version = ""
	got := WorkerVersion()
	assert.True(t, got == "dev" || strings.HasPrefix(got, "dev+"), "want dev or dev+<rev>, got %q", got)
}
