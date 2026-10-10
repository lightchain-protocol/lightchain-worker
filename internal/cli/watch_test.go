package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethereum/go-ethereum/common"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/lightchain/worker/internal/chain"
)

// fakeWatchChain is a WatchChain with a healthy worker by default.
type fakeWatchChain struct {
	now        *time.Time
	block      uint64
	registered bool
	suspended  bool
	until      int64 // suspendedUntil, unix seconds
	hang       bool  // Head blocks until its context ends
	stake      *big.Int
	minStake   *big.Int
	claimLogs  []SessionClaim  // returned by the next SessionClaims scan, then cleared
	eligible   map[uint64]bool // reqID -> the worker was eligible at the asked block
	reqCaps    map[uint64]int64
	asked      []uint64 // blocks EligibleAt was asked about
	scans      [][2]uint64
	err        error
}

func healthyWatchChain(now *time.Time) *fakeWatchChain {
	return &fakeWatchChain{now: now, block: 100, registered: true, stake: lcaiWei(5000), minStake: lcaiWei(5000)}
}

func (f *fakeWatchChain) Head(ctx context.Context) (chain.HeadInfo, error) {
	if f.hang {
		<-ctx.Done()
		return chain.HeadInfo{}, ctx.Err()
	}
	f.block++
	return chain.HeadInfo{Number: f.block, Timestamp: f.now.Unix()}, f.err
}

func (f *fakeWatchChain) IsWorkerRegistered(context.Context, common.Address) (bool, error) {
	return f.registered, f.err
}

func (f *fakeWatchChain) IsWorkerSuspended(context.Context, common.Address) (bool, error) {
	return f.suspended, f.err
}

func (f *fakeWatchChain) GetSuspendedUntil(context.Context, common.Address) (*big.Int, error) {
	return big.NewInt(f.until), f.err
}

func (f *fakeWatchChain) GetWorkerStake(context.Context, common.Address) (*big.Int, error) {
	return f.stake, f.err
}

func (f *fakeWatchChain) GetMinWorkerStake(context.Context) (*big.Int, error) {
	return f.minStake, f.err
}

func (f *fakeWatchChain) SessionClaims(_ context.Context, from, to uint64) ([]SessionClaim, error) {
	f.scans = append(f.scans, [2]uint64{from, to})
	logs := f.claimLogs
	f.claimLogs = nil
	return logs, f.err
}

func (f *fakeWatchChain) EligibleAt(_ context.Context, reqID uint64, _ common.Address, block uint64) (bool, error) {
	f.asked = append(f.asked, block)
	return f.eligible[reqID], f.err
}

func (f *fakeWatchChain) RequiredCapabilities(_ context.Context, reqID uint64) (*big.Int, error) {
	return big.NewInt(f.reqCaps[reqID]), f.err
}

// discordMessage is the subset of a Discord webhook body the tests check.
type discordMessage struct {
	Content string `json:"content"`
	Embeds  []struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Color       int    `json:"color"`
		Timestamp   string `json:"timestamp"`
	} `json:"embeds"`
}

// webhookSink is an in-process webhook receiver.
type webhookSink struct {
	mu     sync.Mutex
	got    []discordMessage
	status int // response status; 0 = 204 like Discord
	srv    *httptest.Server
}

func newWebhookSink(t *testing.T) *webhookSink {
	t.Helper()
	s := &webhookSink{}
	s.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		var m discordMessage
		assert.NoError(t, json.NewDecoder(r.Body).Decode(&m))
		s.mu.Lock()
		defer s.mu.Unlock()
		if s.status != 0 {
			w.WriteHeader(s.status)
			return
		}
		s.got = append(s.got, m)
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(s.srv.Close)
	return s
}

// take returns the messages received since the last call.
func (s *webhookSink) take() []discordMessage {
	s.mu.Lock()
	defer s.mu.Unlock()
	got := s.got
	s.got = nil
	return got
}

func (s *webhookSink) setStatus(code int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = code
}

// fakeOllama serves /api/tags on a fixed address and can be stopped and
// restarted there, so a stop looks like the real thing (connection refused).
type fakeOllama struct {
	t    *testing.T
	addr string
	srv  *httptest.Server
}

func newFakeOllama(t *testing.T) *fakeOllama {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	o := &fakeOllama{t: t, addr: l.Addr().String()}
	o.serve(l)
	t.Cleanup(func() { o.srv.Close() })
	return o
}

func (o *fakeOllama) serve(l net.Listener) {
	o.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"models":[]}`))
	}))
	_ = o.srv.Listener.Close()
	o.srv.Listener = l
	o.srv.Start()
}

func (o *fakeOllama) URL() string { return "http://" + o.addr }
func (o *fakeOllama) stop()       { o.srv.Close() }

func (o *fakeOllama) start() {
	l, err := net.Listen("tcp", o.addr)
	require.NoError(o.t, err)
	o.serve(l)
}

func newWatch(t *testing.T, fc *fakeWatchChain, ollamaURL string, sink *webhookSink) *WatchHandler {
	t.Helper()
	return &WatchHandler{
		Chain:      fc,
		WorkerAddr: testAddr,
		ChainID:    8200,
		OllamaURL:  ollamaURL,
		WebhookURL: sink.srv.URL,
		Interval:   30 * time.Second,
		Cooldown:   time.Hour,
		Now:        func() time.Time { return *fc.now },
		Logger:     slog.New(slog.DiscardHandler),
	}
}

func TestWatch_OllamaStopAlertsOnceAndRecoversOnce(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	ollama := newFakeOllama(t)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, ollama.URL(), sink)
	ctx := context.Background()

	h.Tick(ctx)
	require.Empty(t, sink.take(), "healthy worker must not alert")

	ollama.stop()
	h.Tick(ctx)
	got := sink.take()
	require.Len(t, got, 1, "one alert on the first check after Ollama stops")
	alert := got[0]
	assert.Contains(t, alert.Content, "ollama")
	assert.Contains(t, alert.Content, testAddr.Hex())
	require.Len(t, alert.Embeds, 1)
	assert.Contains(t, alert.Embeds[0].Title, "ollama")
	assert.Contains(t, alert.Embeds[0].Description, ollama.URL())
	assert.Equal(t, 0xE74C3C, alert.Embeds[0].Color)
	ts, err := time.Parse(time.RFC3339, alert.Embeds[0].Timestamp)
	require.NoError(t, err)
	assert.True(t, ts.Equal(now))

	for range 5 {
		now = now.Add(h.Interval)
		h.Tick(ctx)
	}
	require.Empty(t, sink.take(), "no repeat while still down inside the cooldown")

	ollama.start()
	now = now.Add(h.Interval)
	h.Tick(ctx)
	got = sink.take()
	require.Len(t, got, 1, "one recovery message when Ollama returns")
	assert.Contains(t, got[0].Content, "ollama")
	require.Len(t, got[0].Embeds, 1)
	assert.Contains(t, got[0].Embeds[0].Title, "recovered")
	assert.Equal(t, 0x2ECC71, got[0].Embeds[0].Color)

	h.Tick(ctx)
	require.Empty(t, sink.take(), "nothing more once healthy")
}

func TestWatch_CooldownLimitsRepeatsPerCheck(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	ollama := newFakeOllama(t)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, ollama.URL(), sink)
	ctx := context.Background()

	ollama.stop()
	h.Tick(ctx)
	require.Len(t, sink.take(), 1)

	now = now.Add(h.Cooldown - time.Second)
	h.Tick(ctx)
	require.Empty(t, sink.take(), "no repeat inside the cooldown")

	now = now.Add(time.Second)
	h.Tick(ctx)
	got := sink.take()
	require.Len(t, got, 1, "one reminder once the cooldown has passed")
	assert.Contains(t, got[0].Embeds[0].Title, "still failing")
	assert.Contains(t, got[0].Embeds[0].Title, "1h0m0s")

	// A new failure after a recovery is a new transition: it alerts even
	// inside the cooldown of the previous alert.
	ollama.start()
	h.Tick(ctx)
	require.Equal(t, []string{"ollama recovered"}, titles(sink.take()))
	ollama.stop()
	now = now.Add(time.Minute)
	h.Tick(ctx)
	require.Equal(t, []string{"ollama failing"}, titles(sink.take()))
}

func TestWatch_RejectedWebhookIsRetriedNextTick(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	ollama := newFakeOllama(t)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, ollama.URL(), sink)
	ctx := context.Background()

	ollama.stop()
	sink.setStatus(http.StatusTooManyRequests)
	h.Tick(ctx)
	require.Empty(t, sink.take())

	sink.setStatus(0)
	now = now.Add(h.Interval)
	h.Tick(ctx)
	require.Len(t, sink.take(), 1, "the alert the webhook rejected goes out on the next tick")
}

// titles returns the embed titles of msgs.
func titles(msgs []discordMessage) []string {
	var out []string
	for _, m := range msgs {
		for _, e := range m.Embeds {
			out = append(out, e.Title)
		}
	}
	return out
}

func TestWatch_SuspensionAlerts(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	ctx := context.Background()

	h.Tick(ctx)
	require.Empty(t, sink.take())

	fc.suspended = true
	fc.until = now.Add(2 * time.Hour).Unix()
	now = now.Add(h.Interval)
	h.Tick(ctx)
	got := sink.take()
	require.Equal(t, []string{"suspended failing"}, titles(got))
	desc := got[0].Embeds[0].Description
	assert.Contains(t, desc, time.Unix(fc.until, 0).UTC().Format(time.RFC3339))
	assert.Contains(t, desc, "`lightchain-worker reinstate`", "suspension only lifts when the worker reinstates")

	fc.suspended = false
	now = now.Add(h.Interval)
	h.Tick(ctx)
	assert.Equal(t, []string{"suspended recovered"}, titles(sink.take()))
}

func TestWatch_SuspensionPastCooldownSaysReinstate(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.suspended = true
	fc.until = now.Add(-time.Hour).Unix()
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)

	h.Tick(context.Background())

	got := sink.take()
	require.Equal(t, []string{"suspended failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "cooldown ended")
	assert.Contains(t, got[0].Embeds[0].Description, "`lightchain-worker reinstate`")
}

func TestWatch_HungRPCStillAlertsWithinTheInterval(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.hang = true
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	h.Interval = 100 * time.Millisecond

	start := time.Now()
	h.Tick(context.Background())

	assert.Less(t, time.Since(start), time.Second, "a hung RPC must not stall the tick")
	got := sink.take()
	require.Equal(t, []string{"rpc failing"}, titles(got), "the alert goes out on the tick's own context")
	assert.Contains(t, got[0].Embeds[0].Description, "deadline exceeded")
}

func TestWatch_StakeBelowMinimumAlerts(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.stake = lcaiWei(4925)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)

	h.Tick(context.Background())

	got := sink.take()
	require.Equal(t, []string{"stake failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "4925 LCAI below the on-chain minimum 5000 LCAI")
	assert.Contains(t, got[0].Embeds[0].Description, "`lightchain-worker top-up-stake", "the alert names its fix")
}

func TestWatch_NotRegisteredAlertsOnceNotPerDerivedCheck(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.registered = false
	fc.stake = big.NewInt(0)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)

	h.Tick(context.Background())

	assert.Equal(t, []string{"registered failing"}, titles(sink.take()))
}

func TestWatch_RPCDownAlertsRPCOnlyAndKeepsChainCheckState(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.suspended = true
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	ctx := context.Background()

	h.Tick(ctx)
	require.Equal(t, []string{"suspended failing"}, titles(sink.take()))

	fc.err = errors.New("connection refused")
	now = now.Add(h.Interval)
	h.Tick(ctx)
	got := sink.take()
	require.Equal(t, []string{"rpc failing"}, titles(got), "an unreadable chain is not a recovery")
	assert.Contains(t, got[0].Embeds[0].Description, "connection refused")

	fc.err = nil
	now = now.Add(h.Interval)
	h.Tick(ctx)
	assert.Equal(t, []string{"rpc recovered"}, titles(sink.take()), "suspension still failing, inside its cooldown")
}

func TestWatch_StalledChainAlertsRPC(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	stalled := now
	fc.now = &stalled
	now = now.Add(5 * time.Minute)
	h.Now = func() time.Time { return now }

	h.Tick(context.Background())

	got := sink.take()
	require.Equal(t, []string{"rpc failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "5m0s old")
}

func TestWatch_LivenessEndpointDown(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	healthz := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/healthz", r.URL.Path)
		_, _ = w.Write([]byte("ok\n"))
	}))
	h.LivenessURL = healthz.URL + "/healthz"
	ctx := context.Background()

	h.Tick(ctx)
	require.Empty(t, sink.take())

	healthz.Close()
	h.Tick(ctx)
	got := sink.take()
	require.Equal(t, []string{"liveness failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "/healthz")
}

func TestWatch_HeartbeatAge(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	t.Cleanup(func() { _ = rdb.Close() })
	h.Heartbeat = rdb
	h.HeartbeatMaxAge = 30 * time.Second
	key := "worker:" + strings.ToLower(testAddr.Hex()) + ":health"
	beat := func(at time.Time) { mr.HSet(key, "lastHeartbeat", strconv.FormatInt(at.Unix(), 10)) }
	ctx := context.Background()

	beat(now.Add(-10 * time.Second))
	h.Tick(ctx)
	require.Empty(t, sink.take())

	now = now.Add(time.Minute)
	h.Tick(ctx)
	got := sink.take()
	require.Equal(t, []string{"heartbeat failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "1m10s ago")

	beat(now)
	h.Tick(ctx)
	require.Equal(t, []string{"heartbeat recovered"}, titles(sink.take()))

	// The worker's heartbeat key expires after three intervals, so a worker
	// that stopped long ago has no key at all.
	mr.Del(key)
	now = now.Add(h.Cooldown)
	h.Tick(ctx)
	got = sink.take()
	require.Equal(t, []string{"heartbeat failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "no heartbeat")
}

var otherWorker = common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

func TestWatch_MissedClaimsAlertAndOwnClaimRecovers(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	fc.eligible = map[uint64]bool{1: true, 2: true, 3: true}
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	h.MissedClaims = 3
	ctx := context.Background()
	tick := func(logs ...SessionClaim) {
		fc.claimLogs = logs
		now = now.Add(h.Interval)
		h.Tick(ctx)
	}

	h.Tick(ctx) // starts at head: no back-scan
	tick(SessionClaim{ReqID: 1, Worker: otherWorker, Block: 120})
	tick(SessionClaim{ReqID: 2, Worker: otherWorker, Block: 130})
	require.Empty(t, sink.take(), "two misses are below the threshold")
	assert.Equal(t, []uint64{120 - claimGraceBlocks, 130 - claimGraceBlocks}, fc.asked,
		"eligibility is judged claimGraceBlocks before the other worker's claim")

	tick(SessionClaim{ReqID: 3, Worker: otherWorker, Block: 140})
	got := sink.take()
	require.Equal(t, []string{"claims failing"}, titles(got))
	assert.Contains(t, got[0].Embeds[0].Description, "3 session(s) in a row")

	tick(SessionClaim{ReqID: 4, Worker: testAddr, Block: 150})
	assert.Equal(t, []string{"claims recovered"}, titles(sink.take()))

	// Scans are contiguous and never re-read a block.
	for i := 1; i < len(fc.scans); i++ {
		assert.Equal(t, fc.scans[i-1][1]+1, fc.scans[i][0], "scan %d", i)
	}
}

func TestWatch_LosingSortitionFairlyIsNotAMiss(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	// Request 9 went to another worker before this one was eligible; request 7
	// needed a capability, which watch does not judge.
	fc.eligible = map[uint64]bool{7: true}
	fc.reqCaps = map[uint64]int64{7: 1}
	sink := newWebhookSink(t)
	h := newWatch(t, fc, newFakeOllama(t).URL(), sink)
	h.MissedClaims = 1
	ctx := context.Background()

	h.Tick(ctx)
	for i := range 10 {
		fc.claimLogs = []SessionClaim{
			{ReqID: 9, Worker: otherWorker, Block: uint64(200 + i)},
			{ReqID: 7, Worker: otherWorker, Block: uint64(200 + i)},
			{ReqID: 5, Worker: otherWorker, Block: claimGraceBlocks}, // too early to judge
		}
		now = now.Add(time.Hour)
		h.Tick(ctx)
	}
	assert.Empty(t, sink.take())
}

func TestWatch_RunAlertsWithinOneIntervalAndStopsOnCancel(t *testing.T) {
	t.Parallel()
	now := time.Now()
	fc := healthyWatchChain(&now)
	ollama := newFakeOllama(t)
	ollama.stop()
	sink := newWebhookSink(t)
	h := newWatch(t, fc, ollama.URL(), sink)
	h.Now = nil
	h.Interval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		h.Run(ctx)
	}()

	var got []discordMessage
	require.Eventually(t, func() bool {
		got = append(got, sink.take()...)
		return len(got) > 0
	}, time.Second, 5*time.Millisecond)
	time.Sleep(5 * h.Interval)
	got = append(got, sink.take()...)
	assert.Equal(t, []string{"ollama failing"}, titles(got))

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestKeystoreAddress_ReadsWithoutPassword(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	good := filepath.Join(dir, "ks.json")
	require.NoError(t, os.WriteFile(good, []byte(`{"address":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","crypto":{}}`), 0o600))
	bad := filepath.Join(dir, "bad.json")
	require.NoError(t, os.WriteFile(bad, []byte(`{"crypto":{}}`), 0o600))

	addr, err := KeystoreAddress(good)
	require.NoError(t, err)
	assert.Equal(t, testAddr, addr)

	_, err = KeystoreAddress(bad)
	assert.ErrorContains(t, err, "no address")
	_, err = KeystoreAddress(filepath.Join(dir, "missing.json"))
	assert.Error(t, err)
}

func TestWatchWorkerAddress_FlagThenEnvThenKeystore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	ks := filepath.Join(dir, "ks.json")
	require.NoError(t, os.WriteFile(ks, []byte(`{"address":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","crypto":{}}`), 0o600))
	bare := filepath.Join(dir, "bare.json")
	require.NoError(t, os.WriteFile(bare, []byte(`{"crypto":{}}`), 0o600))
	flagAddr := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	envAddr := common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")

	cases := []struct {
		name     string
		flag     string
		env      common.Address
		keystore string
		want     common.Address
		wantErr  string
	}{
		{name: "flag first", flag: flagAddr.Hex(), env: envAddr, keystore: ks, want: flagAddr},
		{name: "env before the keystore", env: envAddr, keystore: ks, want: envAddr},
		{name: "env, keystore without an address field", env: envAddr, keystore: bare, want: envAddr},
		{name: "env, no keystore", env: envAddr, want: envAddr},
		{name: "keystore last", keystore: ks, want: testAddr},
		{name: "a bad flag is an error, not a fallback", flag: "nope", env: envAddr, keystore: ks, wantErr: `--worker must be a hex address, got "nope"`},
		{name: "keystore without an address field", keystore: bare, wantErr: "has no address field — set WATCH_WORKER_ADDRESS or pass --worker"},
		{name: "nothing set", wantErr: "watch needs --worker, WATCH_WORKER_ADDRESS or WORKER_KEYSTORE_PATH"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := WatchWorkerAddress(tc.flag, tc.env, tc.keystore)
			if tc.wantErr != "" {
				require.ErrorContains(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestWatch_SourceHasNoWritePath pins "watch never remediates" at the source:
// nothing on the watch code path may load a signing key, build a transaction,
// or write Redis / drain state.
func TestWatch_SourceHasNoWritePath(t *testing.T) {
	t.Parallel()
	forbidden := []string{
		"TransactOpts", "Transactor", "SendTransaction", "DecryptKey", "loadSigningKey", "keystore.Load",
		"HSet", "Set(ctx", "Del(", "Expire", "SetDraining", "Undrain", "SendDrain",
	}
	for _, f := range []string{"watch.go", "../config/watch.go", "../../cmd/cli/watch.go"} {
		src, err := os.ReadFile(f)
		require.NoError(t, err)
		for _, tok := range forbidden {
			assert.False(t, strings.Contains(string(src), tok), "%s must stay read-only but contains %q", f, tok)
		}
	}
}

func TestWatch_WebhookFailureLogNeverShowsTheWebhookToken(t *testing.T) {
	t.Parallel()
	now := time.Unix(1_800_000_000, 0)
	fc := healthyWatchChain(&now)
	ollama := newFakeOllama(t)
	ollama.stop()
	h := newWatch(t, fc, ollama.URL(), newWebhookSink(t))
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()
	h.WebhookURL = dead.URL + "/api/webhooks/1/secret-token"
	var logs bytes.Buffer
	h.Logger = slog.New(slog.NewTextHandler(&logs, nil))

	h.Tick(context.Background())

	assert.Contains(t, logs.String(), "webhook post failed")
	assert.NotContains(t, logs.String(), "secret-token")
}
