package cli

import (
	"bufio"
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"math/big"
	"os"
	"path/filepath"
	"strings"

	ethkeystore "github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"

	"github.com/lightchain/worker/internal/chain"
)

// InitHandler runs the guided `init` subcommand: worker key, registration
// with stake, models, then the preflight. Every step checks what is already
// in place first, so re-running it resumes where the last run stopped.
type InitHandler struct {
	KeystorePath string
	KeystorePass string
	// Unattended answers to the key prompt: Generate creates a new key,
	// ImportKeyFile imports the hex private key held in that file.
	Generate      bool
	ImportKeyFile string

	WorkerAddr common.Address // set by EnsureKey

	// Used by Run, once the key is known.
	Chain       InitChain
	ChainID     int64 // expected chain id from CHAIN_ID
	ModelIDs    [][32]byte
	ModelNames  []string // parallel to ModelIDs
	WorkerStake *big.Int // nil = the on-chain minimum
	ECDHKeyPath string
	ECDHPass    string
	LoadECDHKey ECDHKeyLoader // creates the ECDH key when registering
	Preflight   *PreflightHandler
	Yes         bool // stake without asking
	// ReconcileSeed is handed to the registration; see Handler.ReconcileSeed.
	ReconcileSeed *ReconcileSeed

	In     *bufio.Reader // shared by every prompt so none reads ahead of another
	Out    io.Writer
	Logger *slog.Logger
}

// InitChain is what init drives: the registration writes plus the
// preflight's reads.
type InitChain interface {
	chain.RegistrationClient
	PreflightChain
}

// EnsureKey loads the worker keystore, creating it first when it does not
// exist yet. The private key is never printed.
func (h *InitHandler) EnsureKey() (*ecdsa.PrivateKey, error) {
	data, err := os.ReadFile(h.KeystorePath)
	switch {
	case err == nil:
		stored, err := ethkeystore.DecryptKey(data, h.KeystorePass)
		if err != nil {
			return nil, fmt.Errorf("cannot open the worker key at %s (%v) — WORKER_KEYSTORE_PASSWORD must be the password the key was created with", h.KeystorePath, err)
		}
		h.WorkerAddr = stored.Address
		h.say("key", "already at %s (worker %s)", h.KeystorePath, h.WorkerAddr.Hex())
		return stored.PrivateKey, nil
	case !errors.Is(err, fs.ErrNotExist):
		return nil, fmt.Errorf("read worker key: %w", err)
	}

	importFile := h.ImportKeyFile
	for !h.Generate && importFile == "" {
		answer, err := h.ask(fmt.Sprintf("No worker key at %s yet. [g]enerate a new one or [i]mport an existing private key? ", h.KeystorePath))
		if err != nil {
			return nil, fmt.Errorf("no answer to the key prompt — without a terminal, pass --generate or --import-key-file FILE")
		}
		switch strings.ToLower(answer) {
		case "g", "generate":
			h.Generate = true
		case "i", "import":
			if importFile, err = h.ask("File holding the hex private key (read, never shown): "); err != nil {
				return nil, fmt.Errorf("no key file given")
			}
		}
	}

	var key *ecdsa.PrivateKey
	verb := "created"
	if importFile != "" {
		raw, err := os.ReadFile(importFile)
		if err != nil {
			return nil, fmt.Errorf("read key file: %w", err)
		}
		// The parse error is dropped on purpose: it quotes the file's contents.
		if key, err = crypto.HexToECDSA(strings.TrimPrefix(strings.TrimSpace(string(raw)), "0x")); err != nil {
			return nil, fmt.Errorf("%s does not hold a hex private key (64 hex characters, 0x optional)", importFile)
		}
		verb = "imported"
	} else if key, err = crypto.GenerateKey(); err != nil {
		return nil, fmt.Errorf("generate worker key: %w", err)
	}
	if err := writeKeystore(h.KeystorePath, key, h.KeystorePass); err != nil {
		return nil, err
	}
	h.WorkerAddr = crypto.PubkeyToAddress(key.PublicKey)
	h.say("key", "%s %s for worker %s — back up this file and its password", verb, h.KeystorePath, h.WorkerAddr.Hex())
	return key, nil
}

// Run registers the worker if needed, adds the models it does not serve
// yet, and finishes with the preflight. Each failure is explained in terms of
// what to change; nothing is spent before the network, the models and the
// balance check out.
func (h *InitHandler) Run(ctx context.Context) error {
	if len(h.ModelIDs) == 0 {
		return errors.New("SUPPORTED_MODELS is empty — set it to the Ollama model names this worker serves, comma-separated")
	}
	id, err := h.Chain.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("cannot reach the chain RPC (%v) — check RPC_URL and this host's internet access", err)
	}
	if id.Int64() != h.ChainID {
		return fmt.Errorf("the RPC serves chain %s but CHAIN_ID is %d — RPC_URL points at another network", id, h.ChainID)
	}
	if err := h.checkModels(ctx); err != nil {
		return err
	}

	registered, err := h.Chain.IsWorkerRegistered(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read registration: %w", err)
	}
	if registered {
		h.say("register", "already registered")
	} else if err := h.register(ctx); err != nil {
		return err
	}
	if err := h.addMissingModels(ctx); err != nil {
		return err
	}

	if !h.Preflight.Run(ctx) {
		return errors.New("preflight found problems — fix each [FAIL] line above, then re-run `lightchain-worker init` (finished steps are skipped)")
	}
	h.say("init", "done — start the worker with `lightchain-worker run`")
	return nil
}

// checkModels refuses names the chain would reject, before any gas is spent.
func (h *InitHandler) checkModels(ctx context.Context) error {
	for i, id := range h.ModelIDs {
		name := h.ModelNames[i]
		whitelisted, err := h.Chain.IsModelWhitelisted(ctx, id)
		if err != nil {
			return fmt.Errorf("read model %s: %w", name, err)
		}
		if !whitelisted {
			return fmt.Errorf("model %q is not on this network's model list — SUPPORTED_MODELS names must match it exactly, tag included", name)
		}
		enabled, err := h.Chain.IsModelEnabled(ctx, id)
		if err != nil {
			return fmt.Errorf("read model %s: %w", name, err)
		}
		if !enabled {
			return fmt.Errorf("model %q is on the list but disabled — remove it from SUPPORTED_MODELS", name)
		}
	}
	return nil
}

func (h *InitHandler) register(ctx context.Context) error {
	stake := h.WorkerStake
	if stake == nil {
		var err error
		if stake, err = h.Chain.GetMinWorkerStake(ctx); err != nil {
			return fmt.Errorf("read the minimum stake: %w", err)
		}
	}
	bal, err := h.Chain.Balance(ctx, h.WorkerAddr)
	if err != nil {
		return fmt.Errorf("read balance: %w", err)
	}
	need := new(big.Int).Add(stake, lcaiToWei(gasBufferLCAI))
	if bal.Cmp(stake) <= 0 {
		return fmt.Errorf("worker %s holds %s; registering stakes %s and needs gas on top — send at least %s to it, then re-run `lightchain-worker init`",
			h.WorkerAddr.Hex(), lcai(bal), lcai(stake), lcai(new(big.Int).Sub(need, bal)))
	}
	h.say("register", "not registered yet: stake %s, balance %s", lcai(stake), lcai(bal))
	if bal.Cmp(need) < 0 {
		h.say("register", "note: that leaves under %d LCAI for gas; top up soon after", gasBufferLCAI)
	}
	if !h.Yes && !confirm(h.Out, h.In, fmt.Sprintf("Stake %s and register %s on %s? [y/N] ", lcai(stake), h.WorkerAddr.Hex(), networkName(h.ChainID))) {
		return errors.New("registration not confirmed, nothing was staked — re-run `lightchain-worker init` when ready (or pass --yes)")
	}

	reg := &Handler{
		Client:      h.Chain,
		WorkerAddr:  h.WorkerAddr,
		ModelIDs:    h.ModelIDs,
		ModelNames:  h.ModelNames,
		ECDHKeyPath: h.ECDHKeyPath,
		ECDHPass:    h.ECDHPass,
		WorkerStake: stake,
		LoadECDHKey: h.LoadECDHKey,
		Out:         h.Out,
		Logger:      h.Logger,
	}
	reg.ReconcileSeed = h.ReconcileSeed
	if err := reg.Register(ctx); err != nil {
		return fmt.Errorf("registration failed: %w — re-run `lightchain-worker init` once fixed; it reads the chain before acting", err)
	}
	return nil
}

func (h *InitHandler) addMissingModels(ctx context.Context) error {
	add := &Handler{Client: h.Chain, WorkerAddr: h.WorkerAddr, Out: h.Out, Logger: h.Logger}
	for i, id := range h.ModelIDs {
		added, err := h.Chain.WorkerSupportsModel(ctx, h.WorkerAddr, id)
		if err != nil {
			return fmt.Errorf("read the worker's models: %w", err)
		}
		if !added {
			add.ModelIDs = append(add.ModelIDs, id)
			add.ModelNames = append(add.ModelNames, h.ModelNames[i])
		}
	}
	if len(add.ModelIDs) == 0 {
		h.say("models", "all %d added on-chain", len(h.ModelIDs))
		return nil
	}
	h.say("models", "adding %s", strings.Join(add.ModelNames, ", "))
	if err := add.AddModels(ctx); err != nil {
		return fmt.Errorf("add models: %w — re-run `lightchain-worker init` once fixed", err)
	}
	return nil
}

func (h *InitHandler) say(step, format string, args ...any) {
	fmt.Fprintf(h.Out, "%-10s %s\n", step, fmt.Sprintf(format, args...))
}

// ask prints a question and returns the trimmed answer; an error means no
// answer is coming (no terminal, or end of input).
func (h *InitHandler) ask(question string) (string, error) {
	return ask(h.Out, h.In, question)
}

// confirm asks a yes-or-no question for init and top-up-stake; anything but
// y or yes, including no answer at all, is a no.
func confirm(out io.Writer, in *bufio.Reader, question string) bool {
	answer, _ := ask(out, in, question)
	a := strings.ToLower(answer)
	return a == "y" || a == "yes"
}

func ask(out io.Writer, in *bufio.Reader, question string) (string, error) {
	_, _ = fmt.Fprint(out, question)
	if in == nil {
		return "", io.EOF
	}
	line, err := in.ReadString('\n')
	if err != nil && line == "" {
		return "", err
	}
	return strings.TrimSpace(line), nil
}

// writeKeystore stores key at path in the geth keystore format (the one
// import-key writes), readable by the owner only.
func writeKeystore(path string, key *ecdsa.PrivateKey, password string) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return fmt.Errorf("create keystore directory: %w", err)
	}
	tmp, err := os.MkdirTemp(dir, ".keystore-")
	if err != nil {
		return fmt.Errorf("create keystore directory: %w", err)
	}
	defer func() { _ = os.RemoveAll(tmp) }()
	// geth writes the file 0600 under a random name; move it into place.
	acct, err := ethkeystore.NewKeyStore(tmp, ethkeystore.StandardScryptN, ethkeystore.StandardScryptP).ImportECDSA(key, password)
	if err != nil {
		return fmt.Errorf("encrypt worker key: %w", err)
	}
	if err := os.Rename(acct.URL.Path, path); err != nil {
		return fmt.Errorf("write keystore %s: %w", path, err)
	}
	return nil
}
