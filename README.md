# LightChain worker

The worker serves AI inference on LightChain. It claims sessions through on-chain sortition, runs each job on a local Ollama, streams the tokens to the consumer, and settles the result on-chain as an EIP-4844 blob.

## Run a worker

Follow the install guide: **[docs/install.md](docs/install.md)**. It goes from a bare host to a serving testnet worker. A copy for the docs site (<https://docs.lightchain.ai>, under worker-and-model / run-a-worker) is pending; until it is published, `docs/install.md` is the reference.

In short:

```bash
curl -fsSL https://github.com/lightchain-protocol/lightchain-worker/releases/latest/download/install.sh | sudo sh
lightchain-worker init     # key, register with stake, models, preflight (env from the guide)
lightchain-worker run      # serve; the guide wraps it in a systemd unit
```

## Binaries

| Path | Binary | Role |
|---|---|---|
| `cmd/cli` | `lightchain-worker` | Operator CLI: `init`, `run`, `preflight`, `register`, `add-models`, `status`, `drain`, `balance`, `withdraw`, `top-up-stake`, `reinstate`, `deregister`, ... (`lightchain-worker help`) |
| `cmd/sidecar` | `worker` | The worker service alone. It is the Docker image's entrypoint and the same code as `lightchain-worker run` |

Both read their configuration from environment variables.

## Build and test

The worker imports `github.com/lightchain/pkg` through a Go workspace, so build inside the orchestrator checkout (its `go.work` joins `worker/` and `pkg/`), or write a `go.work` the way the `Dockerfile` does.

```bash
go test ./...
make test-install   # installer against a file:// release: good checksum installs, tampered one does not
```

## Releases

`make release` writes `dist/`: one CLI binary per platform (`lightchain-worker-{linux,darwin}-{amd64,arm64}`), `install.sh`, and a `checksums.txt` (SHA-256) covering them all. Attach every file in `dist/` to a GitHub release. The installer downloads from `…/releases/latest/download/` (or `…/releases/download/<tag>/` with `LIGHTCHAIN_WORKER_VERSION`), so a release must carry all of them.
