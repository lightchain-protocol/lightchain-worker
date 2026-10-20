# LightChain worker

The worker serves AI inference on LightChain. It claims sessions through on-chain sortition, runs each job on a local Ollama, streams the tokens to the consumer, and settles the result on-chain as an EIP-4844 blob.

## Run a worker

Follow the install guide on the docs site: **[Run a worker on testnet with the CLI](https://docs.lightchain.ai/docs/worker-and-model/run-a-worker/testnet-cli)**. It goes from a bare host to a serving testnet worker. The same guide is in this repository as [docs/install.md](docs/install.md). For alerts when the worker needs attention, see [Worker alerts](https://docs.lightchain.ai/docs/worker-and-model/worker-alerts).

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

Every pull request runs these in CI (`.github/workflows/ci.yml`), along with golangci-lint on the lines it changes, a build of the Docker image and the release build for every platform.

## Releases

A release is a version tag on a commit of `testnet`:

```bash
git tag v1.2.3 <commit> && git push origin v1.2.3
```

CI runs the checks on the tag, publishes the image as `ghcr.io/lightchain-protocol/worker:v1.2.3`, and creates the GitHub release with everything `make release` writes to `dist/`: one CLI binary per platform (`lightchain-worker-{linux,darwin}-{amd64,arm64}`), `install.sh`, and a `checksums.txt` (SHA-256) covering them all. The release notes are generated from the merged pull requests and can be edited afterwards.

Release binaries and the image carry the tag: `lightchain-worker version` prints it, with the commit when the build recorded one, and the worker logs it on start. A plain `go build` reports `dev`.

The installer downloads from `…/releases/latest/download/` (or `…/releases/download/<tag>/` with `LIGHTCHAIN_WORKER_VERSION`), so a release must carry all of those files. A tag with a hyphen (`v1.2.3-rc.1`) becomes a pre-release: it can be installed by naming it, and `latest` stays where it was.
