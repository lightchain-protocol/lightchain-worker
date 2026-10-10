# Run a LightChain worker on testnet

This guide takes a bare Linux host to a testnet worker that claims and serves inference jobs. You install one binary, write one config file, run `lightchain-worker init`, and start a systemd service. Docker is optional: see [Docker Compose recipe](#docker-compose-recipe) at the end.

A worker claims jobs on-chain through sortition, runs them on your own [Ollama](https://ollama.com), streams tokens to the consumer through the public worker gateway, and settles the result on-chain. It needs no inbound ports, no VPN and no allowlist.

## What you need

- A Linux host (amd64 or arm64) with a GPU, running Ollama. macOS builds exist too, but serving wants a Linux GPU box.
- Outbound HTTPS and WSS access.
- Testnet LCAI: **5,000 LCAI** stake (the on-chain minimum, `AIConfig.getMinWorkerStake()`) plus a gas buffer of **50 LCAI**. `init` asks for at least **5,050 LCAI**; send about **5,060 LCAI** so the balance stays above the buffer after registering. Ask the LightChain team or the community channels for testnet LCAI.

## 1. Install the CLI

```bash
curl -fsSL https://github.com/lightchain-protocol/lightchain-worker/releases/latest/download/install.sh | sudo sh
```

The installer picks the build for your OS and CPU, checks its SHA-256 against the release's `checksums.txt`, and installs `/usr/local/bin/lightchain-worker`. It does nothing else: no service, no config, no keys. To pin a release or install elsewhere, pass `LIGHTCHAIN_WORKER_VERSION` or `INSTALL_DIR` after `sudo`, which drops the caller's environment: `… | sudo LIGHTCHAIN_WORKER_VERSION=v1.2.3 sh`.

To check the installer before you run it, download `install.sh` and `checksums.txt` from the same release and compare `sha256sum install.sh` with its line in `checksums.txt`.

`lightchain-worker` is the whole worker: `init` sets it up, `run` serves, and the other commands (`preflight`, `status`, `balance`, `deregister`, ...) manage it. Run `lightchain-worker help` for the list.

## 2. Pull your models into Ollama

On-chain, a model's id is `keccak256` of its name, and the worker passes that same name to Ollama. So each name you serve must be on the testnet's model list, and Ollama must have a model under exactly that name. The live list is at <https://chat-api.testnet.lightchain.ai/api/indexer/models>. It shows each model by its id, which is what `cast keccak NAME` prints; `init` checks your names against the chain before it spends anything.

```bash
ollama pull gemma4:e2b
# a list name that is not an Ollama tag needs a local copy under that name:
ollama pull llama3:8b && ollama cp llama3:8b llama3-8b
```

## 3. Write the config file

One environment file holds everything: `init` reads it, and the systemd service reads it too. The command below creates it with a random keystore password. Replace `SUPPORTED_MODELS` with the names you pulled, comma-separated.

```bash
sudo mkdir -p /etc/lightchain/worker /var/lib/lightchain-worker
sudo chmod 700 /etc/lightchain/worker
sudo tee /etc/lightchain/worker/env >/dev/null <<EOF
# chain (testnet)
CHAIN_ID=8200
RPC_URL=https://rpc.testnet.lightchain.ai
BEACON_API_URL=https://beacon.testnet.lightchain.ai
WORKER_REGISTRY_ADDRESS=0x0000000000000000000000000000000000001002
JOB_REGISTRY_ADDRESS=0x531b3A87c5D785441B9cF55b98169F20FD9056a7
SESSION_MANAGER_ADDRESS=0x86AdA80864e87dE2275200FeE905b5C32b32Bf68
AI_CONFIG_ADDRESS=0xeCF4Ca5Ba6D97ae586993e170764a1E92231b67e

# external worker profile: sortition claims, egress through the gateway
SORTITION_ENABLED=true
WORKER_GATEWAY_URL=https://worker-gateway.testnet.lightchain.ai

# identity (init creates both keys)
WORKER_KEYSTORE_PATH=/etc/lightchain/worker/keystore.json
WORKER_KEYSTORE_PASSWORD=$(openssl rand -hex 24)
ENCRYPTION_KEYSTORE_PATH=/etc/lightchain/worker/encryption.key

# models and inference
SUPPORTED_MODELS=llama3-8b
OLLAMA_URL=http://localhost:11434

# how long a stop waits for in-flight jobs (keep it under the unit's TimeoutStopSec)
SHUTDOWN_TIMEOUT=240s

# state that must survive restarts
SESSION_KEY_FILE=/var/lib/lightchain-worker/session-keys.enc
SORTITION_STATE_DIR=/var/lib/lightchain-worker/sortition-state
RELEASE_STATE_PATH=/var/lib/lightchain-worker/release_state.json

LOG_FORMAT=text
EOF
sudo chmod 600 /etc/lightchain/worker/env
```

There is no `REDIS_URL`: the external profile streams results over an authenticated WebSocket to the gateway and sends heartbeats and drain over gateway HTTPS.

The CLI reads its configuration from the environment. This shell function loads the file for each command; define it once per shell:

```bash
lcw() { sudo sh -c 'set -a; . /etc/lightchain/worker/env; exec lightchain-worker "$@"' lcw "$@"; }
```

## 4. Run `init`

```bash
lcw init
```

`init` works through four steps. Each step first checks what is already done, so you can stop at any point and run `lcw init` again.

1. **Worker key.** With no keystore at `WORKER_KEYSTORE_PATH` yet, `init` asks whether to **generate** a new key or **import** one. To import, put the hex private key in a file only root can read, answer `i`, and give the file's path; `init` reads it without printing it, and you can delete the file afterwards. The keystore is written in the standard geth format with mode 0600. `init` prints the worker address, never the key.
2. **Register with stake.** `init` checks the model names against the chain and your balance against the stake before it spends anything. If the address is not funded yet, it tells you how much to send:

   ```
   init: worker 0x7E7D…2838 holds 0 LCAI; registering stakes 5000 LCAI and needs gas on top — send at least 5050 LCAI to it, then re-run `lightchain-worker init`
   ```

   Fund the address and run `lcw init` again. Send a little more than the minimum it names, since registering spends some gas; otherwise preflight later warns that the balance is below the 50 LCAI buffer. It asks you to confirm the stake, then registers the worker. That publishes the encryption key (created now at `ENCRYPTION_KEYSTORE_PATH`), stakes the minimum, and adds your models. Set `WORKER_STAKE` (in wei) only to stake more than the minimum.
3. **Add models.** Any model in `SUPPORTED_MODELS` that the worker does not serve on-chain yet is added. To serve another model later, pull it, append it to `SUPPORTED_MODELS`, and run `lcw init` again.
4. **Preflight.** The read-only go-live check runs last: RPC and chain id, registration, suspension, stake, balance, encryption key against the on-chain key, each model's on-chain state, gateway login, Ollama tags and the beacon API. Each failing line says what to fix. A ready worker ends like this:

   ```
   [PASS] model      llama3-8b whitelisted, enabled, added
   [PASS] gateway    ok
   [PASS] ollama     1 model(s) present at http://localhost:11434
   [PASS] beacon     Prysm/v7.1.2 (linux amd64)
   preflight: 10 passed, 0 warnings, 0 failed
   init       done — start the worker with `lightchain-worker run`
   ```

**Back up `/etc/lightchain/worker/`**: the keystore, the encryption key, and the env file that holds their password. If you lose the encryption key after registering, the worker cannot serve sessions encrypted to it.

For an unattended run, `init --generate` (or `--import-key-file FILE`) answers the key question and `--yes` confirms the stake.

## 5. Run it as a service

```bash
sudo tee /etc/systemd/system/lightchain-worker.service >/dev/null <<'EOF'
[Unit]
Description=LightChain worker (testnet)
After=network-online.target ollama.service
Wants=network-online.target

[Service]
EnvironmentFile=/etc/lightchain/worker/env
ExecStart=/usr/local/bin/lightchain-worker run
Restart=always
RestartSec=5
KillSignal=SIGTERM
TimeoutStopSec=300

[Install]
WantedBy=multi-user.target
EOF
sudo systemctl daemon-reload
sudo systemctl enable --now lightchain-worker
journalctl -u lightchain-worker -f
```

These startup lines confirm the external profile is active:

```
external worker profile: sortition assignment with gateway egress (responses, heartbeat, drain via worker-gateway)
authenticated with worker-gateway
gateway stream connected
worker sidecar running (sortition mode) — waiting for shutdown signal
```

On its first start the worker does not read the chain's history: it starts its session and job cursors 2000 blocks behind the chain head (`SORTITION_SESSION_LOOKBACK_BLOCKS`), so it can claim as soon as it is eligible. A claim logs `claimed session request`. From then on the worker serves its jobs on Ollama, streams tokens through the gateway, and submits the result as an on-chain blob.

## Day to day

| Task | Command |
|---|---|
| Check everything | `lcw preflight` |
| Registration and key status | `lcw status` |
| Add a model | `ollama pull NAME`, append it to `SUPPORTED_MODELS`, `lcw init`, `sudo systemctl restart lightchain-worker` |
| Earnings | `lcw balance`; `lcw withdraw` moves them to the worker address |
| Stake under the minimum after a slash | `lcw top-up-stake AMOUNT` adds AMOUNT LCAI to the stake after asking (`lcw top-up-stake --yes AMOUNT` does not ask); `lcw preflight` shows how much is missing. Needs a release newer than v0.0.1 |
| Back from a suspension | `lcw reinstate`, once the cooldown is over and the stake is back at the minimum. Needs a release newer than v0.0.1 |
| Alerts | `lightchain-worker watch` posts to a Discord webhook when the worker needs attention, and again when it recovers. The unit and its install steps are in [`deploy/lightchain-worker-watch@.service`](../deploy/lightchain-worker-watch@.service) |
| Upgrade | re-run the installer, then `sudo systemctl restart lightchain-worker` |
| Stop | `sudo systemctl stop lightchain-worker` drains first: no new sessions, and in-flight jobs get up to `SHUTDOWN_TIMEOUT` to finish |
| Leave | stop the service, then `lcw deregister` returns the stake once no jobs are active |

Fees accrue in JobRegistry, and the worker settles them to its balance after the dispute window. A timeout or a lost dispute counts as an offense, and three offenses suspend the worker for 7 days. On testnet the slash rates are currently 0, so an offense takes no stake; on a network with non-zero rates it also takes a share of the minimum stake. Keep the host up and the models loaded, and avoid killing the service mid-job.

## Troubleshooting

Run `lcw preflight` first; each `[FAIL]` line names its fix.

| Symptom | Cause and fix |
|---|---|
| `init: … is not on this network's model list` | The name must match the testnet list exactly, tag included (`llama3-8b`, not `llama3:8b`) |
| `init: cannot open the worker key …` | `WORKER_KEYSTORE_PASSWORD` is not the password the keystore was created with |
| `init: cannot reach the chain RPC` | Check `RPC_URL` and the host's outbound access |
| `[FAIL] ollama … not pulled` | `ollama pull` the name, or `ollama cp` a pulled model to it |
| `[FAIL] gateway … 403 worker not registered` | Registration missing, or the env file points at another keystore |
| Registered but never claims | A model was not added for this worker, or the worker is suspended |

## Docker Compose recipe

The image `registry.lightchain.ai/testnet/worker:latest` carries the same CLI (`/bin/lightchain-worker`) and the worker (`/bin/worker`, its entrypoint). The env file above works unchanged except for `OLLAMA_URL=http://host.docker.internal:11434`. Mount the two directories at the same paths, owned by the image's user (uid 1000):

```yaml
# compose.yaml
services:
  worker:
    image: registry.lightchain.ai/testnet/worker:latest
    restart: unless-stopped
    env_file: /etc/lightchain/worker/env
    volumes:
      - /etc/lightchain/worker:/etc/lightchain/worker
      - /var/lib/lightchain-worker:/var/lib/lightchain-worker
    extra_hosts:
      - host.docker.internal:host-gateway
    stop_grace_period: 5m
```

```bash
sudo chown -R 1000 /etc/lightchain/worker /var/lib/lightchain-worker
sudo docker compose run --rm --entrypoint /bin/lightchain-worker worker init
sudo docker compose up -d
```

`sudo` is needed because the env file is readable by its owner only.

`init` in the container needs an image built from a worker release that has the command.

## Testnet reference

| Item | Value |
|---|---|
| Chain ID | `8200` |
| RPC | `https://rpc.testnet.lightchain.ai` |
| Beacon API | `https://beacon.testnet.lightchain.ai` |
| Worker gateway | `https://worker-gateway.testnet.lightchain.ai` |
| WorkerRegistry | `0x0000000000000000000000000000000000001002` |
| JobRegistry | `0x531b3A87c5D785441B9cF55b98169F20FD9056a7` |
| SessionManager | `0x86AdA80864e87dE2275200FeE905b5C32b32Bf68` |
| AIConfig | `0xeCF4Ca5Ba6D97ae586993e170764a1E92231b67e` |
| Model list | `https://chat-api.testnet.lightchain.ai/api/indexer/models` |
