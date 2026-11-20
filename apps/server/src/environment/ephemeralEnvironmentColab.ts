import type {
  EphemeralEnvironmentColabHardware,
  EphemeralEnvironmentProvisionerColab,
} from "@t3tools/contracts";

const hardwareFlags = {
  "cpu-high-mem": "--high-mem",
  cpu: "",
  "gpu-t4": "--gpu T4 --high-mem",
  "gpu-l4": "--gpu L4",
  "gpu-g4": "--gpu G4 --high-mem",
  "gpu-a100": "--gpu A100 --high-mem",
  "gpu-h100": "--gpu H100 --high-mem",
  "tpu-v5e1": "--tpu v5e1",
  "tpu-v6e1": "--tpu v6e1",
} satisfies Record<EphemeralEnvironmentColabHardware, string>;

const commonScript = `set -euo pipefail
umask 077
export PATH="$HOME/.local/bin:/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:\${PATH:-}"

STATE_DIR="$HOME/.t3/ephemeral-colab"

session_for_lease() {
  local safe_id
  safe_id="$(printf '%s' "$1" | tr -c 'A-Za-z0-9-' '-' | cut -c1-24)"
  printf 't3-%s' "$safe_id"
}

stop_session() {
  local handle="$1"
  local pid_file="$STATE_DIR/\${handle}.pid"
  local pid command_line sessions
  if [[ -f "$pid_file" ]]; then
    pid="$(cat "$pid_file" 2>/dev/null || true)"
    if [[ "$pid" =~ ^[1-9][0-9]*$ ]]; then
      # The PID can have been reused since the tunnel exited.
      command_line="$(ps -ww -p "$pid" -o command= 2>/dev/null || true)"
      if [[ "$command_line" == ssh\\ -N\\ *"HostKeyAlias=$handle "* ]]; then
        kill "$pid" 2>/dev/null || true
      fi
    fi
  fi
  rm -f "$pid_file" "$STATE_DIR/\${handle}.port" "$STATE_DIR/\${handle}.tunnel.log" || return 1

  if ! "$COLAB_BIN" stop -s "$handle" </dev/null >&2; then
    sessions="$("$COLAB_BIN" sessions </dev/null)" || return 1
    if grep -Fq "[$handle] " <<< "$sessions"; then
      echo "[t3-colab] Failed to stop still-active session '$handle'." >&2
      return 1
    fi
  fi
  rm -f "$STATE_DIR/\${handle}.requested"
}`;

const provisionScript = `SSH_KEY="$HOME/.ssh/t3_colab_ed25519"
if ! COLAB_BIN="$(command -v colab)"; then
  echo "[t3-colab] Install the Colab CLI on the T3 Code host." >&2
  exit 1
fi
SESSION="$(session_for_lease "\${T3_EPHEMERAL_LEASE_ID:?Missing lease ID}")"
REMOTE_PORT=3773
TUNNEL_PID=""

mkdir -p "$STATE_DIR" "$HOME/.ssh"
chmod 700 "$STATE_DIR"

if [[ ! -f "$SSH_KEY" ]]; then
  # Publish a complete key even when two leases start at the same time.
  KEY_DIR="$(mktemp -d "$HOME/.ssh/.t3-colab-key.XXXXXXXX")"
  trap 'rm -rf "$KEY_DIR"' EXIT
  ssh-keygen -q -t ed25519 -N "" -C "t3-ephemeral-colab" -f "$KEY_DIR/key" </dev/null >&2
  ln "$KEY_DIR/key" "$SSH_KEY" 2>/dev/null || [[ -f "$SSH_KEY" ]]
  rm -rf "$KEY_DIR"
fi

cleanup_on_error() {
  local rc=$?
  if [[ "$rc" -ne 0 ]]; then
    if [[ -n "$TUNNEL_PID" ]]; then
      kill "$TUNNEL_PID" 2>/dev/null || true
    fi
    stop_session "$SESSION" || true
  fi
}
trap cleanup_on_error EXIT
trap 'exit 143' TERM
trap 'exit 130' INT

echo "[t3-colab] Provisioning Colab session '$SESSION' ($*)..." >&2
touch "$STATE_DIR/\${SESSION}.requested"
"$COLAB_BIN" new -s "$SESSION" "$@" </dev/null >&2

echo "[t3-colab] Bootstrapping T3 Code and providers on '$SESSION'..." >&2
"$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -s >&2 <<'REMOTE_BOOTSTRAP'
set -euo pipefail
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
mkdir -p "$HOME/.local/bin" "$HOME/.t3" "$HOME/.codex" "$HOME/.claude" /content/work
chmod 700 "$HOME/.t3" "$HOME/.codex" "$HOME/.claude"

if ! command -v t3 >/dev/null 2>&1; then
  curl -fsSL https://t3.codes/install.sh | T3CODE_CHANNEL=nightly sh >&2
fi

if ! command -v gh >/dev/null 2>&1; then
  GH_VER="$(curl -fsSL https://api.github.com/repos/cli/cli/releases/latest | python3 -c 'import json,sys; print(json.load(sys.stdin)["tag_name"].lstrip("v"))')"
  curl -fsSL "https://github.com/cli/cli/releases/download/v\${GH_VER}/gh_\${GH_VER}_linux_amd64.tar.gz" \\
    | tar -xz --strip-components=2 -C "$HOME/.local/bin" "gh_\${GH_VER}_linux_amd64/bin/gh"
fi

if ! command -v codex >/dev/null 2>&1; then
  CODEX_URL="https://github.com/openai/codex/releases/latest/download/codex-x86_64-unknown-linux-musl.tar.gz"
  if curl -fsSL "$CODEX_URL" | tar -xz -C "$HOME/.local/bin" 2>/dev/null; then
    if [[ -f "$HOME/.local/bin/codex-x86_64-unknown-linux-musl" ]]; then
      mv "$HOME/.local/bin/codex-x86_64-unknown-linux-musl" "$HOME/.local/bin/codex"
    fi
    chmod +x "$HOME/.local/bin/codex" 2>/dev/null || true
  fi
  if ! command -v codex >/dev/null 2>&1 && command -v npm >/dev/null 2>&1; then
    npm install -g --no-audit --no-fund @openai/codex >&2 || true
  fi
fi

if ! command -v claude >/dev/null 2>&1; then
  curl -fsSL https://claude.ai/install.sh | bash >&2 || true
fi

if ! grep -q '\\.local/bin' "$HOME/.bashrc" 2>/dev/null; then
  printf '\\nexport PATH="$HOME/.local/bin:$PATH"\\n' >> "$HOME/.bashrc"
fi
REMOTE_BOOTSTRAP`;

const syncCredentialsScript = `echo "[t3-colab] Syncing stored credentials on '$SESSION'..." >&2

if [[ -f "$HOME/.codex/auth.json" ]]; then
  "$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -c 'umask 077 && cat > "$HOME/.codex/auth.json" && chmod 600 "$HOME/.codex/auth.json"' < "$HOME/.codex/auth.json" >&2
fi

if [[ -f "$HOME/.gitconfig" ]]; then
  "$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -c 'umask 077 && cat > "$HOME/.gitconfig"' < "$HOME/.gitconfig" >&2
fi

CLAUDE_DEST='umask 077 && cat > "$HOME/.claude/.credentials.json" && chmod 600 "$HOME/.claude/.credentials.json"'
case "$(uname -s)" in
  Darwin)
    CLAUDE_CREDS="$(security find-generic-password -s "Claude Code-credentials" -w 2>/dev/null || true)"
    if [[ -n "$CLAUDE_CREDS" ]]; then
      printf '%s' "$CLAUDE_CREDS" | "$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -c "$CLAUDE_DEST" >&2
      unset CLAUDE_CREDS
    fi
    ;;
  Linux)
    if [[ -f "$HOME/.claude/.credentials.json" ]]; then
      "$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -c "$CLAUDE_DEST" < "$HOME/.claude/.credentials.json" >&2
    fi
    ;;
esac

if command -v gh >/dev/null 2>&1; then
  GH_TOKEN="$(gh auth token </dev/null 2>/dev/null || true)"
  if [[ -n "$GH_TOKEN" ]]; then
    printf '%s\\n' "$GH_TOKEN" | "$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -c 'export PATH="$HOME/.local/bin:$PATH"; gh auth login --with-token && gh auth setup-git' >&2 || true
    unset GH_TOKEN
  fi
fi`;

const startServerScript = `echo "[t3-colab] Starting T3 Code server on '$SESSION'..." >&2
REMOTE_TOKEN_INFO="$("$COLAB_BIN" ssh -i "$SSH_KEY" -s "$SESSION" -- bash -s "$REMOTE_PORT" <<'REMOTE_START'
set -euo pipefail
REMOTE_PORT="$1"
export PATH="$HOME/.local/bin:/usr/local/bin:/usr/bin:/bin:$PATH"
T3_HOME="$HOME/.t3"
BASE_URL="http://127.0.0.1:$REMOTE_PORT"

nohup env T3CODE_NO_BROWSER=1 t3 serve --host 127.0.0.1 --port "$REMOTE_PORT" --base-dir "$T3_HOME" >"$T3_HOME/server.log" 2>&1 </dev/null &

for _ in $(seq 1 60); do
  if curl -fsS "$BASE_URL/.well-known/t3/environment" >/dev/null 2>&1; then
    break
  fi
  sleep 0.5
done

ENV_ID="$(curl -fsS "$BASE_URL/.well-known/t3/environment" | python3 -c 'import json,sys; print(json.load(sys.stdin)["environmentId"])')"
PAIR_CRED="$(t3 auth pairing create --base-dir "$T3_HOME" --json | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("credential") or d.get("token") or "")')"
if [[ -z "$PAIR_CRED" ]]; then
  echo "Failed to create pairing credential" >&2
  exit 1
fi

printf '%s' "$PAIR_CRED" | curl -fsS -X POST "$BASE_URL/oauth/token" \\
  -H "Content-Type: application/x-www-form-urlencoded" \\
  --data-urlencode "grant_type=urn:ietf:params:oauth:grant-type:token-exchange" \\
  --data-urlencode "subject_token@-" \\
  --data-urlencode "subject_token_type=urn:t3:params:oauth:token-type:environment-bootstrap" \\
  --data-urlencode "requested_token_type=urn:ietf:params:oauth:token-type:access_token" \\
  | python3 -c '
import json, sys
print(json.dumps({
    "environmentId": sys.argv[1],
    "bearerToken": json.load(sys.stdin)["access_token"],
}))
' "$ENV_ID"
REMOTE_START
)"

LOCAL_PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
PROXY_COMMAND="$(printf '%q ssh --proxy-mode -s %q -i %q' "$COLAB_BIN" "$SESSION" "$SSH_KEY")"

echo "[t3-colab] Opening SSH port forward on 127.0.0.1:$LOCAL_PORT..." >&2
# ProcessRunner terminates the provision command's process group when its scope closes.
set -m
ssh -N \\
  -o BatchMode=yes \\
  -o ExitOnForwardFailure=yes \\
  -o ServerAliveInterval=30 \\
  -o ServerAliveCountMax=3 \\
  -o StrictHostKeyChecking=no \\
  -o UserKnownHostsFile=/dev/null \\
  -o "HostKeyAlias=$SESSION" \\
  -o LogLevel=ERROR \\
  -o "ProxyCommand=$PROXY_COMMAND" \\
  -i "$SSH_KEY" \\
  -L "127.0.0.1:$LOCAL_PORT:127.0.0.1:$REMOTE_PORT" \\
  root@colab-runtime \\
  </dev/null >/dev/null 2>"$STATE_DIR/\${SESSION}.tunnel.log" &
TUNNEL_PID="$!"
disown "$TUNNEL_PID"
set +m

printf '%s\\n' "$TUNNEL_PID" > "$STATE_DIR/\${SESSION}.pid"
printf '%s\\n' "$LOCAL_PORT" > "$STATE_DIR/\${SESSION}.port"

for _ in $(seq 1 40); do
  if ! kill -0 "$TUNNEL_PID" 2>/dev/null; then
    echo "[t3-colab] SSH tunnel exited prematurely." >&2
    exit 1
  fi
  if curl -fsS "http://127.0.0.1:$LOCAL_PORT/.well-known/t3/environment" >/dev/null 2>&1; then
    printf '%s' "$REMOTE_TOKEN_INFO" | python3 -c '
import json, sys
port, session = sys.argv[1:3]
connection = json.load(sys.stdin)
print(json.dumps({
    "httpUrl": f"http://127.0.0.1:{port}",
    "wsUrl": f"ws://127.0.0.1:{port}/ws",
    "allocationHandle": session,
    "environmentId": connection["environmentId"],
    "bearerToken": connection["bearerToken"],
}))
' "$LOCAL_PORT" "$SESSION"
    exit 0
  fi
  sleep 0.5
done

echo "[t3-colab] Timed out waiting for tunneled T3 Code server on 127.0.0.1:$LOCAL_PORT." >&2
exit 1`;

const teardownScript = `HANDLE="\${T3_EPHEMERAL_ALLOCATION_HANDLE:-}"
if [[ -z "$HANDLE" && -n "\${T3_EPHEMERAL_LEASE_ID:-}" ]]; then
  HANDLE="$(session_for_lease "$T3_EPHEMERAL_LEASE_ID")"
  if [[ ! -f "$STATE_DIR/\${HANDLE}.requested" ]]; then
    exit 0
  fi
fi
if [[ -z "$HANDLE" ]]; then
  exit 0
fi
if [[ ! "$HANDLE" =~ ^t3-[A-Za-z0-9-]{1,24}$ ]]; then
  echo "[t3-colab] Invalid allocation handle." >&2
  exit 1
fi

COLAB_BIN="$(command -v colab)"
stop_session "$HANDLE"
echo "[t3-colab] Released Colab session '$HANDLE'." >&2`;

export function buildColabProvisionCommand(provisioner: EphemeralEnvironmentProvisionerColab) {
  const flags = hardwareFlags[provisioner.hardware];
  return [
    `exec bash -s --${flags ? ` ${flags}` : ""} <<'T3_COLAB'`,
    commonScript,
    provisionScript,
    ...(provisioner.syncLocalCredentials ? [syncCredentialsScript] : []),
    startServerScript,
    "T3_COLAB",
  ].join("\n");
}

export function buildColabTeardownCommand() {
  return ["exec bash -s <<'T3_COLAB'", commonScript, teardownScript, "T3_COLAB"].join("\n");
}
