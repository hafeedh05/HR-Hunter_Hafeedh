# GCP MCP

This repo now includes a local MCP server that wraps the Google Cloud CLI and can deploy local workspaces to existing Compute Engine VMs.

## Install

```bash
pip install ".[mcp]"
```

Or with `uv`:

```bash
uv sync --extra mcp
```

If `uv` is not installed, use a plain virtual environment:

```bash
python3 -m venv .mcp-venv
.mcp-venv/bin/python -m pip install --upgrade pip setuptools wheel
.mcp-venv/bin/python -m pip install -e ".[mcp]"
```

For a Codex-global install that is available from every repo, use:

```bash
./scripts/install_codex_global_mcp.sh
```

That creates a stable venv under `~/.codex/mcp/hr-hunter-gcp`.

## Authenticate gcloud

Run these locally before you use the MCP server:

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <PROJECT_ID>
```

Optional default zone:

```bash
gcloud config set compute/zone <ZONE>
```

## Run the MCP Server

```bash
python src/hr_hunter/gcp_mcp.py
```

If you use the installed script entrypoint:

```bash
hr-hunter-gcp-mcp
```

If you installed into `.mcp-venv`, this also works:

```bash
.mcp-venv/bin/python -m hr_hunter.gcp_mcp
```

## Install It Into an MCP Client

The official Python MCP SDK supports installing FastMCP servers into desktop clients:

```bash
mcp install src/hr_hunter/gcp_mcp.py
```

If you use `uv`:

```bash
uv run mcp install src/hr_hunter/gcp_mcp.py --with-editable .
```

If `uv` is unavailable, point your MCP client at the venv-backed Python module instead:

- command: `/Users/ahmad/HR Hunter/.mcp-venv/bin/python`
- args: `["-m", "hr_hunter.gcp_mcp"]`

For Codex specifically, you can register the global install in `~/.codex/config.toml`:

```toml
[mcp_servers.hrHunterGcp]
command = "/Users/ahmad/.codex/mcp/hr-hunter-gcp/.venv/bin/python"
args = ["-m", "hr_hunter.gcp_mcp"]
```

## Available Tools

- `gcloud_auth_status`
- `gcloud_set_project`
- `gcp_list_instances`
- `gcp_describe_instance`
- `gcp_ssh_command`
- `gcp_copy_to_instance`
- `gcp_install_workspace`
- `gcp_install_hr_hunter`

`gcp_install_workspace` accepts an optional `workspace_path` and can deploy any git-tracked repo. `gcp_install_hr_hunter` is kept as a compatibility wrapper and still defaults to `~/hr-hunter-vm`.

## What `gcp_install_workspace` Does

1. Packages the current non-ignored workspace.
2. Copies the bundle and installer script to the target VM with `gcloud compute scp`.
3. Installs the project into `~/deployments/<workspace-name>` by default.
4. Creates a fresh `.venv`.
5. Installs the package in editable mode.
6. Writes a `run-hr-hunter.sh` wrapper that can load secrets from `/etc/reap/reap.env` or another file you pass in.

## Remote Install Result

After install, this command should exist on the VM:

```bash
~/deployments/<workspace-name>/run-hr-hunter.sh
```

Example:

```bash
~/deployments/hr-hunter/run-hr-hunter.sh search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26_vm.yaml \
  --providers scrapingbee_google \
  --limit 150
```
