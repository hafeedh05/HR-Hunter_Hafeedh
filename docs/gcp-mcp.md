# GCP MCP

This repo now includes a local MCP server that wraps the Google Cloud CLI and can deploy the current workspace to an existing Compute Engine VM.

## Install

```bash
pip install ".[mcp]"
```

Or with `uv`:

```bash
uv sync --extra mcp
```

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

## Install It Into an MCP Client

The official Python MCP SDK supports installing FastMCP servers into desktop clients:

```bash
mcp install src/hr_hunter/gcp_mcp.py
```

If you use `uv`:

```bash
uv run mcp install src/hr_hunter/gcp_mcp.py --with-editable .
```

## Available Tools

- `gcloud_auth_status`
- `gcloud_set_project`
- `gcp_list_instances`
- `gcp_describe_instance`
- `gcp_ssh_command`
- `gcp_copy_to_instance`
- `gcp_install_hr_hunter`

## What `gcp_install_hr_hunter` Does

1. Packages the current non-ignored workspace.
2. Copies the bundle and installer script to the target VM with `gcloud compute scp`.
3. Installs the project into `~/hr-hunter` by default.
4. Creates a fresh `.venv`.
5. Installs the package in editable mode.
6. Writes a `run-hr-hunter.sh` wrapper that can load secrets from `/etc/reap/reap.env` or another file you pass in.

## Remote Install Result

After install, this command should exist on the VM:

```bash
~/hr-hunter/run-hr-hunter.sh
```

Example:

```bash
~/hr-hunter/run-hr-hunter.sh search \
  --brief examples/search_briefs/sr_product_lead_ai_jan26_vm.yaml \
  --providers scrapingbee_google \
  --limit 150
```
