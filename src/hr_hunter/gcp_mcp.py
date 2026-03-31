from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import tempfile
import re
from pathlib import Path
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - optional dependency
    raise RuntimeError(
        "MCP support is not installed. Run `uv sync --extra mcp` or `pip install '.[mcp]'`."
    ) from exc


SERVER_ROOT = Path(__file__).resolve().parents[2]
REMOTE_INSTALL_SCRIPT = SERVER_ROOT / "scripts" / "install_on_gcp_vm.sh"

mcp = FastMCP(
    "HR Hunter GCP",
    json_response=True,
    instructions=(
        "Manage local gcloud authentication, inspect Compute Engine VMs, and deploy local "
        "workspaces onto existing GCP VMs."
    ),
)


def _gcloud_binary() -> str:
    binary = shutil.which("gcloud")
    if binary:
        return binary

    for candidate in ("/opt/homebrew/bin/gcloud", "/usr/local/bin/gcloud", "/usr/bin/gcloud"):
        if Path(candidate).exists():
            return candidate

    raise RuntimeError("gcloud is not installed or not on PATH.")


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 300,
    text: bool = True,
    input_data: str | bytes | None = None,
    env: dict[str, str] | None = None,
) -> dict[str, Any]:
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        check=False,
        text=text,
        input=input_data,
        timeout=timeout,
        env=env,
    )
    payload: dict[str, Any] = {
        "ok": completed.returncode == 0,
        "returncode": completed.returncode,
        "command": args,
    }
    if text:
        payload["stdout"] = completed.stdout
        payload["stderr"] = completed.stderr
    else:
        payload["stdout"] = completed.stdout.decode("utf-8", errors="replace")
        payload["stderr"] = completed.stderr.decode("utf-8", errors="replace")
    return payload


def _slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "workspace"


def _resolve_workspace_root(workspace_path: str | None = None) -> Path:
    raw_workspace_path = workspace_path or os.environ.get("HR_HUNTER_MCP_WORKSPACE_ROOT")
    if raw_workspace_path:
        candidate = Path(raw_workspace_path).expanduser()
        if not candidate.is_absolute():
            candidate = (Path.cwd() / candidate).resolve()
        else:
            candidate = candidate.resolve()
    else:
        candidate = SERVER_ROOT

    if not candidate.exists():
        raise FileNotFoundError(f"Workspace path does not exist: {candidate}")
    if not candidate.is_dir():
        raise NotADirectoryError(f"Workspace path is not a directory: {candidate}")
    return candidate


def _resolve_local_path(local_path: str, *, workspace_path: str | None = None) -> Path:
    resolved_local = Path(local_path).expanduser()
    if not resolved_local.is_absolute():
        resolved_local = (_resolve_workspace_root(workspace_path) / resolved_local).resolve()
    else:
        resolved_local = resolved_local.resolve()
    if not resolved_local.exists():
        raise FileNotFoundError(f"Local path does not exist: {resolved_local}")
    return resolved_local


def _parse_json_output(result: dict[str, Any]) -> Any:
    stdout = result.get("stdout", "")
    if not stdout.strip():
        return None
    return json.loads(stdout)


def _apply_project_and_zone(
    args: list[str],
    *,
    project_id: str | None = None,
    zone: str | None = None,
    use_iap: bool = False,
) -> list[str]:
    if project_id:
        args.extend(["--project", project_id])
    if zone:
        args.extend(["--zone", zone])
    if use_iap:
        args.append("--tunnel-through-iap")
    return args


def _workspace_file_list(workspace_root: Path) -> bytes:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(workspace_root),
            "ls-files",
            "--cached",
            "--others",
            "--exclude-standard",
            "-z",
        ],
        capture_output=True,
        check=False,
        text=False,
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.decode("utf-8", errors="replace") or "git ls-files failed")
    return result.stdout


def _build_workspace_bundle(workspace_root: Path) -> Path:
    file_list = _workspace_file_list(workspace_root)
    temp_dir = Path(tempfile.mkdtemp(prefix="hr-hunter-gcp-"))
    bundle_path = temp_dir / "hr-hunter-workspace.tgz"
    env = os.environ.copy()
    env["COPYFILE_DISABLE"] = "1"
    result = _run(
        [
            "tar",
            "-czf",
            str(bundle_path),
            "-C",
            str(workspace_root),
            "--null",
            "-T",
            "-",
        ],
        text=False,
        input_data=file_list,
        env=env,
    )
    if not result["ok"]:
        raise RuntimeError(result.get("stderr", "tar failed"))
    return bundle_path


@mcp.resource("gcp://auth-help")
def gcp_auth_help() -> str:
    return (
        "Authenticate the local gcloud CLI before using the deployment tools.\n\n"
        "Recommended sequence:\n"
        "1. `gcloud auth login`\n"
        "2. `gcloud auth application-default login`\n"
        "3. `gcloud config set project <PROJECT_ID>`\n"
        "4. Optional: `gcloud config set compute/zone <ZONE>`\n\n"
        "If you are on a headless machine, use:\n"
        "- `gcloud auth login --no-launch-browser`\n"
        "- `gcloud auth application-default login --no-launch-browser`\n"
    )


@mcp.resource("gcp://install-help")
def gcp_install_help() -> str:
    return (
        "The `gcp_install_workspace` tool packages a chosen local workspace, copies it to the "
        "target VM with `gcloud compute scp`, and runs `scripts/install_on_gcp_vm.sh` over "
        "SSH. The remote installer creates a fresh `.venv`, installs the package in editable "
        "mode, and writes a `run-hr-hunter.sh` wrapper that can read secrets from an env file "
        "such as `/etc/reap/reap.env`."
    )


@mcp.tool()
def gcloud_auth_status() -> dict[str, Any]:
    """Return the active gcloud account, current config, and ADC status."""
    gcloud = _gcloud_binary()
    auth_result = _run([gcloud, "auth", "list", "--format=json"])
    config_result = _run([gcloud, "config", "list", "--format=json"])
    user_token_result = _run([gcloud, "auth", "print-access-token"], timeout=60)
    adc_result = _run(
        [gcloud, "auth", "application-default", "print-access-token"],
        timeout=60,
    )

    auth_entries = _parse_json_output(auth_result) or []
    config = _parse_json_output(config_result) or {}
    active_accounts = [entry for entry in auth_entries if entry.get("status") == "ACTIVE"]
    return {
        "gcloud_path": gcloud,
        "active_accounts": active_accounts,
        "config": config,
        "user_token_ok": user_token_result["ok"],
        "user_token_error": None if user_token_result["ok"] else user_token_result.get("stderr", "").strip(),
        "adc_configured": adc_result["ok"],
        "adc_error": None if adc_result["ok"] else adc_result.get("stderr", "").strip(),
    }


@mcp.tool()
def gcloud_set_project(project_id: str) -> dict[str, Any]:
    """Set the active gcloud project."""
    gcloud = _gcloud_binary()
    result = _run([gcloud, "config", "set", "project", project_id], timeout=60)
    return {
        "project_id": project_id,
        "result": result,
    }


@mcp.tool()
def gcp_list_instances(
    project_id: str | None = None,
    zone: str | None = None,
    instance_filter: str | None = None,
) -> dict[str, Any]:
    """List Compute Engine instances visible to the local gcloud auth."""
    gcloud = _gcloud_binary()
    args = [gcloud, "compute", "instances", "list", "--format=json"]
    if instance_filter:
        args.extend(["--filter", instance_filter])
    args = _apply_project_and_zone(args, project_id=project_id, zone=zone)
    result = _run(args, timeout=120)
    return {
        "result": result,
        "instances": _parse_json_output(result) if result["ok"] else None,
    }


@mcp.tool()
def gcp_describe_instance(
    instance: str,
    zone: str,
    project_id: str | None = None,
) -> dict[str, Any]:
    """Describe a single Compute Engine instance."""
    gcloud = _gcloud_binary()
    args = [gcloud, "compute", "instances", "describe", instance, "--format=json"]
    args = _apply_project_and_zone(args, project_id=project_id, zone=zone)
    result = _run(args, timeout=120)
    return {
        "result": result,
        "instance": _parse_json_output(result) if result["ok"] else None,
    }


@mcp.tool()
def gcp_ssh_command(
    instance: str,
    zone: str,
    command: str,
    project_id: str | None = None,
    use_iap: bool = False,
) -> dict[str, Any]:
    """Run a shell command on a Compute Engine VM over gcloud SSH."""
    gcloud = _gcloud_binary()
    args = [gcloud, "compute", "ssh", instance, "--command", command, "--quiet"]
    args = _apply_project_and_zone(args, project_id=project_id, zone=zone, use_iap=use_iap)
    return _run(args, timeout=900)


@mcp.tool()
def gcp_copy_to_instance(
    instance: str,
    zone: str,
    local_path: str,
    remote_path: str,
    project_id: str | None = None,
    recurse: bool = False,
    use_iap: bool = False,
    workspace_path: str | None = None,
) -> dict[str, Any]:
    """Copy a local file or directory to a Compute Engine VM with gcloud scp."""
    gcloud = _gcloud_binary()
    resolved_local = _resolve_local_path(local_path, workspace_path=workspace_path)
    resolved_workspace_root = (
        str(_resolve_workspace_root(workspace_path)) if workspace_path else None
    )

    args = [gcloud, "compute", "scp"]
    if recurse:
        args.append("--recurse")
    args.append(str(resolved_local))
    args.append(f"{instance}:{remote_path}")
    args = _apply_project_and_zone(args, project_id=project_id, zone=zone, use_iap=use_iap)
    result = _run(args, timeout=1800)
    return {
        "ok": result["ok"],
        "resolved_local_path": str(resolved_local),
        "workspace_root": resolved_workspace_root,
        "result": result,
    }


def _install_workspace(
    *,
    instance: str,
    zone: str,
    project_id: str | None,
    workspace_path: str | None,
    remote_dir: str,
    python_bin: str,
    secret_env_file: str,
    use_iap: bool,
) -> dict[str, Any]:
    workspace_root = _resolve_workspace_root(workspace_path)
    bundle_path = _build_workspace_bundle(workspace_root)
    workspace_slug = _slugify(workspace_root.name)
    remote_bundle_path = f"/tmp/{workspace_slug}-workspace.tgz"
    remote_script_path = "/tmp/install_on_gcp_vm.sh"

    copy_bundle = gcp_copy_to_instance(
        instance=instance,
        zone=zone,
        local_path=str(bundle_path),
        remote_path=remote_bundle_path,
        project_id=project_id,
        recurse=False,
        use_iap=use_iap,
    )
    if not copy_bundle["ok"]:
        return {
            "ok": False,
            "stage": "copy_bundle",
            "workspace_root": str(workspace_root),
            "copy_bundle": copy_bundle,
        }

    copy_script = gcp_copy_to_instance(
        instance=instance,
        zone=zone,
        local_path=str(REMOTE_INSTALL_SCRIPT),
        remote_path=remote_script_path,
        project_id=project_id,
        recurse=False,
        use_iap=use_iap,
    )
    if not copy_script["ok"]:
        return {
            "ok": False,
            "stage": "copy_script",
            "workspace_root": str(workspace_root),
            "copy_bundle": copy_bundle,
            "copy_script": copy_script,
        }

    remote_dir_arg = remote_dir
    if remote_dir_arg == "~":
        remote_dir_arg = "$HOME"
    elif remote_dir_arg.startswith("~/"):
        remote_dir_arg = f"$HOME/{remote_dir_arg[2:]}"

    install_command = " ".join(
        [
            "chmod +x",
            shlex.quote(remote_script_path),
            "&&",
            shlex.quote(remote_script_path),
            shlex.quote(remote_bundle_path),
            remote_dir_arg if remote_dir_arg.startswith("$HOME") else shlex.quote(remote_dir_arg),
            shlex.quote(python_bin),
            shlex.quote(secret_env_file),
        ]
    )
    install_result = gcp_ssh_command(
        instance=instance,
        zone=zone,
        command=install_command,
        project_id=project_id,
        use_iap=use_iap,
    )
    return {
        "ok": install_result["ok"],
        "workspace_root": str(workspace_root),
        "bundle_path": str(bundle_path),
        "remote_bundle_path": remote_bundle_path,
        "remote_dir": remote_dir,
        "copy_bundle": copy_bundle,
        "copy_script": copy_script,
        "install_result": install_result,
    }


@mcp.tool()
def gcp_install_workspace(
    instance: str,
    zone: str,
    project_id: str | None = None,
    workspace_path: str | None = None,
    remote_dir: str | None = None,
    python_bin: str = "python3",
    secret_env_file: str = "/etc/reap/reap.env",
    use_iap: bool = False,
) -> dict[str, Any]:
    """Package a local workspace, copy it to a VM, and install it there."""
    workspace_root = _resolve_workspace_root(workspace_path)
    chosen_remote_dir = remote_dir or f"~/deployments/{_slugify(workspace_root.name)}"
    return _install_workspace(
        instance=instance,
        zone=zone,
        project_id=project_id,
        workspace_path=str(workspace_root),
        remote_dir=chosen_remote_dir,
        python_bin=python_bin,
        secret_env_file=secret_env_file,
        use_iap=use_iap,
    )


@mcp.tool()
def gcp_install_hr_hunter(
    instance: str,
    zone: str,
    project_id: str | None = None,
    workspace_path: str | None = None,
    remote_dir: str = "~/hr-hunter-vm",
    python_bin: str = "python3",
    scrapingbee_env_file: str = "/etc/reap/reap.env",
    use_iap: bool = False,
) -> dict[str, Any]:
    """Package a workspace, copy it to a VM, and install it there."""
    if not REMOTE_INSTALL_SCRIPT.exists():
        raise FileNotFoundError(f"Remote installer is missing: {REMOTE_INSTALL_SCRIPT}")
    return _install_workspace(
        instance=instance,
        zone=zone,
        project_id=project_id,
        workspace_path=workspace_path,
        remote_dir=remote_dir,
        python_bin=python_bin,
        secret_env_file=scrapingbee_env_file,
        use_iap=use_iap,
    )


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
