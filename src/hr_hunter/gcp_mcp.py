from __future__ import annotations

import json
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path
from typing import Any

try:
    from mcp.server.fastmcp import FastMCP
except ImportError as exc:  # pragma: no cover - optional dependency
    raise RuntimeError(
        "MCP support is not installed. Run `uv sync --extra mcp` or `pip install '.[mcp]'`."
    ) from exc


WORKSPACE_ROOT = Path(__file__).resolve().parents[2]
REMOTE_INSTALL_SCRIPT = WORKSPACE_ROOT / "scripts" / "install_on_gcp_vm.sh"

mcp = FastMCP(
    "HR Hunter GCP",
    json_response=True,
    instructions=(
        "Manage local gcloud authentication, inspect Compute Engine VMs, and deploy the "
        "current HR Hunter workspace onto an existing GCP VM."
    ),
)


def _gcloud_binary() -> str:
    binary = shutil.which("gcloud")
    if not binary:
        raise RuntimeError("gcloud is not installed or not on PATH.")
    return binary


def _run(
    args: list[str],
    *,
    cwd: Path | None = None,
    timeout: int = 300,
    text: bool = True,
    input_data: str | bytes | None = None,
) -> dict[str, Any]:
    completed = subprocess.run(
        args,
        cwd=str(cwd) if cwd else None,
        capture_output=True,
        check=False,
        text=text,
        input=input_data,
        timeout=timeout,
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


def _workspace_file_list() -> bytes:
    result = subprocess.run(
        [
            "git",
            "-C",
            str(WORKSPACE_ROOT),
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


def _build_workspace_bundle() -> Path:
    file_list = _workspace_file_list()
    temp_dir = Path(tempfile.mkdtemp(prefix="hr-hunter-gcp-"))
    bundle_path = temp_dir / "hr-hunter-workspace.tgz"
    result = _run(
        [
            "tar",
            "-czf",
            str(bundle_path),
            "-C",
            str(WORKSPACE_ROOT),
            "--null",
            "-T",
            "-",
        ],
        text=False,
        input_data=file_list,
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
        "The `gcp_install_hr_hunter` tool packages the current workspace, copies it to the "
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
) -> dict[str, Any]:
    """Copy a local file or directory to a Compute Engine VM with gcloud scp."""
    gcloud = _gcloud_binary()
    resolved_local = Path(local_path).expanduser()
    if not resolved_local.is_absolute():
        resolved_local = (WORKSPACE_ROOT / resolved_local).resolve()
    if not resolved_local.exists():
        raise FileNotFoundError(f"Local path does not exist: {resolved_local}")

    args = [gcloud, "compute", "scp"]
    if recurse:
        args.append("--recurse")
    args.append(str(resolved_local))
    args.append(f"{instance}:{remote_path}")
    args = _apply_project_and_zone(args, project_id=project_id, zone=zone, use_iap=use_iap)
    return _run(args, timeout=1800)


@mcp.tool()
def gcp_install_hr_hunter(
    instance: str,
    zone: str,
    project_id: str | None = None,
    remote_dir: str = "~/hr-hunter",
    python_bin: str = "python3",
    scrapingbee_env_file: str = "/etc/reap/reap.env",
    use_iap: bool = False,
) -> dict[str, Any]:
    """Package the current workspace, copy it to a VM, and install HR Hunter there."""
    if not REMOTE_INSTALL_SCRIPT.exists():
        raise FileNotFoundError(f"Remote installer is missing: {REMOTE_INSTALL_SCRIPT}")

    bundle_path = _build_workspace_bundle()
    remote_bundle_path = "/tmp/hr-hunter-workspace.tgz"
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
            "copy_bundle": copy_bundle,
            "copy_script": copy_script,
        }

    install_command = " ".join(
        [
            "chmod +x",
            shlex.quote(remote_script_path),
            "&&",
            shlex.quote(remote_script_path),
            shlex.quote(remote_bundle_path),
            shlex.quote(remote_dir),
            shlex.quote(python_bin),
            shlex.quote(scrapingbee_env_file),
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
        "bundle_path": str(bundle_path),
        "remote_bundle_path": remote_bundle_path,
        "remote_dir": remote_dir,
        "copy_bundle": copy_bundle,
        "copy_script": copy_script,
        "install_result": install_result,
    }


def main() -> None:
    mcp.run()


if __name__ == "__main__":
    main()
