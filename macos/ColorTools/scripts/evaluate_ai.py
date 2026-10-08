#!/usr/bin/env python3
"""Developer evaluation runner; the macOS application does not depend on Python."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import subprocess
import time


def main():
    parser = argparse.ArgumentParser()
    for name in ("codex", "helper", "darktable", "source", "job"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--model", default="gpt-6-luna")
    parser.add_argument("--timeout", type=int, default=600)
    args = parser.parse_args()
    root = args.job.resolve()
    root.mkdir(parents=True, exist_ok=False)
    schema = {"type": "object", "properties": {
        "status": {"type": "string", "enum": ["selected", "unchanged", "needs_review"]},
        "candidateID": {"type": "string"}, "reason": {"type": "string"}},
        "required": ["status", "candidateID", "reason"], "additionalProperties": False}
    (root / "result.schema.json").write_text(json.dumps(schema))
    skill = Path(__file__).resolve().parents[1] / "Skills/keeps-color/SKILL.md"
    prompt = skill.read_text() + "\nComplete this photo independently using only Keeps tools. Return the specified result, with reason in Chinese."
    command = [str(args.codex.resolve()), "exec", "--ignore-user-config", "--ignore-rules",
               "--skip-git-repo-check", "--json", "-m", args.model, "-s", "read-only", "-C", str(root),
               "--output-schema", str(root / "result.schema.json"), "-o", str(root / "result.json")]
    for feature in ("shell_tool", "unified_exec", "apps", "hooks", "plugins", "remote_plugin", "skill_search"):
        command += ["--disable", feature]
    command += ["--enable", "skip_host_skill_discovery"]
    settings = {
        "model_reasoning_effort": "high", "web_search": "disabled", "project_doc_max_bytes": 0,
        "mcp_servers.keeps_color.required": True,
        "mcp_servers.keeps_color.default_tools_approval_mode": "auto",
        "mcp_servers.keeps_color.command": str(args.helper.resolve()),
        "mcp_servers.keeps_color.args": ["--darktable", str(args.darktable.resolve()), "--source", str(args.source.resolve()), "--job", str(root / "render")],
        "mcp_servers.keeps_color.startup_timeout_sec": 180,
        "mcp_servers.keeps_color.tool_timeout_sec": 240,
    }
    for key, value in settings.items():
        command += ["-c", key + "=" + json.dumps(value)]
    started = time.monotonic()
    version = subprocess.check_output([str(args.codex.resolve()), "--version"], text=True).strip()
    metadata = {"model": args.model, "codexVersion": version,
                "skillSHA256": hashlib.sha256(skill.read_bytes()).hexdigest(), "engineVersion": "5.6.2"}
    with (root / "events.jsonl").open("w") as output, (root / "stderr.log").open("w") as errors:
        process = subprocess.Popen(command + ["-"], stdin=subprocess.PIPE, stdout=output,
                                   stderr=errors, text=True, start_new_session=True)
        try:
            process.communicate(prompt, timeout=args.timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait()
            raise
    metadata.update(elapsedSeconds=time.monotonic()-started, exitCode=process.returncode)
    (root / "run.json").write_text(json.dumps(metadata, indent=2))
    if process.returncode:
        raise RuntimeError(f"Codex exited {process.returncode}; see {root / 'stderr.log'}")
    result = json.loads((root / "result.json").read_text())
    candidates = json.loads((root / "render/candidates.json").read_text())["candidates"]
    if result["candidateID"] not in {candidate["id"] for candidate in candidates}:
        raise RuntimeError("Model returned an unknown candidate; refusing to treat run as successful")
    if result["status"] == "selected":
        selected = json.loads((root / "render/selection.json").read_text())
        if selected["candidateID"] != result["candidateID"] or not Path(selected["fullSize"]).is_file():
            raise RuntimeError("Model selection does not match the rendered result")
    print(json.dumps({**metadata, **result}, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
