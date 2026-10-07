#!/usr/bin/env python3
"""Pack an isolated DSM probe from a prepared native runtime payload."""

import argparse
import io
import pathlib
import tarfile


TEMPLATE = pathlib.Path(__file__).parent / "package"


def add_file(archive, source, name, executable=False):
    info = archive.gettarinfo(str(source), arcname=name)
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    if executable:
        info.mode = 0o755
    with source.open("rb") as content:
        archive.addfile(info, content)


def runtime_member(info):
    info.uid = info.gid = 0
    info.uname = info.gname = "root"
    if info.isdir():
        info.mode = 0o755
    return info


def pack(payload, output):
    payload = pathlib.Path(payload)
    binary = payload / "bin/keeps-server"
    if not binary.is_file() or not binary.stat().st_mode & 0o111:
        raise ValueError("payload/bin/keeps-server must be executable")
    if not (payload / "runtime").is_dir():
        raise ValueError("payload/runtime is required")
    if (payload / "bin/launch-probe").exists():
        raise ValueError("bin/launch-probe is reserved by the package")
    inner = io.BytesIO()
    with tarfile.open(fileobj=inner, mode="w:gz") as archive:
        for child in sorted(payload.iterdir()):
            archive.add(child, arcname=child.name, filter=runtime_member)
        add_file(archive, TEMPLATE / "bin/launch-probe", "bin/launch-probe", True)
    with tarfile.open(output, "w") as archive:
        for source in sorted(TEMPLATE.rglob("*")):
            relative = source.relative_to(TEMPLATE)
            if "__pycache__" in relative.parts or source.suffix == ".pyc":
                continue
            if source.is_file() and relative.parts[0] != "bin":
                add_file(archive, source, str(relative), relative.parts[0] == "scripts")
        info = tarfile.TarInfo("package.tgz")
        info.size = inner.tell()
        info.mode = 0o644
        inner.seek(0)
        archive.addfile(info, inner)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--payload", required=True, type=pathlib.Path)
    parser.add_argument("--output", required=True, type=pathlib.Path)
    args = parser.parse_args()
    pack(args.payload, args.output)
