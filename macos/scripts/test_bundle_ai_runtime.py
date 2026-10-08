import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).with_name("bundle_ai_runtime.sh")


class AIRuntimePackagingTests(unittest.TestCase):
    def run_bundle(self, destination, **inputs):
        environment = dict(os.environ)
        environment.pop("KEEPS_CODEX_BINARY", None)
        environment.pop("KEEPS_DARKTABLE_APP", None)
        environment.pop("CONFIGURATION", None)
        environment.pop("EXPANDED_CODE_SIGN_IDENTITY", None)
        environment.update(inputs)
        return subprocess.run(["bash", str(SCRIPT), str(destination)], env=environment,
                              capture_output=True, text=True)

    def test_no_explicit_inputs_leaves_runtime_absent(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            result = self.run_bundle(destination)
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertFalse((destination / "AIEditing").exists())

    def test_release_requires_explicit_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            result = self.run_bundle(Path(temporary), CONFIGURATION="Release")
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("must both be supplied", result.stderr)

    def test_release_requires_signing_identity(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            codex = root / "codex"
            darktable = root / "darktable.app"
            cli = darktable / "Contents/MacOS/darktable-cli"
            cli.parent.mkdir(parents=True)
            for binary in (codex, root / "codex-code-mode-host", cli):
                binary.write_text("#!/bin/sh\nexit 0\n")
                binary.chmod(0o755)
            (darktable / "Contents/Info.plist").write_text("test")
            result = self.run_bundle(root / "Resources", CONFIGURATION="Release",
                                     KEEPS_CODEX_BINARY=str(codex), KEEPS_DARKTABLE_APP=str(darktable))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("requires a code signing identity", result.stderr)

    def test_missing_code_mode_host_fails_before_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            codex = root / "codex"
            darktable = root / "darktable.app"
            cli = darktable / "Contents/MacOS/darktable-cli"
            cli.parent.mkdir(parents=True)
            for binary in (codex, cli):
                binary.write_text("#!/bin/sh\nexit 0\n")
                binary.chmod(0o755)
            (darktable / "Contents/Info.plist").write_text("test")
            result = self.run_bundle(root / "Resources", CONFIGURATION="Release",
                                     KEEPS_CODEX_BINARY=str(codex), KEEPS_DARKTABLE_APP=str(darktable))
            self.assertNotEqual(result.returncode, 0)
            self.assertIn("codex-code-mode-host in the same directory", result.stderr)
            self.assertFalse((root / "Resources").exists())

    def test_incomplete_configuration_fails_without_creating_runtime(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            for inputs in ({"KEEPS_CODEX_BINARY": "/missing/codex"},
                           {"KEEPS_DARKTABLE_APP": "/missing/darktable.app"}):
                with self.subTest(inputs=inputs):
                    result = self.run_bundle(destination, **inputs)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("must both be supplied", result.stderr)
                    self.assertFalse((destination / "AIEditing").exists())

    def test_relative_and_missing_inputs_fail_before_build(self):
        with tempfile.TemporaryDirectory() as temporary:
            destination = Path(temporary)
            relative = self.run_bundle(destination, KEEPS_CODEX_BINARY="codex",
                                       KEEPS_DARKTABLE_APP="darktable.app")
            self.assertNotEqual(relative.returncode, 0)
            self.assertIn("absolute paths", relative.stderr)
            missing = self.run_bundle(destination, KEEPS_CODEX_BINARY="/missing/codex",
                                      KEEPS_DARKTABLE_APP="/missing/darktable.app")
            self.assertNotEqual(missing.returncode, 0)
            self.assertIn("complete darktable.app", missing.stderr)
            self.assertFalse((destination / "AIEditing").exists())


if __name__ == "__main__":
    unittest.main()
