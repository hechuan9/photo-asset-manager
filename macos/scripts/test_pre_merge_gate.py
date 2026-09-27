import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class SecretGateTests(unittest.TestCase):
    def test_identifiers_pass_and_literal_secret_is_redacted(self):
        script = Path(__file__).with_name("pre_merge_gate.sh")
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "scripts").mkdir()
            shutil.copy(script, root / "scripts/pre_merge_gate.sh")
            (root / "bin").mkdir()
            swift = root / "bin/swift"
            swift.write_text("#!/bin/sh\nexit 0\n")
            swift.chmod(0o755)
            environment = dict(os.environ, PATH=f"{root / 'bin'}:{os.environ['PATH']}")
            subprocess.run(["git", "init", "--quiet", str(root)], check=True)
            source = root / "Client.swift"
            source.write_text('let authorization = "Bearer \\(token)"\nstruct DTO { let token: String }\n')

            def run_gate():
                return subprocess.run(
                    ["bash", "scripts/pre_merge_gate.sh"], cwd=root, env=environment,
                    capture_output=True, text=True,
                )

            clean = run_gate()
            self.assertEqual(clean.returncode, 0, clean.stdout + clean.stderr)
            token = "ghp_" + "aB3dE6gH9jK2mN5pQ8sT1vW4yZ7cF0iL3oR6"
            source.write_text(f'let token = "{token}"\n')
            leaked = run_gate()
            self.assertNotEqual(leaked.returncode, 0)
            self.assertIn("leaks found: 1", leaked.stdout + leaked.stderr)
            self.assertNotIn(token, leaked.stdout + leaked.stderr)


if __name__ == "__main__":
    unittest.main()
