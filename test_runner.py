#!/usr/bin/env python3

import os
import subprocess
import sys
import tempfile
import unittest


ROOT = os.path.dirname(os.path.abspath(__file__))
TEST_SCRIPTS = ("test_pshell.py", "test_parser.py", "test_mfclient.py",
                "test_s3client.py", "test_runner.py")


class TestRunnerStatus(unittest.TestCase):
    def test_test_scripts_return_failure(self):
        code = (
            'import runpy, unittest; '
            'unittest.TextTestRunner.run = lambda self, suite: '
            'type("Failed", (), {"wasSuccessful": lambda self: False})(); '
            'runpy.run_path(%r, run_name="__main__")'
        )
        for script in TEST_SCRIPTS[:-1]:
            with self.subTest(script=script):
                result = subprocess.run(
                    [sys.executable, "-c", code % script], cwd=ROOT,
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
                self.assertEqual(result.returncode, 1, result.stderr)
                self.assertNotIn(b"Traceback", result.stderr)

    def test_test_all_returns_failure_and_runs_all_scripts(self):
        with tempfile.TemporaryDirectory() as directory:
            python = os.path.join(directory, "python3")
            with open(python, "w") as fake:
                fake.write('#!/bin/sh\necho "$1" >> "$CALLS"\n'
                           '[ "$1" != "$FAIL_TEST" ]\n')
            os.chmod(python, 0o755)
            calls = os.path.join(directory, "calls")
            environment = os.environ.copy()
            environment["PATH"] = directory + os.pathsep + environment["PATH"]
            environment["CALLS"] = calls
            for fail in ("", TEST_SCRIPTS[0], TEST_SCRIPTS[-1]):
                with self.subTest(fail=fail):
                    with open(calls, "w"):
                        pass
                    environment["FAIL_TEST"] = fail
                    result = subprocess.run(["./test_all"], cwd=ROOT, env=environment)
                    self.assertEqual(result.returncode, 1 if fail else 0)
                    with open(calls) as recorded:
                        self.assertEqual(recorded.read().splitlines(), list(TEST_SCRIPTS))


if __name__ == "__main__":
    unittest.main(verbosity=2)
