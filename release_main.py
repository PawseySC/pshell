#!/usr/bin/env python3

import os
import stat
import subprocess
import sys
import tempfile
import zipfile


def main():
    try:
        with tempfile.TemporaryDirectory(prefix="pshell-") as directory:
            with zipfile.ZipFile(sys.argv[0]) as archive:
                for entry in archive.infolist():
                    parts = entry.filename.split("/")
                    if (entry.filename.startswith("/") or "\\" in entry.filename
                            or ".." in parts or stat.S_ISLNK(entry.external_attr >> 16)):
                        raise ValueError("unsafe archive entry: %s" % entry.filename)
                archive.extractall(directory)

            dependency_set = "modern" if sys.version_info >= (3, 13) else "legacy"
            environment = os.environ.copy()
            dependency_path = os.path.join(directory, "vendor", dependency_set)
            if environment.get("PYTHONPATH"):
                dependency_path += os.pathsep + environment["PYTHONPATH"]
            environment["PYTHONPATH"] = dependency_path
            process = subprocess.Popen(
                [sys.executable, os.path.join(directory, "pshell.py")] + sys.argv[1:],
                env=environment,
            )
            try:
                returncode = process.wait()
            except KeyboardInterrupt:
                returncode = process.wait()
            if returncode < 0:
                return 128 - returncode
            return returncode
    except (OSError, ValueError, zipfile.BadZipFile) as error:
        print("Unable to start bundled pshell: %s" % error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
