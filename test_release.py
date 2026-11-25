#!/usr/bin/env python3

import configparser
import glob
import json
import os
import subprocess
import sys
import tempfile
import threading
import unittest
import venv
import zipfile
from http.server import BaseHTTPRequestHandler, HTTPServer


ROOT = os.path.dirname(os.path.abspath(__file__))
RELEASE = os.path.join(ROOT, "release", "pshell")


class S3Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.server.authorizations.append(self.headers.get("Authorization", ""))
        self.server.ca_available.append(bool(glob.glob(
            os.path.join(self.server.temp_root, "pshell-*", "vendor", "*",
                         "botocore", "cacert.pem")
        )))
        body = (b'<?xml version="1.0" encoding="UTF-8"?>'
                b'<ListAllMyBucketsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">'
                b'<Buckets><Bucket><Name>example-bucket</Name>'
                b'<CreationDate>2020-01-01T00:00:00.000Z</CreationDate>'
                b'</Bucket></Buckets></ListAllMyBucketsResult>')
        self.send_response(200)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format, *args):
        pass


class ReleaseTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not os.path.isfile(RELEASE):
            raise RuntimeError("Run ./build_release before test_release.py")
        cls.temp = tempfile.TemporaryDirectory()
        cls.root = cls.temp.name
        cls.home = os.path.join(cls.root, "home")
        cls.runtime_temp = os.path.join(cls.root, "tmp")
        os.mkdir(cls.home)
        os.mkdir(cls.runtime_temp)
        environment = os.path.join(cls.root, "venv")
        venv.create(environment, with_pip=False)
        cls.python = os.path.join(environment, "Scripts" if os.name == "nt" else "bin",
                                  "python.exe" if os.name == "nt" else "python")
        cls.server = HTTPServer(("127.0.0.1", 0), S3Handler)
        cls.server.authorizations = []
        cls.server.ca_available = []
        cls.server.temp_root = cls.runtime_temp
        cls.thread = threading.Thread(target=cls.server.serve_forever)
        cls.thread.daemon = True
        cls.thread.start()
        config = configparser.ConfigParser()
        config["pawsey"] = {
            "endpoints": json.dumps({
                "private": {
                    "type": "s3", "url": "http://127.0.0.1:%d" % cls.server.server_port,
                    "access": "dummy-access", "secret": "dummy-secret",
                },
                "mediaflux": {
                    "type": "mflux", "url": "https://example.invalid:443",
                    "session": "dummy-session", "token": "dummy-token",
                },
            }),
            "remotes_current": "private",
            "remotes_home": "/",
        }
        with open(os.path.join(cls.home, ".pshell_config"), "w") as config_file:
            config.write(config_file)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.thread.join()
        cls.server.server_close()
        cls.temp.cleanup()

    def run_release(self, path, *args):
        env = {key: value for key, value in os.environ.items()
               if not key.startswith("AWS_") and key not in (
                   "PYTHONPATH", "PYTHONHOME", "HTTP_PROXY", "HTTPS_PROXY",
                   "http_proxy", "https_proxy",
               )}
        env.update({"HOME": self.home, "TMPDIR": self.runtime_temp,
                    "TEMP": self.runtime_temp, "TMP": self.runtime_temp,
                    "AWS_EC2_METADATA_DISABLED": "true", "PYTHONNOUSERSITE": "1"})
        return subprocess.run([self.python, path] + list(args), env=env, cwd=self.home,
                              stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                              universal_newlines=True, timeout=60)

    def test_s3_request_without_installed_dependencies(self):
        dependency = subprocess.run([self.python, "-I", "-c", "import boto3"],
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        self.assertNotEqual(dependency.returncode, 0,
                            "Test environment must not have boto3 installed")
        result = self.run_release(RELEASE, "-v", "2", "ls")
        self.assertEqual(result.returncode, 0, result.stdout)
        self.assertIn("example-bucket", result.stdout)
        self.assertNotIn("Unable to load data", result.stdout)
        for credential in ("dummy-access", "dummy-secret", "dummy-session", "dummy-token"):
            self.assertNotIn(credential, result.stdout)
        self.assertTrue(self.server.authorizations)
        self.assertTrue(all(header.startswith("AWS4-HMAC-SHA256")
                            for header in self.server.authorizations))
        self.assertTrue(all(self.server.ca_available))
        self.assertEqual(os.listdir(self.runtime_temp), [])

    def test_arguments_and_exit_status(self):
        version = self.run_release(RELEASE, "--version")
        self.assertEqual(version.returncode, 0, version.stdout)
        self.assertIn("pshell version", version.stdout)
        invalid = self.run_release(RELEASE, "--invalid-option")
        self.assertNotEqual(invalid.returncode, 0)
        self.assertIn("unrecognized arguments", invalid.stdout)
        self.assertEqual(os.listdir(self.runtime_temp), [])

    def test_rejects_unsafe_archive_entry(self):
        bundle = os.path.join(self.root, "unsafe.zip")
        with zipfile.ZipFile(bundle, "w") as archive:
            archive.write(os.path.join(ROOT, "release_main.py"), "__main__.py")
            archive.writestr("../escaped", "invalid")
        result = self.run_release(bundle)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("unsafe archive entry", result.stdout)
        self.assertFalse(os.path.exists(os.path.join(self.runtime_temp, "escaped")))
        self.assertEqual(os.listdir(self.runtime_temp), [])


if __name__ == "__main__":
    unittest.main()
