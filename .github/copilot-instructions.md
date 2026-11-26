# pshell

SFTP-like command line client for Mediaflux and S3 (Pawsey). Python >= 3.6, standard library only except `boto3` (optional; needed only for S3).

## Build, test, run

- Run: `python3 pshell.py [-c config] [-v 0|1|2] [-i script] [-o failed_script] [-u url -t mflux|s3] ["cmd1 && cmd2"]`
- All tests: `./test_all` (runs the four main test modules and `test_runner.py` with `python3`; release bundle tests are separate)
- Single module: `python3 test_parser.py` (also `test_mfclient.py`, `test_s3client.py`, `test_pshell.py`)
- The four main test modules use `unittest` with a custom `__main__` that builds the suite from a `test_class_list`. Single method: `python3 -m unittest test_s3client.s3client_standard.test_convert` — except `test_s3client.py`/`test_mfclient.py`, whose `setUp` uses a client global created in `__main__` (and `test_pshell.py`, which uses `config`, `python`, `script`, `verbosity` globals); for those, edit `test_class_list` and run as a script.
- `test_mfclient.py` / `test_s3client.py` use dummy clients (no server needed); `test_pshell.py` runs `pshell.py` as a subprocess and checks exit codes.
- No linter configured. GitHub Actions runs the full suite and release bundle tests on Python 3.9, 3.12 and 3.6 (in a container). The older Bitbucket pipeline only runs `python test_mfclient.py` on python:3.6.
- Packaging: `build_test` calls `build_release`, which uses `release_main.py` as `__main__.py` and bundles `pshell.py`, `parser.py`, `mfclient.py`, `s3client.py`, `util.py`, `LICENSE`, `NOTICE`, and both dependency sets into an executable zip. Add new application modules to the file list in `build_release`. The release script stamps the `build` line in its staged copy of `pshell.py`.
- `v3/` is a legacy/experimental tree (FUSE mount, older mfclient); `keystone.py` is a standalone helper. Main code is at the repo root.

## Architecture

- `pshell.py` – entry point only: argparse, logging, loads `~/.pshell_config` (falls back to `./.pshell_config`), builds endpoints, configures readline, then either runs `parser.loop_interactively()` or feeds `-i` script lines / `&&`-split command-line text to `onecmd()`. Non-interactive failures `sys.exit(-1)`; tests depend on those exit codes.
- `parser.py` – `parser(cmd.Cmd)` holds all user commands (`do_*`), tab completion (`complete_*` delegating to `remote_complete`), the unified get/put progress reporter, a thread pool (`thread_max`), and the `remotes` dict (name → client) with `remotes_current` and `cwd`. Unknown commands fall through `default()` to the active remote's `command()` passthrough; failure raises `SyntaxError`.
- `mfclient.py` (`mf_client`) and `s3client.py` (`s3_client`) are interchangeable backends. `parser.remote_add` picks one by endpoint `type` (`mflux` / `s3`) via `from_endpoint()`, and `endpoint()` serialises it back to config. The parser calls the same method set on both (`cd`, `ls_iter`, `get`, `put`, `rm`, `mkdir`, `rmdir`, `publish`, `info_iter`, `complete_path`, `command`, `login`, `status`...). When adding a command, implement it in both clients (or fail gracefully in one) and add `do_*`/`complete_*` in `parser.py`.
- `util.py` – config file normalisation and endpoint URL helpers.
- Config: INI file with a section per config name (default `pawsey`); `endpoints` is a JSON dict of remote name → `{type, url, ...}`, plus `remotes_current` and `remotes_home`. First-run `pawsey` defaults are hard-coded in `pshell.py`.
- `parser.precmd` blocks commands when the active remote isn't authenticated, except those in the `local_commands` allowlist in `requires_auth`; update it when adding a command that doesn't need a server.

## Conventions

- Procedural style; comments are `# ...` at column 0 above the block they describe, and `# ---` separates methods in `parser.py`.
- Remote paths are always `/`-separated; for S3, `path_convert()` maps `/bucket/prefix/key`, and a trailing `/` marks a prefix rather than a key.
- `boto3` is imported in a guarded block in `s3client.py`; release builds bundle it for S3 support, while source runs require it to be installed. don't add other non-stdlib dependencies.
- Bump `version` in `pshell.py` for releases.
