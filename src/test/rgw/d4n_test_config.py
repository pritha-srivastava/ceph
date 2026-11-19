"""
Configuration for the D4N distributed (two RGW) end-to-end tests.

The tests run against an already running setup: two RGWs with the D4N filter, sharing one
FoundationDB directory, on top of a RADOS cluster (e.g. vstart). Nothing is started here.

Endpoints, admin sockets and paths come from environment variables with defaults that match a
vstart build directory. Per-RGW D4N settings (cache directory, local address, write cache,
remote put, cleaning interval) are read from each running RGW through its admin socket, so the
tests always use the values the RGW actually runs with.

Environment variables:
  D4N_BUILD_DIR             build directory with bin/ and out/ (default: current directory)
  D4N_CEPH_CONF             ceph.conf used by radosgw-admin (default: $D4N_BUILD_DIR/ceph.conf)
  D4N_RGW1_PORT             port of RGW1 (default: 8000)
  D4N_RGW2_PORT             port of RGW2 (default: 8001)
  D4N_RGW1_ASOK             admin socket of RGW1 (default: $D4N_BUILD_DIR/out/radosgw.<port>.asok)
  D4N_RGW2_ASOK             admin socket of RGW2 (default: $D4N_BUILD_DIR/out/radosgw.<port>.asok)
  D4N_FDB_CLUSTER_FILE      FoundationDB cluster file (default: FoundationDB's default)
  D4N_ACCESS_KEY            S3 access key (default: test3)
  D4N_SECRET_KEY            S3 secret key (default: test3)
  D4N_MIN_CLEANING_INTERVAL minimum rgw_d4n_cache_cleaning_interval in seconds required so that
                            no version is written back during a test (default: 1800)
  D4N_CHECK_CACHE_HITS      if "1", also check RGW2's d4n_cache_hits perf counter on GETs

The S3 user is a manual prerequisite (run once from the build directory):
  ./bin/radosgw-admin -c ceph.conf user create --uid=test3 --display-name=test3 \\
      --access-key=test3 --secret-key=test3
"""

import json
import os
import subprocess

BUILD_DIR = os.environ.get('D4N_BUILD_DIR', os.getcwd())
CEPH_CONF = os.environ.get('D4N_CEPH_CONF', os.path.join(BUILD_DIR, 'ceph.conf'))
CEPH_BIN = os.path.join(BUILD_DIR, 'bin', 'ceph')
RADOSGW_ADMIN_BIN = os.path.join(BUILD_DIR, 'bin', 'radosgw-admin')

ACCESS_KEY = os.environ.get('D4N_ACCESS_KEY', 'test3')
SECRET_KEY = os.environ.get('D4N_SECRET_KEY', 'test3')

FDB_CLUSTER_FILE = os.environ.get('D4N_FDB_CLUSTER_FILE') or None
FDB_API_VERSION = 730

MIN_CLEANING_INTERVAL = int(os.environ.get('D4N_MIN_CLEANING_INTERVAL', '1800'))
CHECK_CACHE_HITS = os.environ.get('D4N_CHECK_CACHE_HITS', '0') == '1'

# Object size used by the tests: larger than one 4 MiB block, so data spans more than one block.
OBJECT_SIZE = 7 * 1024 * 1024
BLOCK_SIZE = 4 * 1024 * 1024

# How long to wait for something that should already be true when a request returns
# (e.g. the replica written by a synchronous remote put).
SETTLE_TIMEOUT = float(os.environ.get('D4N_SETTLE_TIMEOUT', '5'))


class RGWInstance:
    """One running RGW: its S3 endpoint and its admin socket."""

    def __init__(self, label, port, asok):
        self.label = label
        self.port = port
        self.endpoint = f'http://127.0.0.1:{port}'
        self.asok = asok

    def _admin_socket(self, *args):
        # stderr is kept out of the JSON: a vstart build's bin/ceph prints a
        # "DEVELOPER MODE" banner there.
        proc = subprocess.run([CEPH_BIN, '--admin-daemon', self.asok, *args],
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False)
        if proc.returncode != 0:
            raise RuntimeError(f'{" ".join(args)} failed ({proc.returncode}): '
                               f'{proc.stderr.decode(errors="replace").strip()}')
        return json.loads(proc.stdout)

    def config_get(self, option):
        """Value of a config option in the running RGW, as a string."""
        return str(self._admin_socket('config', 'get', option)[option])

    def perf_counter(self, name):
        """Sum of a perf counter over all sections of the RGW's perf dump (0 if absent)."""
        dump = self._admin_socket('perf', 'dump')
        total = 0
        for section in dump.values():
            if isinstance(section, dict) and name in section:
                value = section[name]
                total += value if isinstance(value, int) else int(value.get('avgcount', 0))
        return total

    def __repr__(self):
        return f'{self.label}({self.endpoint})'


def _rgw(label, port_var, default_port, asok_var):
    port = int(os.environ.get(port_var, default_port))
    asok = os.environ.get(asok_var, os.path.join(BUILD_DIR, 'out', f'radosgw.{port}.asok'))
    return RGWInstance(label, port, asok)


RGW1 = _rgw('RGW1', 'D4N_RGW1_PORT', '8000', 'D4N_RGW1_ASOK')
RGW2 = _rgw('RGW2', 'D4N_RGW2_PORT', '8001', 'D4N_RGW2_ASOK')
