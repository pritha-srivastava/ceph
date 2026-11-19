"""
Helpers shared by the D4N distributed end-to-end tests: S3 clients, payloads, inspection of an
RGW's on-disk D4N cache and of the FoundationDB block directory, and cleanup.

On-disk cache layout (rgw_ssd_driver.cc, parse_key()):
  <cache dir>/<url_encode(bucket id)>/<url_encode(object name)>/<url_encode(version)>#<offset>#<length>
for a data block. In a versioned bucket the D4N version is the S3 VersionId.

Block directory keys (d4n_directory.cc, BlockDirectory::build_index()):
  <url_encode(bucket id)>#<url_encode(objName)>/block/0/0                      HEAD entries
  <url_encode(bucket id)>#<url_encode(objName)>/block/<version>/<offset>/<len>  data block entries
The value is a zpp_bits serialized std::map<std::string, std::string> of fields
(FDBBlockDirectory::set_values()): blockID, version, deleteMarker, invalid, size, objName,
bucketName, creationTime, dirty, hosts ("_" separated), etag, objSize, ..., attr_<name>.
"""

import hashlib
import json
import os
import struct
import subprocess
import time
import uuid

import boto3
import botocore
import fdb

import d4n_test_config as cfg

fdb.api_version(cfg.FDB_API_VERSION)


# --- S3 ------------------------------------------------------------------------------------

def make_client(rgw):
    return boto3.client(service_name='s3',
                        aws_access_key_id=cfg.ACCESS_KEY,
                        aws_secret_access_key=cfg.SECRET_KEY,
                        endpoint_url=rgw.endpoint,
                        use_ssl=False,
                        verify=False,
                        config=botocore.config.Config(retries={'max_attempts': 1}))


def unique_name(prefix):
    # Only [a-z0-9-]: no url encoding differences, no leading '_' (which D4N escapes).
    return f'{prefix}-{uuid.uuid4().hex[:12]}'


def random_bytes(size):
    return os.urandom(size)


def md5_hex(data):
    return hashlib.md5(data).hexdigest()


def create_versioned_bucket(client, bucket):
    client.create_bucket(Bucket=bucket)
    client.put_bucket_versioning(Bucket=bucket, VersioningConfiguration={'Status': 'Enabled'})


def put_versions(client, bucket, key, payloads):
    """PUTs each payload in order; returns [(version_id, data)] in write order."""
    versions = []
    for data in payloads:
        resp = client.put_object(Bucket=bucket, Key=key, Body=data)
        version_id = resp.get('VersionId')
        if not version_id:
            raise AssertionError(f'PUT of {key} returned no VersionId; is versioning enabled?')
        versions.append((version_id, data))
    return versions


def get_object(client, bucket, key, **kwargs):
    """GET; returns (body bytes, response)."""
    resp = client.get_object(Bucket=bucket, Key=key, **kwargs)
    return resp['Body'].read(), resp


def assert_no_such_key(test, client, bucket, key, **kwargs):
    """A GET (optionally with VersionId=...) must fail with 404 NoSuchKey."""
    with test.assertRaises(botocore.exceptions.ClientError) as ctx:
        client.get_object(Bucket=bucket, Key=key, **kwargs)
    err = ctx.exception.response
    test.assertEqual(err['ResponseMetadata']['HTTPStatusCode'], 404, err)
    test.assertEqual(err['Error']['Code'], 'NoSuchKey', err)
    return err


def list_versions(client, bucket, key):
    """ListObjectVersions for one key; returns (versions, delete_markers), newest first.
    Lists the whole bucket and filters by key: with the write cache on, a listing with a Prefix
    returns nothing (FDBBucketDirectory::fdb_scan() prefix range, a known D4N issue)."""
    versions, markers = [], []
    paginator = client.get_paginator('list_object_versions')
    for page in paginator.paginate(Bucket=bucket):
        versions += [v for v in page.get('Versions', []) if v['Key'] == key]
        markers += [m for m in page.get('DeleteMarkers', []) if m['Key'] == key]
    return versions, markers


def delete_bucket_and_versions(client, bucket):
    """Best-effort cleanup: delete every version and delete marker, then the bucket."""
    try:
        paginator = client.get_paginator('list_object_versions')
        for page in paginator.paginate(Bucket=bucket):
            for entry in page.get('Versions', []) + page.get('DeleteMarkers', []):
                client.delete_object(Bucket=bucket, Key=entry['Key'], VersionId=entry['VersionId'])
        client.delete_bucket(Bucket=bucket)
    except botocore.exceptions.ClientError as e:
        print(f'cleanup of bucket {bucket} failed: {e}')


# --- radosgw-admin -------------------------------------------------------------------------

def radosgw_admin(*args, timeout=60):
    """Runs radosgw-admin against the backend only: --rgw-filter=none, because ceph.conf's
    [client] section sets rgw_filter=d4n and radosgw-admin hangs with the D4N filter loaded."""
    out = subprocess.check_output([cfg.RADOSGW_ADMIN_BIN, '-c', cfg.CEPH_CONF,
                                   '--rgw-filter=none', *args],
                                  stderr=subprocess.DEVNULL, timeout=timeout)
    return json.loads(out)


def bucket_id(bucket):
    return radosgw_admin('bucket', 'stats', '--bucket', bucket)['id']


# --- polling -------------------------------------------------------------------------------

def wait_until(predicate, timeout=cfg.SETTLE_TIMEOUT, interval=0.2):
    """Polls predicate() until it returns a truthy value or timeout; returns the last value."""
    deadline = time.monotonic() + timeout
    while True:
        value = predicate()
        if value or time.monotonic() >= deadline:
            return value
        time.sleep(interval)


# --- on-disk cache -------------------------------------------------------------------------

_URL_ENCODE_CHARS = set(b'"#%&+,/:;<>=?@[]\\^`{}')


def url_encode(s):
    """Mirror of rgw url_encode(src, encode_slash=true) (rgw_common.cc)."""
    out = []
    for c in s.encode('utf-8'):
        if c <= 0x20 or c >= 0x7f or c in _URL_ENCODE_CHARS:
            out.append(f'%{c:02X}')
        else:
            out.append(chr(c))
    return ''.join(out)


def cached_blocks(cache_dir, bkt_id, key, version):
    """Data block files of one version in an RGW's cache dir: sorted [(offset, length, path)].
    Temporary files (a "_<n>" suffix on the length) are ignored."""
    obj_dir = os.path.join(cache_dir, url_encode(bkt_id), url_encode(key))
    if not os.path.isdir(obj_dir):
        return []
    prefix = url_encode(version) + '#'
    blocks = []
    for name in os.listdir(obj_dir):
        if not name.startswith(prefix):
            continue
        parts = name[len(prefix):].split('#')
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            continue
        blocks.append((int(parts[0]), int(parts[1]), os.path.join(obj_dir, name)))
    return sorted(blocks)


def read_cached_object(blocks):
    """Concatenates block files; returns None unless they cover [0, end) without gaps."""
    data = bytearray()
    for offset, length, path in blocks:
        if offset != len(data):
            return None
        with open(path, 'rb') as f:
            chunk = f.read()
        if len(chunk) != length:
            return None
        data += chunk
    return bytes(data)


def assert_cached(test, rgw, cache_dir, bkt_id, key, version, data, timeout=cfg.SETTLE_TIMEOUT):
    """All of `data` is cached on `rgw` as blocks of `version`, with matching content."""
    def complete():
        cached = read_cached_object(cached_blocks(cache_dir, bkt_id, key, version))
        return cached is not None and len(cached) == len(data)

    wait_until(complete, timeout)
    blocks = cached_blocks(cache_dir, bkt_id, key, version)
    cached = read_cached_object(blocks)
    test.assertIsNotNone(cached, f'{rgw}: blocks of {key} version {version} missing or not '
                                 f'contiguous in {cache_dir}: {[(o, l) for o, l, _ in blocks]}')
    test.assertEqual(len(cached), len(data), f'{rgw}: cached size of {key} version {version}')
    test.assertEqual(md5_hex(cached), md5_hex(data), f'{rgw}: cached content of {key} version {version}')


# --- FoundationDB block directory ----------------------------------------------------------

def open_fdb():
    return fdb.open(cfg.FDB_CLUSTER_FILE) if cfg.FDB_CLUSTER_FILE else fdb.open()


def decode_fields(value):
    """Decodes a zpp_bits serialized std::map<std::string, std::string>
    (uint32 count, then per entry: uint32 length + key bytes, uint32 length + value bytes)."""
    offset = 0

    def take_u32():
        nonlocal offset
        (n,) = struct.unpack_from('<I', value, offset)
        offset += 4
        return n

    def take_str():
        nonlocal offset
        n = take_u32()
        s = bytes(value[offset:offset + n])
        offset += n
        return s

    fields = {}
    for _ in range(take_u32()):
        k = take_str().decode('utf-8', 'replace')
        fields[k] = take_str().decode('utf-8', 'replace')
    if offset != len(value):
        raise ValueError(f'unexpected trailing bytes in directory value ({offset} of {len(value)})')
    return fields


class DirEntry:
    def __init__(self, key, fields):
        self.key = key
        self.fields = fields

    def get(self, name, default=None):
        return self.fields.get(name, default)

    @property
    def is_head(self):
        return self.get('blockID') == '0' and self.get('size') == '0'

    @property
    def hosts(self):
        return {h for h in self.get('hosts', '').split('_') if h}

    def flag(self, name):
        return self.get(name) == '1'

    def __repr__(self):
        shown = {k: v for k, v in self.fields.items() if not k.startswith('attr_')}
        return f'DirEntry({self.key!r}, {shown})'


def object_dir_entries(db, bkt_id, key):
    """All block directory entries whose key starts with <bucket id>#: entries of `key` itself
    (plain HEAD and data blocks) and of its version-specific HEAD names, which embed `key`."""
    prefix = (url_encode(bkt_id) + '#').encode()
    entries = []
    for kv in db.get_range_startswith(prefix):
        k = kv.key.decode('utf-8', 'replace')
        if '/block/' not in k:
            continue
        try:
            fields = decode_fields(kv.value)
        except (ValueError, struct.error):
            continue
        obj_name = fields.get('objName', '')
        if obj_name == key or obj_name.endswith('_' + key) or obj_name.endswith('#' + key):
            entries.append(DirEntry(k, fields))
    return entries


def plain_head(entries, bkt_id, key):
    """The plain-name HEAD (the "latest" pointer), or None."""
    want = f'{url_encode(bkt_id)}#{url_encode(key)}/block/0/0'
    return next((e for e in entries if e.key == want), None)


def version_heads(entries, key, version):
    """Version-specific HEAD entries of one version (not the plain-name HEAD)."""
    return [e for e in entries
            if e.is_head and e.get('version') == version and e.get('objName') != key]


def data_block_entries(entries, key, version):
    return sorted((e for e in entries
                   if not e.is_head and e.get('objName') == key and e.get('version') == version),
                  key=lambda e: int(e.get('blockID', '0')))
