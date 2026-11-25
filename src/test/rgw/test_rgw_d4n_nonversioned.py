"""
D4N distributed end-to-end tests: non-versioned bucket, write cache on, remote put on.

Runs against a live setup of two RGWs (RGW1, RGW2) with the D4N filter, sharing one
FoundationDB block directory, over RADOS. RGW1 replicates to RGW2
(rgw_d4n_remote_cache_address), synchronously (rgw_d4n_async_remote_put=false). RGW2 may or may
not replicate to RGW1; the tests read its rgw_d4n_remote_cache_address and expect accordingly.

The tests cover PUT, GET, DELETE and COPY of non-versioned objects:
  - PUT on RGW1, GET on RGW2: served from RGW2's replica, dirty in the cache and in the directory;
  - overwrite on RGW2: both RGWs return the new data, which is cached on both, and the replaced
    dirty version is tombstoned; with replication from RGW2 turned off for the overwrite
    (rgw_d4n_remote_put=false on RGW2 through its admin socket), RGW1 caches v2 clean on a GET;
  - range GETs on RGW1, served from RGW1's cache, and range GETs on RGW1 of an object cached only on
    RGW2 (only the blocks read are cached on RGW1, clean);
  - object tagging, user metadata and the content type of a dirty object (stored on every HEAD,
    returned by HeadObject and GetObject on both RGWs, the object stays dirty);
  - conditional PUTs (If-Match, If-None-Match) against a dirty and a deleted object;
  - small (one block) and 0-byte objects, and a name starting with "_";
  - a multipart upload and a copy made by the backend over a dirty object (the replaced dirty
    version is tombstoned, the listing shows the new object);
  - DELETE: on RGW1 and on RGW2, GETs on both RGWs return NoSuchKey, also when an older copy of the
    object is in the backend; delete of a clean (backend) object; delete after an overwrite on RGW2;
    delete of a missing key and a second delete; a PUT after a DELETE creates a new version;
  - COPY: copy of a dirty object to a new key and over a dirty object (cached dirty on both RGWs,
    the replaced version tombstoned), a copy on RGW2 of a source cached only on RGW1, and the
    copy's independence from its source, a copy of a deleted object (404, nothing written), a
    copy on RGW2 of a replicated source, a copy onto itself (REPLACE) and a copy into a versioned
    bucket.

In a non-versioned bucket D4N gives each PUT its own version (the plain-name HEAD's "version"),
which names the block files (<cache dir>/<bucket id>/<name>/<version>#<offset>#<length>) and the
version HEAD "_:<version>_<name>" in the directory.

rgw_d4n_cache_cleaning_interval must be long (see D4N_MIN_CLEANING_INTERVAL in
d4n_test_config.py): every object must still be dirty when a test ends.

Run from the build directory, with the cluster and both RGWs up:
  python3 ../src/test/rgw/test_rgw_d4n_nonversioned.py -v
See d4n_test_config.py for the environment variables and the S3 user prerequisite.
"""

import os
import subprocess
import unittest

import botocore

import d4n_test_config as cfg
import d4n_test_utils as u


class D4NNonVersionedWriteCacheTest(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.rgw1, cls.rgw2 = cfg.RGW1, cfg.RGW2
        cls._check_rgw_config()

        cls.c1 = u.make_client(cls.rgw1)
        cls.c2 = u.make_client(cls.rgw2)
        for rgw, client in ((cls.rgw1, cls.c1), (cls.rgw2, cls.c2)):
            try:
                client.list_buckets()
            except botocore.exceptions.ClientError as e:
                raise AssertionError(
                    f'{rgw}: S3 credentials rejected ({e}); create the user first, see '
                    f'd4n_test_config.py') from e

        cls.bucket = u.unique_name('d4n-plain')
        cls.c1.create_bucket(Bucket=cls.bucket)
        cls.bucket_id = u.bucket_id(cls.bucket)
        # Destination of copies into a versioned bucket.
        cls.bucket_v = u.unique_name('d4n-versioned')
        u.create_versioned_bucket(cls.c1, cls.bucket_v)
        cls.bucket_v_id = u.bucket_id(cls.bucket_v)
        cls.db = u.open_fdb()

    @classmethod
    def tearDownClass(cls):
        for bucket in (cls.bucket, cls.bucket_v):
            u.delete_bucket_and_versions(cls.c1, bucket)

    @classmethod
    def _check_rgw_config(cls):
        """Reads each RGW's live D4N settings; skips the suite if the setup does not match."""
        cls.cache_dir = {}
        cls.address = {}
        problems = []
        for rgw in (cls.rgw1, cls.rgw2):
            try:
                get = rgw.config_get
                cls.cache_dir[rgw.label] = get('rgw_d4n_l1_datacache_persistent_path')
                cls.address[rgw.label] = get('rgw_d4n_local_rgw_address')
                expected = {
                    'rgw_filter': 'd4n',
                    'rgw_d4n_directory_type': 'fdb',
                    'd4n_writecache_enabled': 'true',
                    'rgw_d4n_remote_put': 'true',
                    'rgw_d4n_async_remote_put': 'false',
                }
                for option, value in expected.items():
                    actual = get(option)
                    if actual != value:
                        problems.append(f'{rgw}: {option}={actual}, expected {value}')
                interval = int(get('rgw_d4n_cache_cleaning_interval'))
                if interval < cfg.MIN_CLEANING_INTERVAL:
                    problems.append(f'{rgw}: rgw_d4n_cache_cleaning_interval={interval}, needs >= '
                                    f'{cfg.MIN_CLEANING_INTERVAL} so that nothing is written back '
                                    f'during a test')
            except Exception as e:  # admin socket missing, RGW not running, ...
                problems.append(f'{rgw}: cannot read config through {rgw.asok}: {e}')
        if not problems:
            remote = cls.rgw1.config_get('rgw_d4n_remote_cache_address')
            if remote != cls.address[cls.rgw2.label]:
                problems.append(f'{cls.rgw1}: rgw_d4n_remote_cache_address={remote}, expected '
                                f'RGW2 ({cls.address[cls.rgw2.label]})')
            # Whether a PUT on RGW2 is replicated to RGW1 (not required by the suite).
            cls.rgw2_replicates = (cls.rgw2.config_get('rgw_d4n_remote_cache_address')
                                   == cls.address[cls.rgw1.label])
        if problems:
            raise unittest.SkipTest('D4N setup does not match this suite:\n  ' + '\n  '.join(problems))

    # --- per test ------------------------------------------------------------------------

    def setUp(self):
        self.key = u.unique_name('obj')
        self.data = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=self.key, Body=self.data)
        self.version = self.latest_version()

    # --- helpers -------------------------------------------------------------------------

    def entries(self, key=None):
        return u.object_dir_entries(self.db, self.bucket_id, key or self.key)

    def plain_head(self, entries, bkt_id, key):
        """The plain-name HEAD (the "latest" pointer), or None. D4N stores it under "_" + name for a name
        starting with "_" (get_latest_head_block_name()); data blocks keep the name as is."""
        name = '_' + key if key.startswith('_') else key
        want = f'{u.url_encode(bkt_id)}#{u.url_encode(name)}/block/0/0'
        return next((e for e in entries if e.key == want), None)

    def null_head(self, entries, key):
        """The "_:null_<name>" HEAD, which mirrors the plain-name HEAD in a non-versioned bucket."""
        want = f'{u.url_encode(self.bucket_id)}#{u.url_encode("_:null_" + key)}/block/0/0'
        return next((e for e in entries if e.key == want), None)

    def latest_version(self, key=None, dirty=True):
        """The D4N version of the current object: the plain-name HEAD's version. The plain-name HEAD
        and the "_:null_" HEAD are valid, name the same version, and are dirty iff dirty."""
        key = key or self.key
        entries = self.entries(key)
        head = self.plain_head(entries, self.bucket_id, key)
        self.assertIsNotNone(head, f'no plain-name HEAD for {key}')
        self.assertTrue(head.get('version'), head)
        null = self.null_head(entries, key)
        self.assertIsNotNone(null, f'no "_:null_" HEAD for {key}')
        for h in (head, null):
            self.assertFalse(h.flag('invalid'), h)
            self.assertFalse(h.flag('deleteMarker'), h)
            self.assertEqual(h.flag('dirty'), dirty, h)
            self.assertEqual(h.get('version'), head.get('version'), h)
        return head.get('version')

    def in_backend(self, key):
        """Whether the backend (RADOS, read without the D4N filter) has the object."""
        try:
            u.radosgw_admin('object', 'stat', '--bucket', self.bucket, '--object', key)
            return True
        except subprocess.CalledProcessError:
            return False

    def assert_in_backend(self, key, expected):
        self.assertEqual(self.in_backend(key), expected,
                         f'{key} {"missing from" if expected else "present in"} the backend')

    def assert_get(self, client, rgw, data, key=None, **kwargs):
        key = key or self.key
        body, resp = u.get_object(client, self.bucket, key, **kwargs)
        self.assertIn(resp.get('VersionId'), (None, 'null'), f'{rgw}: VersionId of GET {key} {kwargs}')
        self.assertEqual(len(body), len(data), f'{rgw}: size of GET {key} {kwargs}')
        self.assertEqual(u.md5_hex(body), u.md5_hex(data), f'{rgw}: content of GET {key} {kwargs}')

    def assert_cached_on(self, rgw, version, data, dirty, key=None):
        """All of data is cached on rgw as valid blocks of version, each with user.rgw.dirty set iff
        dirty (a block written by a PUT or received by replication is dirty, one cached by a read is
        not)."""
        key = key or self.key
        cache_dir = self.cache_dir[rgw.label]
        u.assert_cached(self, rgw, cache_dir, self.bucket_id, key, version, data)
        for offset, length, path in u.cached_blocks(cache_dir, self.bucket_id, key, version):
            xattrs = u.cached_xattrs(path)
            self.assertNotEqual(xattrs.get('user.rgw.invalid'), '1', f'{rgw}: {path} xattrs {xattrs}')
            if dirty:
                self.assertEqual(xattrs.get('user.rgw.dirty'), '1', f'{rgw}: {path} xattrs {xattrs}')
            else:
                self.assertNotEqual(xattrs.get('user.rgw.dirty'), '1', f'{rgw}: {path} xattrs {xattrs}')

    def assert_not_cached_on(self, rgw, version, key=None):
        blocks = u.cached_blocks(self.cache_dir[rgw.label], self.bucket_id, key or self.key, version)
        self.assertEqual(blocks, [], f'{rgw}: blocks of {key or self.key} version {version} are cached')

    def assert_dirty_in_directory(self, version, data, holders, key=None):
        """The version HEAD and the data block entries of version are dirty and valid, and every RGW
        in holders is a data block holder."""
        key = key or self.key
        entries = self.entries(key)
        heads = u.version_heads(entries, key, version)
        self.assertTrue(heads, f'no version HEAD for {key} {version}')
        for head in heads:
            self.assertTrue(head.flag('dirty'), head)
            self.assertFalse(head.flag('invalid'), head)
        blocks = u.data_block_entries(entries, key, version)
        self.assertTrue(blocks, f'no data block entries for {key} {version}')
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(data), blocks)
        addresses = {self.address[rgw.label] for rgw in holders}
        for block in blocks:
            self.assertTrue(block.flag('dirty'), block)
            self.assertFalse(block.flag('invalid'), block)
            self.assertTrue(addresses <= block.hosts, f'data block holders: {block}')

    def assert_invalidated(self, version, data, key=None, marked_on=None):
        """A dirty version that was deleted or replaced, until the cleaning cycle removes it: its
        version HEAD is invalid (so it is never written back); its data block entries are still there
        and dirty; its block files are still on both RGWs, dirty. On the RGWs in marked_on (default:
        both) they are also marked invalid (user.rgw.invalid, set by
        LFUDAPolicy::invalidate_dirty_object() on the RGW that handles the request and, through the
        remote put or delete, on the other RGW); elsewhere they are not marked, and the cleaning cycle
        finds the version invalid in the directory instead."""
        key = key or self.key
        marked_on = (self.rgw1, self.rgw2) if marked_on is None else marked_on
        entries = self.entries(key)
        heads = u.version_heads(entries, key, version)
        self.assertTrue(heads, f'no version HEAD for {key} {version}')
        for head in heads:
            self.assertTrue(head.flag('invalid'), f'{key} {version} not invalidated: {head}')
        blocks = u.data_block_entries(entries, key, version)
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(data), blocks)
        for block in blocks:
            self.assertTrue(block.flag('dirty'), block)
        for rgw in (self.rgw1, self.rgw2):
            files = u.cached_blocks(self.cache_dir[rgw.label], self.bucket_id, key, version)
            self.assertEqual(sum(length for _, length, _ in files), len(data),
                             f'{rgw}: blocks of {key} {version}: {files}')
            for offset, length, path in files:
                xattrs = u.cached_xattrs(path)
                self.assertEqual(xattrs.get('user.rgw.invalid') == '1', rgw in marked_on,
                                 f'{rgw}: {path} xattrs {xattrs}')
                self.assertEqual(xattrs.get('user.rgw.dirty'), '1', f'{rgw}: {path} xattrs {xattrs}')

    def assert_deleted(self, version, data, key=None):
        """A dirty object deleted: GETs on both RGWs return NoSuchKey; the plain-name and "_:null_"
        HEADs still name the deleted version, marked invalid; the version is invalidated (see
        assert_invalidated()) and nothing is in the backend."""
        key = key or self.key
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label, key=key):
                u.assert_no_such_key(self, client, self.bucket, key)
        entries = self.entries(key)
        for name, head in (('plain-name', self.plain_head(entries, self.bucket_id, key)),
                           ('"_:null_"', self.null_head(entries, key))):
            self.assertIsNotNone(head, f'no {name} HEAD for deleted {key}')
            self.assertEqual(head.get('version'), version, head)
            self.assertTrue(head.flag('invalid'), f'{name} HEAD of a deleted object: {head}')
        self.assert_invalidated(version, data, key)
        self.assert_in_backend(key, False)

    def assert_nothing_written(self, key):
        """A request that failed wrote nothing for key: no directory entries, no cache files on either
        RGW, nothing in the backend."""
        entries = self.entries(key)
        self.assertEqual(entries, [], f'directory entries of {key}: {entries}')
        for rgw in (self.rgw1, self.rgw2):
            obj_dir = os.path.join(self.cache_dir[rgw.label], u.url_encode(self.bucket_id), u.url_encode(key))
            files = os.listdir(obj_dir) if os.path.isdir(obj_dir) else []
            self.assertEqual(files, [], f'{rgw}: cache files of {key}: {files}')
        self.assert_in_backend(key, False)

    def listed(self, client, key):
        """(ETag, Size) of key in a ListObjectsV2 of the bucket, or None if it is not listed. Lists the
        whole bucket and filters by key (a listing with a Prefix returns nothing with the write cache
        on, a known D4N issue)."""
        for page in client.get_paginator('list_objects_v2').paginate(Bucket=self.bucket):
            for o in page.get('Contents', []):
                if o['Key'] == key:
                    return o['ETag'], o['Size']
        return None

    def copy(self, client, src_key, dst_key, **kwargs):
        """CopyObject within the bucket; returns the copy's ETag."""
        resp = client.copy_object(Bucket=self.bucket, Key=dst_key,
                                  CopySource={'Bucket': self.bucket, 'Key': src_key}, **kwargs)
        return resp['CopyObjectResult']['ETag']

    def etag(self, key=None):
        return self.c1.head_object(Bucket=self.bucket, Key=key or self.key)['ETag']

    def assert_copy_dirty(self, dst, version, data):
        """The copy is cached dirty on both RGWs (written on one, replicated to the other), its
        directory entries are dirty with both RGWs as holders, it is not in the backend, and GETs on
        both RGWs return data."""
        self.assert_cached_on(self.rgw1, version, data, dirty=True, key=dst)
        self.assert_cached_on(self.rgw2, version, data, dirty=True, key=dst)
        self.assert_dirty_in_directory(version, data, (self.rgw1, self.rgw2), key=dst)
        self.assert_in_backend(dst, False)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label, key=dst):
                self.assert_get(client, rgw, data, key=dst)

    def multipart_put(self, key, data):
        """A one-part multipart upload: the backend stores the object and D4N caches only its
        (clean) HEAD."""
        upload_id = self.c1.create_multipart_upload(Bucket=self.bucket, Key=key)['UploadId']
        part = self.c1.upload_part(Bucket=self.bucket, Key=key, UploadId=upload_id, PartNumber=1, Body=data)
        self.c1.complete_multipart_upload(
            Bucket=self.bucket, Key=key, UploadId=upload_id,
            MultipartUpload={'Parts': [{'ETag': part['ETag'], 'PartNumber': 1}]})

    # --- tests ---------------------------------------------------------------------------

    def test_put_rgw1_get_rgw2(self):
        """An object written on RGW1 is read back from RGW2 with the same content, served from RGW2's
        local replica: its blocks are cached on RGW1 and RGW2 with the dirty xattr, the directory
        entries are dirty with RGW1 and RGW2 as holders, and it is not in the backend yet."""
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        hits = self.rgw2.perf_counter('d4n_cache_hits') if cfg.CHECK_CACHE_HITS else 0

        self.assert_get(self.c2, self.rgw2, self.data)

        if cfg.CHECK_CACHE_HITS:
            self.assertGreater(self.rgw2.perf_counter('d4n_cache_hits'), hits,
                               'RGW2 did not serve the GET from its cache')
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assertEqual(self.latest_version(), self.version)
        self.assert_in_backend(self.key, False)

    def test_overwrite_on_rgw2(self):
        """v1 written on RGW1 is overwritten on RGW2 with v2: GETs on RGW1 and RGW2 return v2, v2 is
        cached on both RGWs, and the replaced (dirty) v1 is invalidated so that its write-back cannot
        overwrite v2."""
        v1 = self.version
        d2 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c2.put_object(Bucket=self.bucket, Key=self.key, Body=d2)
        v2 = self.latest_version()
        self.assertNotEqual(v2, v1)

        # Written on RGW2: dirty there, and on RGW1 too if RGW2 replicates to RGW1.
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        if self.rgw2_replicates:
            self.assert_cached_on(self.rgw1, v2, d2, dirty=True)

        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, d2)

        # Cached locally on both RGWs now; without replication RGW1 cached v2 (clean) on the GET.
        self.assert_cached_on(self.rgw1, v2, d2, dirty=self.rgw2_replicates)
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        self.assert_dirty_in_directory(v2, d2, (self.rgw1, self.rgw2))
        self.assertEqual(self.latest_version(), v2)
        self.assert_invalidated(v1, self.data)
        self.assert_in_backend(self.key, False)

    def test_overwrite_on_rgw2_without_replication(self):
        """As test_overwrite_on_rgw2, with replication from RGW2 turned off for the overwrite
        (rgw_d4n_remote_put=false on RGW2, read at runtime): v2 is cached dirty on RGW2 only; a GET on
        RGW1 fetches v2 from RGW2 and caches it on RGW1 clean (no dirty xattr), and RGW1 is added to
        the holders of the (still dirty) directory entries."""
        v1 = self.version
        d2 = u.random_bytes(cfg.OBJECT_SIZE)
        self.rgw2.config_set('rgw_d4n_remote_put', 'false')
        try:
            self.assertEqual(self.rgw2.config_get('rgw_d4n_remote_put'), 'false')
            self.c2.put_object(Bucket=self.bucket, Key=self.key, Body=d2)
        finally:
            self.rgw2.config_set('rgw_d4n_remote_put', 'true')
        v2 = self.latest_version()
        self.assertNotEqual(v2, v1)

        # Not replicated: dirty on RGW2 only.
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        rgw1_blocks = u.cached_blocks(self.cache_dir[self.rgw1.label], self.bucket_id, self.key, v2)
        self.assertEqual(rgw1_blocks, [], 'v2 was replicated to RGW1 although rgw_d4n_remote_put=false '
                                          'on RGW2 (not read at runtime?)')
        self.assert_dirty_in_directory(v2, d2, (self.rgw2,))

        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, d2)

        # RGW1 cached v2 on the GET, clean; the directory entries stay dirty, now held by both RGWs.
        self.assert_cached_on(self.rgw1, v2, d2, dirty=False)
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        self.assert_dirty_in_directory(v2, d2, (self.rgw1, self.rgw2))
        self.assertEqual(self.latest_version(), v2)
        # v1 is invalidated by RGW2, which handled the overwrite; with no remote put RGW1 is not told,
        # so RGW1's v1 blocks are not marked (its cleaning finds v1's HEAD invalid in the directory).
        self.assert_invalidated(v1, self.data, marked_on=(self.rgw2,))
        self.assert_in_backend(self.key, False)

    def test_range_get_rgw1(self):
        """Range GETs on RGW1 (inside the first block, across the block boundary, the end of the
        object) return the right bytes, from the blocks cached on RGW1; the cache and directory are
        unchanged by the reads."""
        size = cfg.OBJECT_SIZE
        ranges = [
            (0, 1023),
            (cfg.BLOCK_SIZE - 512, cfg.BLOCK_SIZE + 511),
            (size - 1000, size - 1),
        ]
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        for first, last in ranges:
            with self.subTest(range=(first, last)):
                body, resp = u.get_object(self.c1, self.bucket, self.key, Range=f'bytes={first}-{last}')
                self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 206)
                self.assertEqual(resp.get('ContentRange'), f'bytes {first}-{last}/{size}')
                self.assertEqual(body, self.data[first:last + 1])
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assertEqual(self.latest_version(), self.version)

    def test_delete(self):
        """A DELETE on RGW1: GETs on RGW1 and RGW2 return NoSuchKey; the HEADs are invalid and the
        deleted dirty version is invalidated in the directory and on both RGWs (the cleaning cycle
        removes it); nothing is in the backend."""
        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(self.version, self.data)

    def test_delete_removes_older_backend_copy(self):
        """x is first written by a multipart upload (in the backend, clean HEAD cached), then
        overwritten by a dirty PUT, then deleted. GETs on both RGWs must return NoSuchKey: the delete
        of the dirty version also removes the older copy in the backend, which a GET would otherwise
        fall back to on the invalid HEAD."""
        x = u.unique_name('obj')
        d0 = u.random_bytes(cfg.OBJECT_SIZE)
        self.multipart_put(x, d0)
        # In the backend; D4N cached only its clean HEAD, no blocks.
        v0 = self.latest_version(x, dirty=False)
        self.assert_in_backend(x, True)
        for rgw in (self.rgw1, self.rgw2):
            self.assert_not_cached_on(rgw, v0, key=x)

        d1 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=x, Body=d1)
        v1 = self.latest_version(x)
        self.assertNotEqual(v1, v0)
        self.assert_cached_on(self.rgw1, v1, d1, dirty=True, key=x)
        self.assert_cached_on(self.rgw2, v1, d1, dirty=True, key=x)
        self.assert_dirty_in_directory(v1, d1, (self.rgw1, self.rgw2), key=x)
        self.assert_in_backend(x, True)  # the older copy, until the delete

        self.c1.delete_object(Bucket=self.bucket, Key=x)

        self.assert_deleted(v1, d1, key=x)  # includes: no longer in the backend

    def test_delete_then_put_again(self):
        """After a DELETE, a PUT of the same key creates a new D4N version: the plain-name HEAD is
        valid and points to it, cached dirty on both RGWs; the deleted version stays invalidated."""
        v1 = self.version
        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(v1, self.data)

        d2 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=self.key, Body=d2)
        v2 = self.latest_version()
        self.assertNotEqual(v2, v1)

        self.assert_cached_on(self.rgw1, v2, d2, dirty=True)
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        self.assert_dirty_in_directory(v2, d2, (self.rgw1, self.rgw2))
        self.assert_invalidated(v1, self.data)
        self.assert_in_backend(self.key, False)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, d2)

    def test_copy_dirty_object_to_new_key(self):
        """Copy of a dirty object to a new key on RGW1: the copy has the source's ETag and data, its
        own D4N version, and is cached dirty on both RGWs; the source is unchanged."""
        src_etag = self.etag()
        dst = u.unique_name('copy')
        self.assertEqual(self.copy(self.c1, self.key, dst), src_etag)

        dst_version = self.latest_version(dst)
        self.assertNotEqual(dst_version, self.version)
        self.assert_copy_dirty(dst, dst_version, self.data)
        self.assertEqual(self.etag(dst), src_etag)

        # Source unchanged.
        self.assertEqual(self.latest_version(), self.version)
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assert_get(self.c2, self.rgw2, self.data)

    def test_copy_over_dirty_object(self):
        """Copy onto a key that holds a dirty object: the copy becomes the current object, cached
        dirty on both RGWs, and the replaced dirty version is invalidated so that its write-back
        cannot overwrite the copy."""
        dst = u.unique_name('obj')
        dx = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=dst, Body=dx)
        old_version = self.latest_version(dst)

        self.copy(self.c1, self.key, dst)

        new_version = self.latest_version(dst)
        self.assertNotEqual(new_version, old_version)
        self.assert_copy_dirty(dst, new_version, self.data)
        self.assert_invalidated(old_version, dx, key=dst)

    def test_copy_on_rgw2_of_source_only_on_rgw1(self):
        """The source is written on RGW1 with replication off (rgw_d4n_remote_put=false on RGW1), so
        it is cached only on RGW1. A copy on RGW2 reads the source from RGW1 (and caches it clean, as
        a holder) and writes the copy on RGW2, which replicates it to RGW1: the copy is cached dirty
        on both RGWs."""
        src = u.unique_name('obj')
        ds = u.random_bytes(cfg.OBJECT_SIZE)
        self.rgw1.config_set('rgw_d4n_remote_put', 'false')
        try:
            self.assertEqual(self.rgw1.config_get('rgw_d4n_remote_put'), 'false')
            self.c1.put_object(Bucket=self.bucket, Key=src, Body=ds)
        finally:
            self.rgw1.config_set('rgw_d4n_remote_put', 'true')
        src_version = self.latest_version(src)
        self.assert_cached_on(self.rgw1, src_version, ds, dirty=True, key=src)
        self.assert_not_cached_on(self.rgw2, src_version, key=src)
        self.assert_dirty_in_directory(src_version, ds, (self.rgw1,), key=src)

        dst = u.unique_name('copy')
        self.copy(self.c2, src, dst)

        dst_version = self.latest_version(dst)
        self.assert_copy_dirty(dst, dst_version, ds)
        self.assertEqual(self.etag(dst), self.etag(src))

        # The source: still dirty on RGW1, now also cached on RGW2 (clean, read for the copy), and
        # still the current, dirty object with both RGWs as holders.
        self.assertEqual(self.latest_version(src), src_version)
        self.assert_cached_on(self.rgw1, src_version, ds, dirty=True, key=src)
        self.assert_cached_on(self.rgw2, src_version, ds, dirty=False, key=src)
        self.assert_dirty_in_directory(src_version, ds, (self.rgw1, self.rgw2), key=src)

    def test_copy_is_independent_of_source(self):
        """Deleting the source does not affect the copy: its blocks and directory entries are
        untouched and GETs return its data. Deleting the copy then makes it NoSuchKey on both RGWs."""
        dst = u.unique_name('copy')
        self.copy(self.c1, self.key, dst)
        dst_version = self.latest_version(dst)
        self.assert_copy_dirty(dst, dst_version, self.data)

        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(self.version, self.data)
        self.assertEqual(self.latest_version(dst), dst_version)
        self.assert_copy_dirty(dst, dst_version, self.data)

        self.c1.delete_object(Bucket=self.bucket, Key=dst)
        self.assert_deleted(dst_version, self.data, key=dst)

    def test_delete_clean_object(self):
        """Delete of a clean object (a multipart upload: in the backend, clean HEAD cached), after a
        GET on RGW2 cached its blocks there (clean). D4N passes the delete to the backend: GETs on both
        RGWs return NoSuchKey, the object is gone from the backend, and its HEAD entries are removed
        from the directory. Nothing of it is left dirty, so the cleaning cycle never writes it back."""
        x = u.unique_name('mp')
        d0 = u.random_bytes(cfg.OBJECT_SIZE)
        self.multipart_put(x, d0)
        v0 = self.latest_version(x, dirty=False)
        self.assert_in_backend(x, True)

        self.assert_get(self.c2, self.rgw2, d0, key=x)
        # RGW2 cached the blocks it read from the backend, clean; the entries are clean, RGW2 a holder.
        self.assert_cached_on(self.rgw2, v0, d0, dirty=False, key=x)
        self.assert_not_cached_on(self.rgw1, v0, key=x)
        blocks = u.data_block_entries(self.entries(x), x, v0)
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(d0), blocks)
        for block in blocks:
            self.assertFalse(block.flag('dirty'), block)
            self.assertIn(self.address[self.rgw2.label], block.hosts, block)

        self.c1.delete_object(Bucket=self.bucket, Key=x)

        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                u.assert_no_such_key(self, client, self.bucket, x)
        self.assert_in_backend(x, False)
        entries = self.entries(x)
        heads = [e for e in entries if e.is_head]
        self.assertEqual(heads, [], f'HEAD entries of a deleted clean object: {heads}')
        # Whatever is left of its blocks (stale, keyed by the deleted version) is clean: evictable,
        # never written back.
        for e in entries:
            self.assertFalse(e.flag('dirty'), e)
        for rgw in (self.rgw1, self.rgw2):
            for offset, length, path in u.cached_blocks(self.cache_dir[rgw.label], self.bucket_id, x, v0):
                xattrs = u.cached_xattrs(path)
                self.assertNotEqual(xattrs.get('user.rgw.dirty'), '1', f'{rgw}: {path} xattrs {xattrs}')

    def test_delete_on_rgw2(self):
        """A DELETE on RGW2 of an object written on RGW1: GETs on both RGWs return NoSuchKey, and the
        deleted version is invalidated in the directory and on both RGWs (on RGW1 through the remote
        delete from RGW2)."""
        self.c2.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(self.version, self.data)

    def test_copy_of_deleted_object(self):
        """Copy of a deleted object: its HEAD is invalid in the directory, which in a non-versioned
        bucket means deleted (not "look in the backend"). The copy fails with 404 NoSuchKey and writes
        nothing for the destination; the source stays deleted."""
        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        dst = u.unique_name('copy')
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(copy_on=rgw.label):
                with self.assertRaises(botocore.exceptions.ClientError) as ctx:
                    self.copy(client, self.key, dst)
                err = ctx.exception.response
                self.assertEqual(err['ResponseMetadata']['HTTPStatusCode'], 404, err)
                self.assertEqual(err['Error']['Code'], 'NoSuchKey', err)
        self.assert_nothing_written(dst)
        self.assert_deleted(self.version, self.data)

    def test_multipart_upload_over_dirty_object(self):
        """A multipart upload (written by the backend) over a dirty object: the upload becomes the
        current object, clean, in the backend; the replaced dirty version's HEAD is tombstoned so that
        its write-back cannot overwrite the upload in the backend. Its blocks are not marked invalid
        (D4NFilterMultipartUpload::complete() does not invalidate them): the cleaning cycle finds the
        version invalid in the directory and drops it. A bucket listing shows the upload (not the
        replaced version), and after a DELETE the object is no longer listed."""
        v1 = self.version
        dm = u.random_bytes(5 * 1024 * 1024)  # a size other than the replaced version's
        self.multipart_put(self.key, dm)
        etag = self.etag()

        vm = self.latest_version(dirty=False)
        self.assertNotEqual(vm, v1)
        self.assert_in_backend(self.key, True)
        self.assert_invalidated(v1, self.data, marked_on=())
        for rgw in (self.rgw1, self.rgw2):
            self.assert_not_cached_on(rgw, vm)

        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, dm)
        # Each GET cached the upload's blocks on the reading RGW, clean; nothing of it is dirty.
        for rgw in (self.rgw1, self.rgw2):
            self.assert_cached_on(rgw, vm, dm, dirty=False)
        blocks = u.data_block_entries(self.entries(), self.key, vm)
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(dm), blocks)
        for block in blocks:
            self.assertFalse(block.flag('dirty'), block)
        self.assertEqual(self.latest_version(dirty=False), vm)
        self.assert_invalidated(v1, self.data, marked_on=())

        # The listing shows the upload, not the replaced dirty version (D4N's bucket directory entry
        # of the dirty PUT must be updated or removed by the upload).
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(list_on=rgw.label):
                self.assertEqual(self.listed(client, self.key), (etag, len(dm)))

        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(after_delete_on=rgw.label):
                u.assert_no_such_key(self, client, self.bucket, self.key)
                self.assertIsNone(self.listed(client, self.key), 'deleted object still listed')
        self.assert_in_backend(self.key, False)
        heads = [e for e in self.entries() if e.is_head and e.get('version') == vm]
        self.assertEqual(heads, [], f'HEAD entries of the deleted upload: {heads}')

    def tag_attr(self, key=None):
        """{HEAD entry key: the x-amz-tagging attribute (encoded) or None} for the plain-name,
        "_:null_" and version HEADs of the current version."""
        key = key or self.key
        entries = self.entries(key)
        version = self.plain_head(entries, self.bucket_id, key).get('version')
        heads = [self.plain_head(entries, self.bucket_id, key), self.null_head(entries, key)]
        heads += u.version_heads(entries, key, version)
        return {h.key: h.get('attr_user.rgw.x-amz-tagging') for h in heads}

    def test_object_tagging_on_dirty_object(self):
        """PutObjectTagging on RGW1 and DeleteObjectTagging on RGW2 of a dirty object: both RGWs return
        the current tags; the tags are stored as an attribute of every HEAD in the directory
        (plain-name, "_:null_" and version HEAD) and removed from all of them; the object stays dirty,
        cached on both RGWs, and is not written to the backend."""
        tags = [{'Key': 'k1', 'Value': 'v1'}, {'Key': 'k2', 'Value': 'v2'}]
        self.c1.put_object_tagging(Bucket=self.bucket, Key=self.key, Tagging={'TagSet': tags})
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_tagging_on=rgw.label):
                got = client.get_object_tagging(Bucket=self.bucket, Key=self.key)['TagSet']
                self.assertEqual(sorted(got, key=lambda t: t['Key']), tags)
        for head, attr in self.tag_attr().items():
            self.assertIsNotNone(attr, f'no tagging attribute on {head}')
            for t in tags:
                self.assertIn(t['Key'], attr, head)
                self.assertIn(t['Value'], attr, head)
        self.assertEqual(self.latest_version(), self.version)
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assert_in_backend(self.key, False)

        self.c2.delete_object_tagging(Bucket=self.bucket, Key=self.key)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_tagging_after_delete_on=rgw.label):
                self.assertEqual(client.get_object_tagging(Bucket=self.bucket, Key=self.key)['TagSet'], [])
        for head, attr in self.tag_attr().items():
            self.assertIsNone(attr, f'tagging attribute left on {head}')
        self.assertEqual(self.latest_version(), self.version)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assert_in_backend(self.key, False)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, self.data)

    def test_remote_range_get(self):
        """An object written on RGW2 with replication off is cached only on RGW2. Range GETs on RGW1
        read from RGW2 and cache only the blocks they touch, whole and clean; RGW1 becomes a holder
        of those blocks only; the directory entries stay dirty."""
        x = u.unique_name('obj')
        dx = u.random_bytes(cfg.OBJECT_SIZE)
        self.rgw2.config_set('rgw_d4n_remote_put', 'false')
        try:
            self.assertEqual(self.rgw2.config_get('rgw_d4n_remote_put'), 'false')
            self.c2.put_object(Bucket=self.bucket, Key=x, Body=dx)
        finally:
            self.rgw2.config_set('rgw_d4n_remote_put', 'true')
        vx = self.latest_version(x)
        self.assert_cached_on(self.rgw2, vx, dx, dirty=True, key=x)
        self.assert_not_cached_on(self.rgw1, vx, key=x)
        self.assert_dirty_in_directory(vx, dx, (self.rgw2,), key=x)

        rgw1_addr = self.address[self.rgw1.label]
        size, bs = cfg.OBJECT_SIZE, cfg.BLOCK_SIZE
        # (range, blocks of RGW1 cached after it)
        steps = [((0, 1023), {0}),
                 ((bs - 512, bs + 511), {0, bs})]
        for (first, last), expect in steps:
            with self.subTest(range=(first, last)):
                body, resp = u.get_object(self.c1, self.bucket, x, Range=f'bytes={first}-{last}')
                self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 206)
                self.assertEqual(resp.get('ContentRange'), f'bytes {first}-{last}/{size}')
                self.assertEqual(body, dx[first:last + 1])

                cache_dir = self.cache_dir[self.rgw1.label]
                u.wait_until(lambda: {o for o, _, _ in u.cached_blocks(cache_dir, self.bucket_id, x, vx)} == expect)
                cached = u.cached_blocks(cache_dir, self.bucket_id, x, vx)
                self.assertEqual({o for o, _, _ in cached}, expect, cached)
                for offset, length, path in cached:
                    with open(path, 'rb') as f:
                        self.assertEqual(f.read(), dx[offset:offset + length], path)
                    xattrs = u.cached_xattrs(path)
                    self.assertNotEqual(xattrs.get('user.rgw.dirty'), '1', f'{path} xattrs {xattrs}')
                for block in u.data_block_entries(self.entries(x), x, vx):
                    self.assertTrue(block.flag('dirty'), block)
                    self.assertEqual(rgw1_addr in block.hosts, int(block.get('blockID')) in expect, block)
        self.assert_cached_on(self.rgw2, vx, dx, dirty=True, key=x)
        self.assertEqual(self.latest_version(x), vx)

    def test_delete_after_overwrite_on_rgw2(self):
        """v1 written on RGW1, overwritten on RGW2 with v2, then deleted on RGW1: GETs on both RGWs
        return NoSuchKey; v2 is the deleted version (HEADs invalid) and both v1 and v2 are invalidated
        in the directory and on both RGWs; nothing is in the backend."""
        v1 = self.version
        d2 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c2.put_object(Bucket=self.bucket, Key=self.key, Body=d2)
        v2 = self.latest_version()
        self.assertNotEqual(v2, v1)

        self.c1.delete_object(Bucket=self.bucket, Key=self.key)

        self.assert_deleted(v2, d2)
        self.assert_invalidated(v1, self.data)

    def test_conditional_put(self):
        """Conditional PUTs, checked against the current (dirty) object in the directory: If-None-Match: *
        on an existing object and If-Match with a wrong ETag fail with 412 and change nothing that is
        committed (the data blocks they wrote have no HEAD, so cleaning removes them); If-Match with the
        current ETag replaces the object; a deleted object does not exist for If-None-Match: *, and
        If-Match on a missing object is 404."""
        etag = self.etag()
        for name, cond in (('If-None-Match: *', {'IfNoneMatch': '*'}),
                           ('If-Match wrong', {'IfMatch': '"0123456789abcdef0123456789abcdef"'})):
            with self.subTest(failed=name):
                with self.assertRaises(botocore.exceptions.ClientError) as ctx:
                    self.c1.put_object(Bucket=self.bucket, Key=self.key, Body=u.random_bytes(cfg.OBJECT_SIZE), **cond)
                err = ctx.exception.response
                self.assertEqual(err['ResponseMetadata']['HTTPStatusCode'], 412, err)
                self.assertEqual(err['Error']['Code'], 'PreconditionFailed', err)
        # Nothing committed: the object is unchanged and every HEAD names the original version.
        self.assertEqual(self.latest_version(), self.version)
        heads = [e for e in self.entries() if e.is_head]
        self.assertTrue(heads)
        for head in heads:
            self.assertEqual(head.get('version'), self.version, f'HEAD of a failed conditional PUT: {head}')
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, self.data)

        # If-Match with the current ETag: the object is replaced.
        d2 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=self.key, Body=d2, IfMatch=etag)
        v2 = self.latest_version()
        self.assertNotEqual(v2, self.version)
        self.assert_cached_on(self.rgw1, v2, d2, dirty=True)
        self.assert_cached_on(self.rgw2, v2, d2, dirty=True)
        self.assert_dirty_in_directory(v2, d2, (self.rgw1, self.rgw2))
        self.assert_invalidated(self.version, self.data)
        self.assert_get(self.c2, self.rgw2, d2)

        # A deleted object does not exist: If-None-Match: * succeeds.
        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(v2, d2)
        d3 = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=self.key, Body=d3, IfNoneMatch='*')
        v3 = self.latest_version()
        self.assert_cached_on(self.rgw1, v3, d3, dirty=True)
        self.assert_cached_on(self.rgw2, v3, d3, dirty=True)
        self.assert_dirty_in_directory(v3, d3, (self.rgw1, self.rgw2))
        self.assert_get(self.c2, self.rgw2, d3)

        # If-Match on an object that does not exist: 404, and no HEAD is written.
        missing = u.unique_name('obj')
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.c1.put_object(Bucket=self.bucket, Key=missing, Body=b'data', IfMatch=etag)
        self.assertEqual(ctx.exception.response['ResponseMetadata']['HTTPStatusCode'], 404, ctx.exception.response)
        heads = [e for e in self.entries(missing) if e.is_head]
        self.assertEqual(heads, [], f'HEAD entries of a failed conditional PUT: {heads}')
        self.assert_in_backend(missing, False)

    def test_copy_by_backend_over_dirty_object(self):
        """A copy made by the backend (multipart source) over a dirty object: the copy is the current
        object, clean, in the backend, with the source's ETag; the replaced dirty version is tombstoned
        (its blocks are marked invalid on RGW1, which made the copy); a bucket listing shows the copy,
        and after a DELETE the object is no longer listed."""
        src = u.unique_name('mp')
        ds = u.random_bytes(5 * 1024 * 1024)  # a size other than the replaced version's
        self.multipart_put(src, ds)
        src_etag = self.etag(src)

        self.assertEqual(self.copy(self.c1, src, self.key), src_etag)

        vc = self.latest_version(dirty=False)
        self.assertNotEqual(vc, self.version)
        self.assert_in_backend(self.key, True)
        self.assert_invalidated(self.version, self.data, marked_on=(self.rgw1,))
        for rgw in (self.rgw1, self.rgw2):
            self.assert_not_cached_on(rgw, vc)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(on=rgw.label):
                self.assertEqual(self.listed(client, self.key), (src_etag, len(ds)))
                self.assert_get(client, rgw, ds)

        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(after_delete_on=rgw.label):
                u.assert_no_such_key(self, client, self.bucket, self.key)
                self.assertIsNone(self.listed(client, self.key), 'deleted object still listed')
        self.assert_in_backend(self.key, False)

    def test_small_and_zero_byte_objects(self):
        """An object smaller than a block is one dirty data block, cached on both RGWs; a 0-byte object
        has only its HEADs (no data block entry, no cache file). Both are read back on RGW2."""
        small = u.unique_name('small')
        ds = u.random_bytes(1000)
        self.c1.put_object(Bucket=self.bucket, Key=small, Body=ds)
        vs = self.latest_version(small)
        for rgw in (self.rgw1, self.rgw2):
            self.assert_cached_on(rgw, vs, ds, dirty=True, key=small)
            self.assertEqual([(o, l) for o, l, _ in u.cached_blocks(self.cache_dir[rgw.label], self.bucket_id, small, vs)],
                             [(0, len(ds))], rgw)
        self.assert_dirty_in_directory(vs, ds, (self.rgw1, self.rgw2), key=small)
        self.assertEqual(len(u.data_block_entries(self.entries(small), small, vs)), 1)
        self.assert_get(self.c2, self.rgw2, ds, key=small)

        zero = u.unique_name('zero')
        self.c1.put_object(Bucket=self.bucket, Key=zero, Body=b'')
        vz = self.latest_version(zero)
        entries = self.entries(zero)
        heads = u.version_heads(entries, zero, vz)
        self.assertTrue(heads, f'no version HEAD for {zero}')
        for head in heads:
            self.assertTrue(head.flag('dirty'), head)
        self.assertEqual([e for e in entries if not e.is_head], [], 'data block entries of a 0-byte object')
        for rgw in (self.rgw1, self.rgw2):
            obj_dir = os.path.join(self.cache_dir[rgw.label], u.url_encode(self.bucket_id), u.url_encode(zero))
            files = os.listdir(obj_dir) if os.path.isdir(obj_dir) else []
            self.assertEqual(files, [], f'{rgw}: cache files of a 0-byte object: {files}')
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, b'', key=zero)
        self.assert_in_backend(zero, False)

    def test_name_starting_with_underscore(self):
        """An object whose name starts with "_": its plain-name HEAD is stored under "_" + name (so that
        it cannot collide with the "_:<version>_<name>" HEADs), its data blocks under the name as is. It
        is written, replicated, read on RGW2 and deleted like any other object."""
        key = '_' + u.unique_name('obj')
        data = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket, Key=key, Body=data)
        version = self.latest_version(key)
        self.assertIsNone(u.plain_head(self.entries(key), self.bucket_id, key),
                          'a plain-name HEAD stored under the unescaped name')
        self.assert_cached_on(self.rgw1, version, data, dirty=True, key=key)
        self.assert_cached_on(self.rgw2, version, data, dirty=True, key=key)
        self.assert_dirty_in_directory(version, data, (self.rgw1, self.rgw2), key=key)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(get_on=rgw.label):
                self.assert_get(client, rgw, data, key=key)

        self.c1.delete_object(Bucket=self.bucket, Key=key)
        self.assert_deleted(version, data, key=key)

    def test_user_metadata_and_head_object(self):
        """User metadata and the content type of a dirty object: stored as attributes of every HEAD in
        the directory, and returned by HeadObject and GetObject on both RGWs with the object's ETag,
        size and mtime. HeadObject reads no data and changes nothing in the cache or the directory."""
        key = u.unique_name('meta')
        data = u.random_bytes(cfg.OBJECT_SIZE)
        meta = {'color': 'blue', 'shape': 'round'}
        etag = self.c1.put_object(Bucket=self.bucket, Key=key, Body=data, Metadata=meta,
                                  ContentType='text/plain')['ETag']
        version = self.latest_version(key)
        entries = self.entries(key)
        heads = [self.plain_head(entries, self.bucket_id, key), self.null_head(entries, key)]
        heads += u.version_heads(entries, key, version)
        for head in heads:
            for k, v in meta.items():
                self.assertEqual(head.get(f'attr_user.rgw.x-amz-meta-{k}', '').rstrip('\0'), v, head)
            self.assertEqual(head.get('attr_user.rgw.content_type', '').rstrip('\0'), 'text/plain', head)

        seen = {}
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(on=rgw.label):
                h = client.head_object(Bucket=self.bucket, Key=key)
                self.assertEqual(h['ETag'], etag)
                self.assertEqual(h['ContentLength'], len(data))
                self.assertEqual(h['Metadata'], meta)
                self.assertEqual(h['ContentType'], 'text/plain')
                seen[rgw.label] = h['LastModified']
                body, g = u.get_object(client, self.bucket, key)
                self.assertEqual(g['Metadata'], meta)
                self.assertEqual(g['ContentType'], 'text/plain')
                self.assertEqual(u.md5_hex(body), u.md5_hex(data))
        self.assertEqual(seen[self.rgw1.label], seen[self.rgw2.label], 'mtime differs between RGW1 and RGW2')

        self.assertEqual(self.latest_version(key), version)
        self.assert_cached_on(self.rgw1, version, data, dirty=True, key=key)
        self.assert_cached_on(self.rgw2, version, data, dirty=True, key=key)
        self.assert_dirty_in_directory(version, data, (self.rgw1, self.rgw2), key=key)
        self.assert_in_backend(key, False)

    def test_delete_missing_and_twice(self):
        """A DELETE of a key that never existed succeeds (204) and creates nothing in the directory, the
        caches or the backend. A second DELETE of a deleted object succeeds too and changes nothing: the
        object stays deleted, with the same invalidated version."""
        missing = u.unique_name('obj')
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(delete_missing_on=rgw.label):
                resp = client.delete_object(Bucket=self.bucket, Key=missing)
                self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 204, resp)
        self.assert_nothing_written(missing)

        self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assert_deleted(self.version, self.data)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(delete_again_on=rgw.label):
                resp = client.delete_object(Bucket=self.bucket, Key=self.key)
                self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 204, resp)
        self.assert_deleted(self.version, self.data)

    def test_copy_onto_itself(self):
        """A copy of an object onto itself without MetadataDirective=REPLACE is rejected (400
        InvalidRequest) and changes nothing. With REPLACE it creates a new version with the same data and
        the new metadata, cached dirty on both RGWs, and invalidates the replaced version."""
        meta = {'color': 'blue'}
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.copy(self.c1, self.key, self.key)
        err = ctx.exception.response
        self.assertEqual(err['ResponseMetadata']['HTTPStatusCode'], 400, err)
        self.assertEqual(err['Error']['Code'], 'InvalidRequest', err)
        self.assertEqual(self.latest_version(), self.version)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)

        self.copy(self.c1, self.key, self.key, MetadataDirective='REPLACE', Metadata=meta)
        v2 = self.latest_version()
        self.assertNotEqual(v2, self.version)
        self.assert_copy_dirty(self.key, v2, self.data)
        self.assert_invalidated(self.version, self.data)
        entries = self.entries()
        for head in [self.plain_head(entries, self.bucket_id, self.key), self.null_head(entries, self.key)] + \
                u.version_heads(entries, self.key, v2):
            self.assertEqual(head.get('attr_user.rgw.x-amz-meta-color', '').rstrip('\0'), 'blue', head)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(head_on=rgw.label):
                self.assertEqual(client.head_object(Bucket=self.bucket, Key=self.key)['Metadata'], meta)

    def test_copy_into_versioned_bucket(self):
        """A copy of a dirty object into a versioned bucket: the copy is a new version there (a real
        VersionId; RGW sends no version header on CopyObject, so it is read with HeadObject), cached
        dirty on both RGWs, with a dirty version HEAD and plain-name HEAD; the source is unchanged."""
        dst = u.unique_name('copy')
        resp = self.c1.copy_object(Bucket=self.bucket_v, Key=dst,
                                   CopySource={'Bucket': self.bucket, 'Key': self.key})
        self.assertEqual(resp['CopyObjectResult']['ETag'], self.etag())
        vid = self.c1.head_object(Bucket=self.bucket_v, Key=dst).get('VersionId')
        self.assertTrue(vid and vid != 'null', vid)

        entries = u.object_dir_entries(self.db, self.bucket_v_id, dst)
        plain = self.plain_head(entries, self.bucket_v_id, dst)
        self.assertIsNotNone(plain, f'no plain-name HEAD for {dst}')
        self.assertEqual(plain.get('version'), vid, plain)
        self.assertTrue(plain.flag('dirty'), plain)
        heads = u.version_heads(entries, dst, vid)
        self.assertTrue(heads, f'no version HEAD for {dst} {vid}')
        for head in heads:
            self.assertTrue(head.flag('dirty'), head)
            self.assertFalse(head.flag('invalid'), head)
        blocks = u.data_block_entries(entries, dst, vid)
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(self.data), blocks)
        both = {self.address[self.rgw1.label], self.address[self.rgw2.label]}
        for block in blocks:
            self.assertTrue(block.flag('dirty'), block)
            self.assertTrue(both <= block.hosts, f'data block holders: {block}')
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(on=rgw.label):
                u.assert_cached(self, rgw, self.cache_dir[rgw.label], self.bucket_v_id, dst, vid, self.data)
                body, g = u.get_object(client, self.bucket_v, dst, VersionId=vid)
                self.assertEqual(g.get('VersionId'), vid)
                self.assertEqual(u.md5_hex(body), u.md5_hex(self.data))

        # Source unchanged.
        self.assertEqual(self.latest_version(), self.version)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))

    def test_copy_on_rgw2_of_replicated_source(self):
        """A copy on RGW2 of a source written on RGW1 and replicated to RGW2: RGW2 reads its own replica
        (the source stays dirty on both RGWs, nothing is cached clean) and writes the copy, which it
        replicates to RGW1: the copy is cached dirty on both RGWs with the source's ETag."""
        dst = u.unique_name('copy')
        hits = self.rgw2.perf_counter('d4n_cache_hits') if cfg.CHECK_CACHE_HITS else 0
        self.assertEqual(self.copy(self.c2, self.key, dst), self.etag())
        if cfg.CHECK_CACHE_HITS:
            self.assertGreater(self.rgw2.perf_counter('d4n_cache_hits'), hits,
                               'RGW2 did not read the source from its cache')
        dst_version = self.latest_version(dst)
        self.assert_copy_dirty(dst, dst_version, self.data)

        # Source unchanged, still dirty on both RGWs.
        self.assertEqual(self.latest_version(), self.version)
        self.assert_cached_on(self.rgw1, self.version, self.data, dirty=True)
        self.assert_cached_on(self.rgw2, self.version, self.data, dirty=True)
        self.assert_dirty_in_directory(self.version, self.data, (self.rgw1, self.rgw2))


if __name__ == '__main__':
    unittest.main()
