"""
D4N distributed end-to-end tests: versioned bucket, write cache on, remote put on.

Runs against a live setup of two RGWs (RGW1, RGW2) with the D4N filter, sharing one
FoundationDB block directory, over RADOS. RGW1 replicates to RGW2
(rgw_d4n_remote_cache_address), synchronously (rgw_d4n_async_remote_put=false).

The tests cover PUT, GET, DELETE and COPY of versioned objects:
  - PUT: each version is cached on RGW1 and replicated to RGW2;
  - GET: simple, versioned and range GETs on both RGWs, served from the local caches, and the
    fallback to the backend when the latest dirty version was deleted;
  - DELETE: simple DELETE (delete marker, replicated to RGW2), version specific DELETE of versions
    and of the delete marker (the next version is promoted), deletes over versions in the backend;
  - COPY: copy of a dirty object (write-back copy: cached, replicated and dirty), of a specific
    version, onto the same key, on RGW2, across buckets, into a non-versioned bucket, of a
    multipart object (copied by the backend), conditional copies, the metadata directive and
    copies of a deleted object.

Each test writes its own object as three 7 MiB versions v1, v2, v3 on RGW1, and checks the S3
responses on both RGWs, the block files (and their cache attributes) in each RGW's cache directory
and the block directory entries in FoundationDB.

rgw_d4n_cache_cleaning_interval must be long (see D4N_MIN_CLEANING_INTERVAL in
d4n_test_config.py): every version must still be dirty when a test ends. Write-back timing is
covered by separate tests.

Run from the build directory, with the cluster and both RGWs up:
  python3 ../src/test/rgw/test_rgw_d4n_versioned.py -v
See d4n_test_config.py for the environment variables and the S3 user prerequisite.
"""

import os
import unittest

import botocore

import d4n_test_config as cfg
import d4n_test_utils as u


class D4NVersionedWriteCacheTest(unittest.TestCase):

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

        cls.bucket = u.unique_name('d4n-versioned')
        u.create_versioned_bucket(cls.c1, cls.bucket)
        cls.bucket_id = u.bucket_id(cls.bucket)
        # Copy object destinations: a second versioned bucket and a non-versioned one.
        cls.bucket_b = u.unique_name('d4n-versioned-b')
        u.create_versioned_bucket(cls.c1, cls.bucket_b)
        cls.bucket_b_id = u.bucket_id(cls.bucket_b)
        cls.bucket_nv = u.unique_name('d4n-plain')
        cls.c1.create_bucket(Bucket=cls.bucket_nv)
        cls.bucket_nv_id = u.bucket_id(cls.bucket_nv)
        cls.db = u.open_fdb()

    @classmethod
    def tearDownClass(cls):
        for bucket in (cls.bucket, cls.bucket_b, cls.bucket_nv):
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
            remote = cfg.RGW1.config_get('rgw_d4n_remote_cache_address')
            if remote != cls.address[cls.rgw2.label]:
                problems.append(f'{cls.rgw1}: rgw_d4n_remote_cache_address={remote}, expected '
                                f'RGW2 ({cls.address[cls.rgw2.label]})')
        if problems:
            raise unittest.SkipTest('D4N setup does not match this suite:\n  ' + '\n  '.join(problems))

    # --- per test ------------------------------------------------------------------------

    def setUp(self):
        self.key = u.unique_name('obj')
        payloads = [u.random_bytes(cfg.OBJECT_SIZE) for _ in range(3)]
        self.versions = u.put_versions(self.c1, self.bucket, self.key, payloads)
        (self.v1, self.d1), (self.v2, self.d2), (self.v3, self.d3) = self.versions

    # --- helpers -------------------------------------------------------------------------

    def entries(self):
        return u.object_dir_entries(self.db, self.bucket_id, self.key)

    def assert_cached_on(self, rgw, version, data):
        u.assert_cached(self, rgw, self.cache_dir[rgw.label], self.bucket_id, self.key, version, data)

    def assert_get(self, client, rgw, version_id, data, **kwargs):
        body, resp = u.get_object(client, self.bucket, self.key, **kwargs)
        self.assertEqual(resp.get('VersionId'), version_id, f'{rgw}: VersionId of GET {kwargs}')
        self.assertEqual(len(body), len(data), f'{rgw}: size of GET {kwargs}')
        self.assertEqual(u.md5_hex(body), u.md5_hex(data), f'{rgw}: content of GET {kwargs}')

    def assert_get_key(self, client, rgw, key, version_id, data, **kwargs):
        body, resp = u.get_object(client, self.bucket, key, **kwargs)
        self.assertEqual(resp.get('VersionId'), version_id, f'{rgw}: VersionId of GET {key} {kwargs}')
        self.assertEqual(u.md5_hex(body), u.md5_hex(data), f'{rgw}: content of GET {key} {kwargs}')

    def assert_latest_in_directory(self, version_id):
        head = u.plain_head(self.entries(), self.bucket_id, self.key)
        self.assertIsNotNone(head, f'no plain-name HEAD for {self.key}')
        self.assertEqual(head.get('version'), version_id, f'plain-name HEAD: {head}')
        self.assertFalse(head.flag('deleteMarker'), f'plain-name HEAD: {head}')
        self.assertFalse(head.flag('invalid'), f'plain-name HEAD: {head}')

    def assert_still_dirty(self, version_ids):
        """Precondition of the suite: nothing was written back during the test."""
        entries = self.entries()
        for vid in version_ids:
            heads = u.version_heads(entries, self.key, vid)
            self.assertTrue(heads, f'no version HEAD for {vid}')
            for head in heads:
                self.assertTrue(head.flag('dirty'),
                                f'version {vid} is no longer dirty (written back during the test?); '
                                f'raise rgw_d4n_cache_cleaning_interval: {head}')

    def assert_tombstoned(self, version_id):
        """A version deleted by version id while dirty: its version HEAD stays in the
        directory, marked invalid, until the cleaning cycle removes it."""
        heads = u.version_heads(self.entries(), self.key, version_id)
        self.assertTrue(heads, f'no version HEAD for deleted version {version_id}')
        for head in heads:
            self.assertTrue(head.flag('invalid'), f'deleted version {version_id} not invalid: {head}')

    def cache_hits_rgw2(self):
        return self.rgw2.perf_counter('d4n_cache_hits') if cfg.CHECK_CACHE_HITS else 0

    def assert_cache_hit_rgw2(self, before):
        if cfg.CHECK_CACHE_HITS:
            self.assertGreater(self.rgw2.perf_counter('d4n_cache_hits'), before,
                               'RGW2 did not serve the GET from its cache')

    # --- tests ---------------------------------------------------------------------------

    def test_put_replicates_versions_to_rgw2(self):
        """Each version written on RGW1 is cached on RGW1, replicated to RGW2, and recorded
        dirty in the directory with both RGWs as data block holders; v3 is the latest.
        The HEAD's hostsList is not checked: HEADs live only in the directory."""
        both = {self.address[self.rgw1.label], self.address[self.rgw2.label]}
        entries = self.entries()
        for vid, data in self.versions:
            with self.subTest(version=vid):
                self.assert_cached_on(self.rgw1, vid, data)
                self.assert_cached_on(self.rgw2, vid, data)

                heads = u.version_heads(entries, self.key, vid)
                self.assertTrue(heads, f'no version HEAD for {vid}')
                for head in heads:
                    self.assertTrue(head.flag('dirty'), head)
                    self.assertFalse(head.flag('invalid'), head)
                    self.assertFalse(head.flag('deleteMarker'), head)

                blocks = u.data_block_entries(entries, self.key, vid)
                self.assertTrue(blocks, f'no data block entries for {vid}')
                self.assertEqual(sum(int(b.get('size')) for b in blocks), len(data), blocks)
                for block in blocks:
                    self.assertTrue(block.flag('dirty'), block)
                    self.assertTrue(both <= block.hosts, f'data block holders: {block}')

        self.assert_latest_in_directory(self.v3)
        self.assert_still_dirty([self.v1, self.v2, self.v3])

    def test_get_latest_from_rgw2(self):
        """A simple GET on RGW2 returns v3, served from RGW2's local replica."""
        self.assert_cached_on(self.rgw2, self.v3, self.d3)
        hits = self.cache_hits_rgw2()
        self.assert_get(self.c2, self.rgw2, self.v3, self.d3)
        self.assert_cache_hit_rgw2(hits)
        self.assert_cached_on(self.rgw2, self.v3, self.d3)
        self.assert_still_dirty([self.v1, self.v2, self.v3])

    def test_get_each_version_from_rgw2(self):
        """A versioned GET on RGW2 returns each version, served from RGW2's local replica."""
        for vid, data in self.versions:
            with self.subTest(version=vid):
                self.assert_cached_on(self.rgw2, vid, data)
                hits = self.cache_hits_rgw2()
                self.assert_get(self.c2, self.rgw2, vid, data, VersionId=vid)
                self.assert_cache_hit_rgw2(hits)
                self.assert_cached_on(self.rgw2, vid, data)
        self.assert_still_dirty([self.v1, self.v2, self.v3])

    def test_range_get_rgw1(self):
        """Range GETs on RGW1, of the latest version and of v1: inside the first block, across
        the block boundary, and the end of the object."""
        size = cfg.OBJECT_SIZE
        ranges = [
            (0, 1023),
            (cfg.BLOCK_SIZE - 512, cfg.BLOCK_SIZE + 511),
            (size - 1000, size - 1),
        ]
        for label, vid, data, extra in (('latest', self.v3, self.d3, {}),
                                        ('v1', self.v1, self.d1, {'VersionId': self.v1})):
            for first, last in ranges:
                with self.subTest(version=label, range=(first, last)):
                    body, resp = u.get_object(self.c1, self.bucket, self.key,
                                              Range=f'bytes={first}-{last}', **extra)
                    self.assertEqual(resp['ResponseMetadata']['HTTPStatusCode'], 206)
                    self.assertEqual(resp.get('VersionId'), vid)
                    self.assertEqual(resp.get('ContentRange'), f'bytes {first}-{last}/{size}')
                    self.assertEqual(body, data[first:last + 1])
        self.assert_still_dirty([self.v1, self.v2, self.v3])

    def test_delete_marker_then_versions(self):
        """Simple DELETE, then delete v3, v1 and the delete marker by version id: v2 becomes
        the latest version, on both RGWs, in the listing and in the directory."""
        with self.subTest(step='simple DELETE on RGW1'):
            resp = self.c1.delete_object(Bucket=self.bucket, Key=self.key)
            self.assertTrue(resp.get('DeleteMarker'), resp)
            dm = resp.get('VersionId')
            self.assertTrue(dm, resp)
            for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
                u.assert_no_such_key(self, client, self.bucket, self.key)
            versions, markers = u.list_versions(self.c1, self.bucket, self.key)
            self.assertEqual([m['VersionId'] for m in markers], [dm])
            self.assertTrue(markers[0]['IsLatest'], markers)
            self.assertFalse(any(v['IsLatest'] for v in versions), versions)
            self.assertEqual({v['VersionId'] for v in versions}, {self.v1, self.v2, self.v3})

        with self.subTest(step='DELETE v3'):
            self.c1.delete_object(Bucket=self.bucket, Key=self.key, VersionId=self.v3)
            self.assert_tombstoned(self.v3)
            for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
                u.assert_no_such_key(self, client, self.bucket, self.key, VersionId=self.v3)

        with self.subTest(step='DELETE v1'):
            self.c1.delete_object(Bucket=self.bucket, Key=self.key, VersionId=self.v1)
            self.assert_tombstoned(self.v1)
            for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
                u.assert_no_such_key(self, client, self.bucket, self.key, VersionId=self.v1)
                # The delete marker is still the latest version.
                u.assert_no_such_key(self, client, self.bucket, self.key)

        with self.subTest(step='DELETE delete marker'):
            self.c1.delete_object(Bucket=self.bucket, Key=self.key, VersionId=dm)
            self.assert_tombstoned(dm)
            versions, markers = u.list_versions(self.c1, self.bucket, self.key)
            self.assertEqual(markers, [])
            self.assertEqual([v['VersionId'] for v in versions], [self.v2], versions)
            self.assertTrue(versions[0]['IsLatest'], versions)
            for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
                self.assert_get(client, rgw, self.v2, self.d2)
                self.assert_get(client, rgw, self.v2, self.d2, VersionId=self.v2)
                u.assert_no_such_key(self, client, self.bucket, self.key, VersionId=dm)
            self.assert_latest_in_directory(self.v2)

        self.assert_still_dirty([self.v2])


    def test_delete_marker_replicated_to_rgw2(self):
        """A simple DELETE on RGW1 creates the delete marker on both RGWs: its 0-byte HEAD is cached on
        RGW1 and on RGW2 under the same version, with the delete marker flag, its version id and the
        same mtime in its cache attributes, and in the directory its version HEAD and the plain name
        HEAD are a dirty delete marker."""
        resp = self.c1.delete_object(Bucket=self.bucket, Key=self.key)
        self.assertTrue(resp.get('DeleteMarker'), resp)
        dm = resp.get('VersionId')
        self.assertTrue(dm, resp)

        head_xattrs = {}
        for rgw in (self.rgw1, self.rgw2):
            path = u.cached_head_path(self.cache_dir[rgw.label], self.bucket_id, self.key, dm)
            self.assertTrue(u.wait_until(lambda: os.path.isfile(path)),
                            f'{rgw}: delete marker HEAD not cached at {path}')
            self.assertEqual(os.path.getsize(path), 0, f'{rgw}: delete marker HEAD {path}')
            # The cache attributes a restart rebuilds the delete marker from (LFUDAPolicy::init()).
            xattrs = u.cached_xattrs(path)
            self.assertEqual(xattrs.get('user.rgw.deleteMarker'), '1', f'{rgw}: {path} xattrs {xattrs}')
            self.assertEqual(xattrs.get('user.rgw.version_id'), dm, f'{rgw}: {path} xattrs {xattrs}')
            self.assertTrue(xattrs.get('user.rgw.mtime', '').isdigit(), f'{rgw}: {path} xattrs {xattrs}')
            head_xattrs[rgw.label] = xattrs
        # Both RGWs register the delete marker for cleaning at the same mtime (set by RGW1).
        self.assertEqual(head_xattrs[self.rgw1.label]['user.rgw.mtime'],
                         head_xattrs[self.rgw2.label]['user.rgw.mtime'],
                         'delete marker mtime differs between RGW1 and RGW2')

        entries = self.entries()
        heads = u.version_heads(entries, self.key, dm)
        self.assertTrue(heads, f'no version HEAD for delete marker {dm}')
        for head in heads:
            self.assertTrue(head.flag('deleteMarker'), head)
            self.assertTrue(head.flag('dirty'), head)
            self.assertFalse(head.flag('invalid'), head)
        plain = u.plain_head(entries, self.bucket_id, self.key)
        self.assertIsNotNone(plain, f'no plain-name HEAD for {self.key}')
        self.assertEqual(plain.get('version'), dm, plain)
        self.assertTrue(plain.flag('deleteMarker'), plain)

        for client in (self.c1, self.c2):
            u.assert_no_such_key(self, client, self.bucket, self.key)
        self.assert_still_dirty([self.v1, self.v2, self.v3, dm])

    def test_latest_dirty_version_deleted_falls_back_to_backend(self):
        """v0 is in the backend (a multipart upload: only its clean HEAD is cached), v1 is dirty on top
        of it. Deleting v1 by version id leaves no dirty version, so the plain name HEAD is tombstoned:
        the current version is v0, in the backend. A GET, object tagging and a simple DELETE without a
        versionId must then use the backend, not fail with NoSuchKey."""
        key = u.unique_name('obj')
        d0 = u.random_bytes(cfg.OBJECT_SIZE)
        v0 = u.multipart_put(self.c1, self.bucket, key, d0)
        d1 = u.random_bytes(cfg.OBJECT_SIZE)
        v1 = self.c1.put_object(Bucket=self.bucket, Key=key, Body=d1)['VersionId']

        body, resp = u.get_object(self.c1, self.bucket, key)
        self.assertEqual(resp.get('VersionId'), v1, 'v1 is the latest version before the delete')

        self.c1.delete_object(Bucket=self.bucket, Key=key, VersionId=v1)
        plain = u.plain_head(u.object_dir_entries(self.db, self.bucket_id, key), self.bucket_id, key)
        self.assertIsNotNone(plain, f'no plain-name HEAD for {key}')
        self.assertTrue(plain.flag('invalid'), f'plain-name HEAD not tombstoned: {plain}')

        with self.subTest(step='GET without versionId returns v0 from the backend'):
            for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
                body, resp = u.get_object(client, self.bucket, key)
                self.assertEqual(resp.get('VersionId'), v0, f'{rgw}: VersionId of GET')
                self.assertEqual(u.md5_hex(body), u.md5_hex(d0), f'{rgw}: content of GET')

        with self.subTest(step='object tagging applies to v0'):
            tags = [{'Key': 'k', 'Value': 'v'}]
            self.c1.put_object_tagging(Bucket=self.bucket, Key=key, Tagging={'TagSet': tags})
            # RGW's GetObjectTagging does not return x-amz-version-id: read v0's tags by its versionId.
            got = self.c1.get_object_tagging(Bucket=self.bucket, Key=key, VersionId=v0)
            self.assertEqual(got.get('TagSet'), tags, got)

        with self.subTest(step='simple DELETE creates a delete marker over v0'):
            resp = self.c1.delete_object(Bucket=self.bucket, Key=key)
            self.assertTrue(resp.get('DeleteMarker'), resp)
            dm = resp.get('VersionId')
            versions, markers = u.list_versions(self.c1, self.bucket, key)
            self.assertEqual([m['VersionId'] for m in markers], [dm], markers)
            self.assertTrue(markers[0]['IsLatest'], markers)
            self.assertEqual([v['VersionId'] for v in versions], [v0], versions)
            for client in (self.c1, self.c2):
                u.assert_no_such_key(self, client, self.bucket, key)

    def test_latest_dirty_version_deleted_over_backend_delete_marker(self):
        """As test_latest_dirty_version_deleted_falls_back_to_backend, but the current version in the
        backend is a delete marker: va is in the backend, a simple DELETE (nothing dirty is cached)
        creates a delete marker over it in the backend, and v1 is dirty on top. After v1 is deleted by
        version id, the backend decides: a GET without versionId is NoSuchKey, and va is still
        readable by its versionId."""
        key = u.unique_name('obj')
        da = u.random_bytes(cfg.OBJECT_SIZE)
        va = u.multipart_put(self.c1, self.bucket, key, da)
        resp = self.c1.delete_object(Bucket=self.bucket, Key=key)
        self.assertTrue(resp.get('DeleteMarker'), resp)
        dm = resp.get('VersionId')
        d1 = u.random_bytes(cfg.OBJECT_SIZE)
        v1 = self.c1.put_object(Bucket=self.bucket, Key=key, Body=d1)['VersionId']

        self.c1.delete_object(Bucket=self.bucket, Key=key, VersionId=v1)
        plain = u.plain_head(u.object_dir_entries(self.db, self.bucket_id, key), self.bucket_id, key)
        self.assertIsNotNone(plain, f'no plain-name HEAD for {key}')
        self.assertTrue(plain.flag('invalid'), f'plain-name HEAD not tombstoned: {plain}')

        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(rgw=rgw.label):
                u.assert_no_such_key(self, client, self.bucket, key)
                self.assert_get_key(client, rgw, key, va, da, VersionId=va)
        versions, markers = u.list_versions(self.c1, self.bucket, key)
        self.assertEqual([m['VersionId'] for m in markers], [dm], markers)
        self.assertTrue(markers[0]['IsLatest'], markers)
        self.assertEqual([v['VersionId'] for v in versions], [va], versions)


    def test_delete_clean_version_after_latest_dirty_version_deleted(self):
        """Regression: v0 is in the backend (a multipart upload), read on RGW2 (so RGW2 has it cached), and
        v1, dirty on top of it, is deleted by version id (the plain name HEAD is tombstoned). Deleting v0 by
        version id on RGW1 then removes the object's last version and must create no delete marker: the
        remote delete is not sent for a clean version (it reached the backend on RGW2 without the version,
        as a simple DELETE)."""
        key = u.unique_name('obj')
        d0 = u.random_bytes(cfg.OBJECT_SIZE)
        v0 = u.multipart_put(self.c1, self.bucket, key, d0)
        v1 = self.c1.put_object(Bucket=self.bucket, Key=key, Body=u.random_bytes(cfg.OBJECT_SIZE))['VersionId']
        self.c1.delete_object(Bucket=self.bucket, Key=key, VersionId=v1)
        self.assert_get_key(self.c2, self.rgw2, key, v0, d0, VersionId=v0)

        self.c1.delete_object(Bucket=self.bucket, Key=key, VersionId=v0)

        versions, markers = u.list_versions(self.c1, self.bucket, key)
        self.assertEqual(versions, [], versions)
        self.assertEqual(markers, [], f'delete marker created by deleting a version: {markers}')
        backend = [e for e in u.radosgw_admin('bucket', 'list', '--bucket', self.bucket) if e['name'] == key]
        self.assertEqual(backend, [], f'backend entries left for {key}: {backend}')
        entries = u.object_dir_entries(self.db, self.bucket_id, key)
        self.assertEqual([e for e in entries if e.flag('deleteMarker')], [], 'delete marker HEAD in the directory')
        self.assertEqual(u.version_heads(entries, key, v0), [], 'HEAD of the deleted clean version')
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            with self.subTest(rgw=rgw.label):
                u.assert_no_such_key(self, client, self.bucket, key)


    # --- copy object ------------------------------------------------------------------------
    # A non-multipart source is copied by D4N (write-back copy): the destination is written dirty in
    # the cache, replicated to the other RGW and committed like a PUT, with the source's ETag. A
    # multipart source is copied by the backend. Each test checks the S3 behaviour first and then,
    # separately, the D4N internals (cache directories and block directory).

    def copy(self, client, dst_bucket, dst_key, src_version=None, src_key=None, src_bucket=None, **kwargs):
        """CopyObject; returns (response, version id of the copy).
        RGW sends the CopyObject response headers before the copy is made (chunked, for progress), so,
        unlike AWS, it returns neither x-amz-version-id nor x-amz-copy-source-version-id: the copy's
        version id is read with a HEAD of the destination (its latest version) right after the copy.
        This is RGW's S3 frontend (RGWCopyObj_ObjStore_S3::send_partial_response()/send_response() in
        rgw_rest_s3.cc), above the SAL: it happens without D4N too."""
        source = {'Bucket': src_bucket or self.bucket, 'Key': src_key or self.key}
        if src_version:
            source['VersionId'] = src_version
        resp = client.copy_object(Bucket=dst_bucket, Key=dst_key, CopySource=source, **kwargs)
        version_id = client.head_object(Bucket=dst_bucket, Key=dst_key).get('VersionId')
        return resp, version_id

    @staticmethod
    def etag(value):
        return value.strip('"')

    def assert_client_error(self, fn, status, code):
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            fn()
        err = ctx.exception.response
        self.assertEqual(err['ResponseMetadata']['HTTPStatusCode'], status, err)
        self.assertEqual(err['Error']['Code'], code, err)

    def assert_copy_cached_dirty(self, bkt_id, key, version, data, rgws):
        """D4N: the copy's blocks are cached on each RGW in rgws, its version HEAD is dirty, and its
        data block entries are dirty with every RGW in rgws as a holder."""
        for rgw in rgws:
            u.assert_cached(self, rgw, self.cache_dir[rgw.label], bkt_id, key, version, data)
        entries = u.object_dir_entries(self.db, bkt_id, key)
        heads = u.version_heads(entries, key, version)
        self.assertTrue(heads, f'no version HEAD for copy {key} {version}')
        for head in heads:
            self.assertTrue(head.flag('dirty'), head)
            self.assertFalse(head.flag('invalid'), head)
        blocks = u.data_block_entries(entries, key, version)
        self.assertEqual(sum(int(b.get('size')) for b in blocks), len(data), blocks)
        holders = {self.address[rgw.label] for rgw in rgws}
        for block in blocks:
            self.assertTrue(block.flag('dirty'), block)
            self.assertTrue(holders <= block.hosts, f'data block holders: {block}')
        return entries

    def assert_nothing_written(self, bkt_id, key):
        """D4N: a copy that failed wrote nothing: no directory entries and no cache files for key."""
        entries = u.object_dir_entries(self.db, bkt_id, key)
        self.assertEqual(entries, [], f'directory entries of a failed copy {key}: {entries}')
        for rgw in (self.rgw1, self.rgw2):
            obj_dir = os.path.join(self.cache_dir[rgw.label], u.url_encode(bkt_id), u.url_encode(key))
            files = os.listdir(obj_dir) if os.path.isdir(obj_dir) else []
            self.assertEqual(files, [], f'{rgw}: cache files of a failed copy {key}: {files}')

    def test_copy_dirty_object_to_new_key(self):
        """Copy of v3 (dirty) to a new key on RGW1: a new version with v3's ETag and data, cached and
        replicated; the source is unchanged."""
        dst = u.unique_name('copy')
        resp, vid = self.copy(self.c1, self.bucket, dst)
        self.assertTrue(vid and vid not in (self.v1, self.v2, self.v3), vid)
        self.assertEqual(self.etag(resp['CopyObjectResult']['ETag']), u.md5_hex(self.d3), resp)

        # D4N (checked before any GET; the HEAD above reads no data blocks).
        entries = self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d3, (self.rgw1, self.rgw2))
        plain = u.plain_head(entries, self.bucket_id, dst)
        self.assertIsNotNone(plain, f'no plain-name HEAD for {dst}')
        self.assertEqual(plain.get('version'), vid, plain)

        # S3
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            body, r = u.get_object(client, self.bucket, dst)
            self.assertEqual(r.get('VersionId'), vid, f'{rgw}: VersionId')
            self.assertEqual(self.etag(r['ETag']), u.md5_hex(self.d3), f'{rgw}: ETag')
            self.assertEqual(u.md5_hex(body), u.md5_hex(self.d3), f'{rgw}: content')
        for first, last in ((0, 1023), (cfg.BLOCK_SIZE - 512, cfg.BLOCK_SIZE + 511),
                            (cfg.OBJECT_SIZE - 1000, cfg.OBJECT_SIZE - 1)):
            body, r = u.get_object(self.c1, self.bucket, dst, Range=f'bytes={first}-{last}')
            self.assertEqual(r['ResponseMetadata']['HTTPStatusCode'], 206)
            self.assertEqual(body, self.d3[first:last + 1], f'range {first}-{last}')
        versions, _ = u.list_versions(self.c1, self.bucket, self.key)
        self.assertEqual([v['VersionId'] for v in versions], [self.v3, self.v2, self.v1], versions)
        self.assertTrue(versions[0]['IsLatest'], versions)
        self.assert_still_dirty([self.v1, self.v2, self.v3])

    def test_copy_specific_source_version(self):
        """Copy of v1 by its versionId: the copy has v1's data and ETag, on both RGWs."""
        dst = u.unique_name('copy')
        resp, vid = self.copy(self.c1, self.bucket, dst, src_version=self.v1)
        self.assertEqual(self.etag(resp['CopyObjectResult']['ETag']), u.md5_hex(self.d1), resp)
        self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d1, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, dst, vid, self.d1)

    def test_copy_older_version_onto_same_key(self):
        """Copying v1 by its versionId onto the same key restores it: a new latest version v4 with v1's data."""
        _, v4 = self.copy(self.c1, self.bucket, self.key, src_version=self.v1)
        self.assertTrue(v4 and v4 not in (self.v1, self.v2, self.v3), v4)
        # D4N: v4 is cached (with v1's data) and replicated, dirty, and the latest version in the directory.
        self.assert_copy_cached_dirty(self.bucket_id, self.key, v4, self.d1, (self.rgw1, self.rgw2))
        self.assert_latest_in_directory(v4)
        self.assert_still_dirty([self.v1, self.v2, self.v3, v4])

        # S3
        versions, _ = u.list_versions(self.c1, self.bucket, self.key)
        self.assertEqual([v['VersionId'] for v in versions], [v4, self.v3, self.v2, self.v1], versions)
        self.assertTrue(versions[0]['IsLatest'], versions)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, self.key, v4, self.d1)
        self.assert_latest_in_directory(v4)

    def test_copy_onto_itself(self):
        """Copying the current object onto itself without a versionId is rejected (400 InvalidRequest)
        unless the metadata is replaced; with MetadataDirective=REPLACE it creates a new latest version."""
        self.assert_client_error(lambda: self.copy(self.c1, self.bucket, self.key), 400, 'InvalidRequest')
        _, v4 = self.copy(self.c1, self.bucket, self.key, MetadataDirective='REPLACE', Metadata={'k': 'v'})
        self.assertTrue(v4 and v4 not in (self.v1, self.v2, self.v3), v4)
        # D4N: the rejected copy wrote nothing; v4 is cached (v3's data), replicated, dirty and the latest.
        self.assert_copy_cached_dirty(self.bucket_id, self.key, v4, self.d3, (self.rgw1, self.rgw2))
        self.assert_latest_in_directory(v4)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, self.key, v4, self.d3)
            self.assertEqual(client.head_object(Bucket=self.bucket, Key=self.key).get('Metadata'), {'k': 'v'},
                             f'{rgw}: metadata')

    def test_copy_on_rgw2(self):
        """A copy made on RGW2 (whose replica of v3 is the source) is cached on RGW2 and replicated to RGW1."""
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c2, self.bucket, dst)
        self.assertTrue(vid, vid)
        self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d3, (self.rgw2, self.rgw1))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, dst, vid, self.d3)

    def test_copy_across_buckets(self):
        """Copy of v3 to another versioned bucket: cached and replicated under that bucket; the source
        bucket is unchanged."""
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c1, self.bucket_b, dst)
        self.assertTrue(vid, vid)
        self.assert_copy_cached_dirty(self.bucket_b_id, dst, vid, self.d3, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            body, r = u.get_object(client, self.bucket_b, dst)
            self.assertEqual(r.get('VersionId'), vid, f'{rgw}: VersionId')
            self.assertEqual(u.md5_hex(body), u.md5_hex(self.d3), f'{rgw}: content')
        versions, _ = u.list_versions(self.c1, self.bucket, self.key)
        self.assertEqual([v['VersionId'] for v in versions], [self.v3, self.v2, self.v1], versions)

    def test_copy_to_non_versioned_bucket_overwrites_dirty_object(self):
        """Copy of v1 over a dirty object in a non-versioned bucket: GET returns v1's data on both RGWs,
        and the replaced (dirty) version is tombstoned so that its write-back cannot overwrite the copy."""
        x = u.unique_name('obj')
        dx = u.random_bytes(cfg.OBJECT_SIZE)
        self.c1.put_object(Bucket=self.bucket_nv, Key=x, Body=dx)
        before = u.plain_head(u.object_dir_entries(self.db, self.bucket_nv_id, x), self.bucket_nv_id, x)
        self.assertIsNotNone(before, f'no plain-name HEAD for {x}')
        old_version = before.get('version')

        _, vid = self.copy(self.c1, self.bucket_nv, x, src_version=self.v1)
        self.assertIn(vid, (None, 'null'), vid)  # non-versioned destination

        # D4N (before any GET): the copy has its own D4N version, cached and replicated, dirty; the
        # replaced (dirty) version is tombstoned.
        entries = u.object_dir_entries(self.db, self.bucket_nv_id, x)
        plain = u.plain_head(entries, self.bucket_nv_id, x)
        new_version = plain.get('version')
        self.assertNotEqual(new_version, old_version, plain)
        self.assertTrue(plain.flag('dirty'), plain)
        self.assert_copy_cached_dirty(self.bucket_nv_id, x, new_version, self.d1, (self.rgw1, self.rgw2))
        old_heads = u.version_heads(entries, x, old_version)
        self.assertTrue(old_heads, f'no version HEAD for the replaced version {old_version}')
        for head in old_heads:
            self.assertTrue(head.flag('invalid'), f'replaced version not tombstoned: {head}')

        # S3
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            body, _ = u.get_object(client, self.bucket_nv, x)
            self.assertEqual(u.md5_hex(body), u.md5_hex(self.d1), f'{rgw}: content')

    def test_copy_multipart_object_is_copied_by_backend(self):
        """Copy of a multipart object: the backend makes the copy; D4N caches only its (clean) HEAD."""
        src = u.unique_name('mp')
        dm = u.random_bytes(cfg.OBJECT_SIZE)
        u.multipart_put(self.c1, self.bucket, src, dm)
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c1, self.bucket, dst, src_key=src)
        self.assertTrue(vid, vid)

        # D4N (before any read, which would cache the copy's blocks on the reading RGW)
        for rgw in (self.rgw1, self.rgw2):
            self.assertEqual(u.cached_blocks(self.cache_dir[rgw.label], self.bucket_id, dst, vid), [],
                             f'{rgw}: blocks of a backend copy are cached')
        heads = u.version_heads(u.object_dir_entries(self.db, self.bucket_id, dst), dst, vid)
        self.assertTrue(heads, f'no version HEAD for copy {dst} {vid}')
        for head in heads:
            self.assertFalse(head.flag('dirty'), head)

        # S3 (no ETag comparison: S3 does not promise a multipart source's ETag for the copy)
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, dst, vid, dm)

    def test_copy_is_independent_of_source(self):
        """Deleting the source version does not affect the copy; deleting the copy by its versionId
        makes it unreadable on both RGWs."""
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c1, self.bucket, dst, src_version=self.v1)
        self.c1.delete_object(Bucket=self.bucket, Key=self.key, VersionId=self.v1)
        # D4N: the copy's own blocks and entries are untouched by the deletion of its source.
        self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d1, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            u.assert_no_such_key(self, client, self.bucket, self.key, VersionId=self.v1)
            self.assert_get_key(client, rgw, dst, vid, self.d1)
        self.c1.delete_object(Bucket=self.bucket, Key=dst, VersionId=vid)
        for client in (self.c1, self.c2):
            u.assert_no_such_key(self, client, self.bucket, dst, VersionId=vid)
        # D4N: the deleted (dirty) copy's version HEAD is tombstoned until cleaning removes it.
        heads = u.version_heads(u.object_dir_entries(self.db, self.bucket_id, dst), dst, vid)
        self.assertTrue(heads, f'no version HEAD for the deleted copy {vid}')
        for head in heads:
            self.assertTrue(head.flag('invalid'), f'deleted copy not tombstoned: {head}')

    def test_conditional_copy(self):
        """x-amz-copy-source-if-match / -if-none-match against v3's ETag."""
        etag3 = u.md5_hex(self.d3)
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c1, self.bucket, dst, CopySourceIfMatch=etag3)
        self.assertTrue(vid, vid)
        self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d3, (self.rgw1, self.rgw2))
        for kwargs in ({'CopySourceIfMatch': '0' * 32}, {'CopySourceIfNoneMatch': etag3}):
            with self.subTest(**kwargs):
                failed = u.unique_name('copy')
                self.assert_client_error(lambda: self.copy(self.c1, self.bucket, failed, **kwargs),
                                         412, 'PreconditionFailed')
                self.assert_nothing_written(self.bucket_id, failed)

    def test_copy_metadata_directive(self):
        """MetadataDirective=COPY (default) keeps the source's user metadata and ignores the request's;
        REPLACE uses the request's."""
        src = u.unique_name('obj')
        self.c1.put_object(Bucket=self.bucket, Key=src, Body=self.d1, Metadata={'a': '1'})
        dst_copy, dst_replace = u.unique_name('copy'), u.unique_name('copy')
        _, vid_copy = self.copy(self.c1, self.bucket, dst_copy, src_key=src, Metadata={'b': '2'})
        _, vid_replace = self.copy(self.c1, self.bucket, dst_replace, src_key=src, MetadataDirective='REPLACE',
                                   Metadata={'b': '2'})
        # D4N: both copies are cached (the source's data) and replicated, dirty.
        self.assert_copy_cached_dirty(self.bucket_id, dst_copy, vid_copy, self.d1, (self.rgw1, self.rgw2))
        self.assert_copy_cached_dirty(self.bucket_id, dst_replace, vid_replace, self.d1, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assertEqual(client.head_object(Bucket=self.bucket, Key=dst_copy).get('Metadata'), {'a': '1'},
                             f'{rgw}: COPY')
            self.assertEqual(client.head_object(Bucket=self.bucket, Key=dst_replace).get('Metadata'), {'b': '2'},
                             f'{rgw}: REPLACE')

    def test_copy_of_deleted_object(self):
        """With a delete marker as the current version, a copy without versionId is NoSuchKey; a copy of
        v1 by its versionId succeeds; a copy of the delete marker by its versionId is InvalidRequest."""
        dm = self.c1.delete_object(Bucket=self.bucket, Key=self.key).get('VersionId')
        failed = u.unique_name('copy')
        self.assert_client_error(lambda: self.copy(self.c1, self.bucket, failed), 404, 'NoSuchKey')
        self.assert_nothing_written(self.bucket_id, failed)
        dst = u.unique_name('copy')
        _, vid = self.copy(self.c1, self.bucket, dst, src_version=self.v1)
        self.assert_copy_cached_dirty(self.bucket_id, dst, vid, self.d1, (self.rgw1, self.rgw2))
        for rgw, client in ((self.rgw1, self.c1), (self.rgw2, self.c2)):
            self.assert_get_key(client, rgw, dst, vid, self.d1)
        with self.subTest(step='S3: copy of a delete marker by its versionId'):
            # AWS: 400 InvalidRequest ("The source of a copy request may not specifically refer to a delete
            # marker by version id"). Observed with D4N: 404 NoSuchKey. RGWCopyObj has no delete marker check,
            # so this is expected to be RGW behaviour independent of D4N; NOT YET CONFIRMED without D4N.
            # To confirm, run the same request against an RGW with rgw_filter=none (or a RADOS-only RGW):
            # if it also returns 404, this is an RGW S3 gap, not a D4N bug.
            failed = u.unique_name('copy')
            self.assert_client_error(lambda: self.copy(self.c1, self.bucket, failed, src_version=dm),
                                     400, 'InvalidRequest')
            self.assert_nothing_written(self.bucket_id, failed)


if __name__ == '__main__':
    unittest.main()
