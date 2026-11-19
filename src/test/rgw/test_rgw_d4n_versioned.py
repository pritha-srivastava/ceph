"""
D4N distributed end-to-end tests: versioned bucket, write cache on, remote put on.

Runs against a live setup of two RGWs (RGW1, RGW2) with the D4N filter, sharing one
FoundationDB block directory, over RADOS. RGW1 replicates to RGW2
(rgw_d4n_remote_cache_address), synchronously (rgw_d4n_async_remote_put=false).

Each test writes its own object as three 7 MiB versions v1, v2, v3 on RGW1, and checks the S3
responses on both RGWs, the block files in each RGW's cache directory and the block directory
entries in FoundationDB.

rgw_d4n_cache_cleaning_interval must be long (see D4N_MIN_CLEANING_INTERVAL in
d4n_test_config.py): every version must still be dirty when a test ends. Write-back timing is
covered by separate tests.

Run from the build directory, with the cluster and both RGWs up:
  python3 ../src/test/rgw/test_rgw_d4n_versioned.py -v
See d4n_test_config.py for the environment variables and the S3 user prerequisite.
"""

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
        cls.db = u.open_fdb()

    @classmethod
    def tearDownClass(cls):
        u.delete_bucket_and_versions(cls.c1, cls.bucket)

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


if __name__ == '__main__':
    unittest.main()
