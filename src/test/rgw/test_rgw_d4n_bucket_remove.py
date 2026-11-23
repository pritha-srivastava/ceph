#!/usr/bin/env python3
"""
D4N Bucket Removal Concurrency Tests

Tests the D4N filter's bucket removal code (D4NFilterBucket::process_remove_objects)
by using radosgw-admin bucket rm --purge-objects while concurrent S3 operations
are in progress.

Unlike the boto3 DeleteBucket API (which requires an empty bucket), radosgw-admin
bucket rm --purge-objects removes non-empty buckets and triggers the D4N filter's
cleanup logic including:
- Cross-page version removal with transaction batching
- Cache invalidation and cleanup
- Directory entry removal
- Proper handling of concurrent reads/writes

Tests verify:
- No stale cache/directory entries remain after removal
- Concurrent reads complete successfully without interruption
- Concurrent writes either complete or fail with appropriate errors
- Final relist/cleanup cycle handles new objects written during removal

Run from the build directory, with the cluster and RGW up:
  python3 ../src/test/rgw/test_rgw_d4n_bucket_remove.py -v
See d4n_test_config.py for the environment variables and the S3 user prerequisite.
"""

import hashlib
import logging
import subprocess
import threading
import time
import unittest

import botocore.exceptions

import d4n_test_config as cfg
import d4n_test_utils as u

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
log = logging.getLogger(__name__)


def radosgw_admin_with_d4n(*args, timeout=120):
    """
    Runs radosgw-admin WITH the D4N filter enabled.

    Unlike d4n_test_utils.radosgw_admin which uses --rgw-filter=none to bypass D4N,
    this function keeps the D4N filter active so we can test D4N's bucket removal logic.

    When called with 'bucket rm --purge-objects', this triggers D4NFilterBucket::remove()
    which calls process_remove_objects() - the core D4N bucket removal code that:
    - Removes all objects and versions (even if bucket is not empty)
    - Invalidates/deletes cache entries
    - Removes directory entries in batched transactions
    - Performs a final relist to catch objects written during removal

    Returns: (returncode, stdout, stderr)
    """
    cmd = [cfg.RADOSGW_ADMIN_BIN, '-c', cfg.CEPH_CONF, *args]
    proc = subprocess.run(cmd, capture_output=True, timeout=timeout, text=True)
    return proc.returncode, proc.stdout, proc.stderr


class TestBucketRemoveConcurrency(unittest.TestCase):
    """Test suite for concurrent bucket removal operations"""

    @classmethod
    def setUpClass(cls):
        """One-time setup for all tests"""
        cls.rgw1 = cfg.RGW1
        cls.client = u.make_client(cls.rgw1)

        # Verify S3 credentials work
        try:
            cls.client.list_buckets()
        except botocore.exceptions.ClientError as e:
            raise AssertionError(
                f'{cls.rgw1}: S3 credentials rejected ({e}); create the user first, see '
                f'd4n_test_config.py') from e

    def setUp(self):
        """Setup for each test"""
        self.test_bucket = None

    def tearDown(self):
        """Cleanup after each test"""
        if self.test_bucket:
            try:
                # First try to verify bucket still exists
                try:
                    self.client.head_bucket(Bucket=self.test_bucket)
                    bucket_exists = True
                except botocore.exceptions.ClientError as e:
                    if e.response['Error']['Code'] == '404':
                        bucket_exists = False
                    else:
                        raise

                if bucket_exists:
                    # Bucket still exists - use radosgw-admin for robust cleanup
                    log.info(f"Cleaning up bucket {self.test_bucket} with radosgw-admin")
                    returncode, stdout, stderr = radosgw_admin_with_d4n(
                        'bucket', 'rm',
                        '--bucket', self.test_bucket,
                        '--purge-objects',
                        timeout=30  # Pass timeout to subprocess.run
                    )
                    if returncode != 0:
                        log.warning(f"radosgw-admin cleanup failed for {self.test_bucket}: {stderr}")
                else:
                    log.info(f"Bucket {self.test_bucket} already removed")

            except Exception as e:
                log.warning(f"Cleanup exception for {self.test_bucket}: {e}")

    def _create_test_data(self, size_kb=100):
        """Generate test data with known checksum"""
        data = u.random_bytes(size_kb * 1024)
        checksum = u.md5_hex(data)
        return data, checksum

    def _verify_bucket_removed(self, bucket_name):
        """Verify bucket is completely removed"""
        # ListObjectVersions should return NoSuchBucket
        with self.assertRaises(botocore.exceptions.ClientError) as ctx:
            self.client.list_object_versions(Bucket=bucket_name)
        err = ctx.exception.response
        self.assertEqual(err['Error']['Code'], 'NoSuchBucket',
                        f"Expected NoSuchBucket, got {err['Error']['Code']}")

    # Case 1: Bucket remove + simultaneous write (non-versioned)
    def test_bucket_remove_concurrent_write_nonversioned(self):
        """
        Case 1: Bucket remove call and then a simultaneous write call

        While the bucket remove call is running, the write should complete and
        populate the cache and directory with the newly written object. Then,
        the final step of the bucket remove call should relist the new cache
        contents and perform another invalidate/delete cycle, leaving the bucket empty.
        """
        bucket_name = u.unique_name('test-remove-write')
        self.test_bucket = bucket_name

        # Create bucket with initial objects
        self.client.create_bucket(Bucket=bucket_name)

        # Create initial objects
        for i in range(5):
            data, _ = self._create_test_data(size_kb=50)
            self.client.put_object(
                Bucket=bucket_name,
                Key=f'initial-object-{i}',
                Body=data
            )

        # Wait for cache propagation
        time.sleep(2)

        # Synchronization primitives
        remove_started = threading.Event()
        remove_result = {'error': None, 'success': False}
        write_result = {'error': None, 'success': False}

        def remove_bucket_thread():
            """Thread to remove bucket using radosgw-admin"""
            try:
                remove_started.set()
                time.sleep(0.5)

                # Use radosgw-admin bucket rm --purge-objects to test D4N filter's bucket removal
                # This removes the bucket even if it's not empty, triggering D4N's cleanup logic
                log.info(f"Starting radosgw-admin bucket rm for {bucket_name}")
                returncode, stdout, stderr = radosgw_admin_with_d4n(
                    'bucket', 'rm',
                    '--bucket', bucket_name,
                    '--purge-objects',
                    timeout=120
                )

                if returncode == 0:
                    remove_result['success'] = True
                    log.info(f"radosgw-admin bucket rm succeeded for {bucket_name}")
                else:
                    remove_result['error'] = f"radosgw-admin failed: {stderr}"
                    log.warning(f"radosgw-admin bucket rm failed for {bucket_name}: {stderr}")

            except subprocess.TimeoutExpired:
                remove_result['error'] = 'timeout'
                log.error(f"radosgw-admin bucket rm timed out for {bucket_name}")
            except Exception as e:
                remove_result['error'] = str(e)
                log.error(f"radosgw-admin bucket rm exception for {bucket_name}: {e}")

        def write_object_thread():
            """Thread to write object during removal"""
            try:
                remove_started.wait(timeout=10)

                # Write new object
                data, checksum = self._create_test_data(size_kb=100)
                self.client.put_object(
                    Bucket=bucket_name,
                    Key='concurrent-write-object',
                    Body=data
                )
                write_result['success'] = True
                write_result['checksum'] = checksum

            except botocore.exceptions.ClientError as e:
                write_result['error'] = e.response['Error']['Code']
            except Exception as e:
                write_result['error'] = str(e)

        # Start threads
        remove_thread = threading.Thread(target=remove_bucket_thread)
        write_thread = threading.Thread(target=write_object_thread)

        remove_thread.start()
        write_thread.start()

        remove_thread.join(timeout=30)
        write_thread.join(timeout=30)

        # Verify results
        if write_result['success']:
            # Write completed - two acceptable outcomes:
            # 1. Removal succeeded (final relist cleaned up the concurrent write)
            # 2. Removal returned BucketNotEmpty (concurrent write not yet handled)

            if remove_result['success']:
                log.info("Case 1 outcome: Write succeeded, removal succeeded (final relist cleaned up)")
                time.sleep(2)
                self._verify_bucket_removed(bucket_name)
            else:
                # Check if error indicates bucket not empty
                error_str = str(remove_result['error'])
                bucket_not_empty = ('BucketNotEmpty' in error_str or
                                   'bucket not empty' in error_str.lower() or
                                   'directory not empty' in error_str.lower())

                self.assertTrue(bucket_not_empty,
                              f"Expected BucketNotEmpty-related error, got: {remove_result['error']}")
                log.info("Case 1 outcome: Write succeeded, removal returned bucket-not-empty - acceptable")

                # Clean up the concurrent write for final verification
                # (radosgw-admin will handle this in tearDown)
        else:
            # Write failed - bucket should be removed successfully
            self.assertTrue(remove_result['success'], f"Remove failed: {remove_result['error']}")
            log.info("Case 1 outcome: Write failed, removal succeeded")
            time.sleep(2)
            self._verify_bucket_removed(bucket_name)

    # Case 2: Read + simultaneous bucket remove (non-versioned)
    def test_concurrent_read_bucket_remove_nonversioned(self):
        """
        Case 2: Read call and then a simultaneous bucket remove call

        The read call should be fully serviced without any -ENOENT interrupting
        it from the bucket remove process.
        """
        bucket_name = u.unique_name('test-read-remove')
        self.test_bucket = bucket_name

        # Create bucket with object
        self.client.create_bucket(Bucket=bucket_name)

        data, expected_checksum = self._create_test_data(size_kb=200)
        object_key = 'read-test-object'

        self.client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=data
        )

        time.sleep(2)

        # Synchronization
        read_started = threading.Event()
        read_result = {'error': None, 'checksum': None, 'success': False}
        remove_result = {'error': None, 'success': False}

        def read_object_thread():
            """Thread to read object"""
            try:
                read_started.set()

                response = self.client.get_object(Bucket=bucket_name, Key=object_key)

                # Read data in chunks to simulate longer operation
                md5_hash = hashlib.md5()
                for chunk in response['Body'].iter_chunks(chunk_size=4096):
                    md5_hash.update(chunk)
                    time.sleep(0.001)

                read_result['checksum'] = md5_hash.hexdigest()
                read_result['success'] = True

            except botocore.exceptions.ClientError as e:
                read_result['error'] = e.response['Error']['Code']
            except Exception as e:
                read_result['error'] = str(e)

        def remove_bucket_thread():
            """Thread to remove bucket using radosgw-admin"""
            try:
                read_started.wait(timeout=10)
                time.sleep(0.1)

                # Use radosgw-admin to remove bucket with D4N filter active
                returncode, stdout, stderr = radosgw_admin_with_d4n(
                    'bucket', 'rm',
                    '--bucket', bucket_name,
                    '--purge-objects'
                )

                if returncode == 0:
                    remove_result['success'] = True
                else:
                    remove_result['error'] = f"radosgw-admin failed: {stderr}"

            except subprocess.TimeoutExpired:
                remove_result['error'] = 'timeout'
            except Exception as e:
                remove_result['error'] = str(e)

        # Start threads
        read_thread = threading.Thread(target=read_object_thread)
        remove_thread = threading.Thread(target=remove_bucket_thread)

        read_thread.start()
        remove_thread.start()

        read_thread.join(timeout=30)
        remove_thread.join(timeout=30)

        # Read should complete successfully with correct data
        self.assertTrue(read_result['success'], f"Read failed: {read_result['error']}")
        self.assertEqual(read_result['checksum'], expected_checksum,
                        "Read data checksum mismatch - read was interrupted")

        time.sleep(2)
        self._verify_bucket_removed(bucket_name)

    # Case 3: Versioned bucket remove + simultaneous write
    def test_versioned_bucket_remove_concurrent_write(self):
        """
        Case 3: Bucket remove call and then a simultaneous write call (versioned)

        Create a versioned bucket containing a few objects, including versions
        and delete markers. Start a bucket remove call and, while it is running,
        issue a PUT of a new object.

        Acceptable outcomes:
        1. PUT completes before bucket removal: bucket remove returns 409 BucketNotEmpty
           OR final relist picks up and removes the new version
        2. Bucket removal completes first: PUT returns 404 NoSuchBucket
        """
        bucket_name = u.unique_name('test-versioned-remove')
        self.test_bucket = bucket_name

        # Create versioned bucket
        u.create_versioned_bucket(self.client, bucket_name)
        time.sleep(2)

        # Create objects with multiple versions
        for i in range(3):
            for version in range(2):
                data, _ = self._create_test_data(size_kb=50)
                self.client.put_object(
                    Bucket=bucket_name,
                    Key=f'object-{i}',
                    Body=data
                )

        # Create delete marker
        self.client.delete_object(Bucket=bucket_name, Key='object-0')
        time.sleep(2)

        # Synchronization
        remove_started = threading.Event()
        remove_result = {'error': None, 'success': False}
        write_result = {'error': None, 'success': False, 'version_id': None}

        def remove_bucket_thread():
            """Thread to remove versioned bucket using radosgw-admin"""
            try:
                remove_started.set()
                time.sleep(0.5)

                # Use radosgw-admin bucket rm --purge-objects
                # This tests D4N's handling of versioned bucket removal with versions present
                returncode, stdout, stderr = radosgw_admin_with_d4n(
                    'bucket', 'rm',
                    '--bucket', bucket_name,
                    '--purge-objects'
                )

                if returncode == 0:
                    remove_result['success'] = True
                else:
                    remove_result['error'] = f"radosgw-admin failed: {stderr}"

            except subprocess.TimeoutExpired:
                remove_result['error'] = 'timeout'
            except Exception as e:
                remove_result['error'] = str(e)

        def write_object_thread():
            """Thread to write new version during removal"""
            try:
                remove_started.wait(timeout=10)

                data, _ = self._create_test_data(size_kb=100)
                response = self.client.put_object(
                    Bucket=bucket_name,
                    Key='concurrent-object',
                    Body=data
                )
                write_result['success'] = True
                write_result['version_id'] = response.get('VersionId')

            except botocore.exceptions.ClientError as e:
                write_result['error'] = e.response['Error']['Code']
            except Exception as e:
                write_result['error'] = str(e)

        # Start threads
        remove_thread = threading.Thread(target=remove_bucket_thread)
        write_thread = threading.Thread(target=write_object_thread)

        remove_thread.start()
        write_thread.start()

        remove_thread.join(timeout=30)
        write_thread.join(timeout=30)

        # Verify acceptable outcomes
        if write_result['success']:
            # PUT completed - two acceptable outcomes
            if remove_result['success']:
                log.info("Case 3 outcome: PUT succeeded, removal succeeded - final relist cleaned up")
                time.sleep(2)
                self._verify_bucket_removed(bucket_name)
            else:
                # Check if error indicates bucket not empty
                error_str = str(remove_result['error'])
                bucket_not_empty = ('BucketNotEmpty' in error_str or
                                   'bucket not empty' in error_str.lower() or
                                   'directory not empty' in error_str.lower())

                self.assertTrue(bucket_not_empty,
                              f"Expected BucketNotEmpty-related error, got: {remove_result['error']}")
                log.info("Case 3 outcome: PUT succeeded, removal returned bucket-not-empty - acceptable")

                # Bucket cleanup will happen in tearDown via radosgw-admin
        else:
            # PUT failed
            self.assertEqual(write_result['error'], 'NoSuchBucket',
                           f"Unexpected write error: {write_result['error']}")
            self.assertTrue(remove_result['success'],
                           f"Remove should succeed if write failed: {remove_result['error']}")
            log.info("Case 3 outcome: Bucket removed before PUT - acceptable")
            time.sleep(2)
            self._verify_bucket_removed(bucket_name)

    # Case 4: Versioned GET with version-id + bucket remove
    def test_versioned_get_with_version_id_concurrent_remove(self):
        """
        Case 4: Read call and then a simultaneous bucket remove call
                (GET with version-id on versioned bucket)

        The read must complete fully if it started before the version was removed.
        If the version is removed before the GET starts, return 404.
        """
        bucket_name = u.unique_name('test-versioned-get-id')
        self.test_bucket = bucket_name

        # Create versioned bucket
        u.create_versioned_bucket(self.client, bucket_name)
        time.sleep(2)

        # Create object with version
        data, expected_checksum = self._create_test_data(size_kb=200)
        put_response = self.client.put_object(
            Bucket=bucket_name,
            Key='versioned-object',
            Body=data
        )
        version_id = put_response['VersionId']
        time.sleep(2)

        # Synchronization
        read_started = threading.Event()
        read_result = {'error': None, 'checksum': None, 'success': False}
        remove_result = {'error': None, 'success': False}

        def read_object_thread():
            """Thread to read specific version"""
            try:
                read_started.set()

                response = self.client.get_object(
                    Bucket=bucket_name,
                    Key='versioned-object',
                    VersionId=version_id
                )

                md5_hash = hashlib.md5()
                for chunk in response['Body'].iter_chunks(chunk_size=4096):
                    md5_hash.update(chunk)
                    time.sleep(0.001)

                read_result['checksum'] = md5_hash.hexdigest()
                read_result['success'] = True

            except botocore.exceptions.ClientError as e:
                read_result['error'] = e.response['Error']['Code']
            except Exception as e:
                read_result['error'] = str(e)

        def remove_bucket_thread():
            """Thread to remove bucket using radosgw-admin"""
            try:
                read_started.wait(timeout=10)
                time.sleep(0.1)

                # Use radosgw-admin to remove versioned bucket while read is in progress
                returncode, stdout, stderr = radosgw_admin_with_d4n(
                    'bucket', 'rm',
                    '--bucket', bucket_name,
                    '--purge-objects'
                )

                if returncode == 0:
                    remove_result['success'] = True
                else:
                    remove_result['error'] = f"radosgw-admin failed: {stderr}"

            except subprocess.TimeoutExpired:
                remove_result['error'] = 'timeout'
            except Exception as e:
                remove_result['error'] = str(e)

        # Start threads
        read_thread = threading.Thread(target=read_object_thread)
        remove_thread = threading.Thread(target=remove_bucket_thread)

        read_thread.start()
        remove_thread.start()

        read_thread.join(timeout=30)
        remove_thread.join(timeout=30)

        # Verify outcomes
        if read_result['success']:
            self.assertEqual(read_result['checksum'], expected_checksum,
                           "Read completed but data is incorrect")
            log.info("Read completed successfully before version removed")
        else:
            self.assertIn(read_result['error'], ['NoSuchVersion', 'NoSuchKey', 'NoSuchBucket'],
                         f"Unexpected read error: {read_result['error']}")
            log.info(f"Read failed with {read_result['error']} - version removed first")

        time.sleep(2)
        self._verify_bucket_removed(bucket_name)

    # Case 5: Versioned GET without version-id + bucket remove
    def test_versioned_get_without_version_id_concurrent_remove(self):
        """
        Case 5: Read call and then a simultaneous bucket remove call
                (GET without version-id on versioned bucket)

        The read returns either the full latest version or 404, never partial data.
        """
        bucket_name = u.unique_name('test-versioned-get-latest')
        self.test_bucket = bucket_name

        # Create versioned bucket
        u.create_versioned_bucket(self.client, bucket_name)
        time.sleep(2)

        object_key = 'latest-version-object'

        # Older version
        old_data, _ = self._create_test_data(size_kb=100)
        self.client.put_object(Bucket=bucket_name, Key=object_key, Body=old_data)

        # Latest version
        latest_data, expected_checksum = self._create_test_data(size_kb=200)
        self.client.put_object(Bucket=bucket_name, Key=object_key, Body=latest_data)
        time.sleep(2)

        # Synchronization
        read_started = threading.Event()
        read_result = {'error': None, 'checksum': None, 'success': False, 'etag': None}
        remove_result = {'error': None, 'success': False}

        def read_object_thread():
            """Thread to read latest version (no version-id specified)"""
            try:
                read_started.set()

                response = self.client.get_object(Bucket=bucket_name, Key=object_key)
                read_result['etag'] = response['ETag'].strip('"')

                md5_hash = hashlib.md5()
                for chunk in response['Body'].iter_chunks(chunk_size=4096):
                    md5_hash.update(chunk)
                    time.sleep(0.001)

                read_result['checksum'] = md5_hash.hexdigest()
                read_result['success'] = True

            except botocore.exceptions.ClientError as e:
                read_result['error'] = e.response['Error']['Code']
            except Exception as e:
                read_result['error'] = str(e)

        def remove_bucket_thread():
            """Thread to remove bucket using radosgw-admin"""
            try:
                read_started.wait(timeout=10)
                time.sleep(0.1)

                # Use radosgw-admin to remove versioned bucket
                returncode, stdout, stderr = radosgw_admin_with_d4n(
                    'bucket', 'rm',
                    '--bucket', bucket_name,
                    '--purge-objects'
                )

                if returncode == 0:
                    remove_result['success'] = True
                else:
                    remove_result['error'] = f"radosgw-admin failed: {stderr}"

            except subprocess.TimeoutExpired:
                remove_result['error'] = 'timeout'
            except Exception as e:
                remove_result['error'] = str(e)

        # Start threads
        read_thread = threading.Thread(target=read_object_thread)
        remove_thread = threading.Thread(target=remove_bucket_thread)

        read_thread.start()
        remove_thread.start()

        read_thread.join(timeout=30)
        remove_thread.join(timeout=30)

        # Verify outcomes
        if read_result['success']:
            self.assertEqual(read_result['checksum'], expected_checksum,
                           "Read completed but returned wrong version or partial data")
            self.assertEqual(read_result['etag'], expected_checksum,
                           "ETag doesn't match expected checksum")
            log.info("Read completed successfully with full latest version")
        else:
            self.assertIn(read_result['error'], ['NoSuchKey', 'NoSuchBucket'],
                         f"Unexpected read error: {read_result['error']}")
            log.info(f"Read failed with {read_result['error']} - acceptable")

        time.sleep(2)
        self._verify_bucket_removed(bucket_name)


if __name__ == '__main__':
    # Run with: python3 ../src/test/rgw/test_rgw_d4n_bucket_remove.py -v
    unittest.main()
