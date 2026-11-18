import re
import boto3
import os
import fdb
import botocore
import string
import random
import unittest
import subprocess
from botocore.client import Config
import hashlib
import shutil

# FoundationDB version
fdb.api_version(730)

from botocore.handlers import validate_bucket_name

endpoint = 'http://localhost:8000'

access = 'test3'
secret = 'test3'

def generate_random(size, part_size=5*1024*1024):
    """
    Generate the specified number random data.
    (actually each MB is a repetition of the first KB)
    """
    chunk = 1024
    allowed = string.ascii_letters
    for x in range(0, size, part_size):
        strpart = ''.join([allowed[random.randint(0, len(allowed) - 1)] for _ in range(chunk)])
        s = ''
        left = size - x
        this_part_size = min(left, part_size)
        for y in range(this_part_size // chunk):
            s = s + strpart
        if this_part_size > len(s):
            s = s + strpart[0:this_part_size - len(s)]
        yield s
        if (x == size):
            return

def _multipart_upload(bucket_name, key, size, part_size=5*1024*1024, client=None, content_type=None, metadata=None, resend_parts=[]):
    """
    generate a multi-part upload for a random file of specifed size,
    if requested, generate a list of the parts
    return the upload descriptor
    """

    if content_type == None and metadata == None:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key)
    else:
        response = client.create_multipart_upload(Bucket=bucket_name, Key=key, Metadata=metadata, ContentType=content_type)

    upload_id = response['UploadId']
    s = ''
    parts = []
    for i, part in enumerate(generate_random(size, part_size)):
        # part_num is necessary because PartNumber for upload_part and in parts must start at 1 and i starts at 0
        part_num = i+1
        s += part
        response = client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)
        parts.append({'ETag': response['ETag'].strip('"'), 'PartNumber': part_num})
        if i in resend_parts:
            client.upload_part(UploadId=upload_id, Bucket=bucket_name, Key=key, PartNumber=part_num, Body=part)

    return (upload_id, s, parts)

ACCESS_KEY = 'test3'
SECRET_KEY = 'test3'

client = boto3.client(service_name='s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=endpoint,
            use_ssl=False,
            verify=False)

s3 = boto3.resource('s3',
            aws_access_key_id=ACCESS_KEY,
            aws_secret_access_key=SECRET_KEY,
            endpoint_url=endpoint,
            use_ssl=False,
            verify=False)
bucket = s3.Bucket('bkt')

bucket.create()
obj = s3.Object(bucket_name='bkt', key='test.txt')

def get_body(response):
    body = response['Body']
    got = body.read()
    if type(got) is bytes:
        got = got.decode()
    return got

db = fdb.open()
class D4NFilterTestCase(unittest.case.TestCase):

    @classmethod
    def setUpClass(cls):
        print("D4NFilterTest setup.")

    def test_small_object(self):
        test_txt = 'test'
        
        response_put = obj.put(Body=test_txt)
        self.assertEqual(response_put.get('ResponseMetadata').get('HTTPStatusCode'), 200)
        
        # Local datacache
        bucketID = subprocess.check_output(['ls', '/home/samarahu/rgw_d4n_datacache/']).decode('latin-1').strip()

        datacache_path = '/home/samarahu/rgw_d4n_datacache/' + bucketID + '/test.txt/'
        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').strip().splitlines()

        datacache = datacache[2]

        output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
        self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5("test".encode('utf-8')).hexdigest())
        
        # Remote datacache
        bucketID = subprocess.check_output(['ls', '/home/samarahu/rgw_d4n_datacache_c2/']).decode('latin-1').strip()

        datacache_path = '/home/samarahu/rgw_d4n_datacache_c2/' + bucketID + '/test.txt/'
        datacache = subprocess.check_output(['ls', '-a', datacache_path])
        datacache = datacache.decode('latin-1').strip().splitlines()

        datacache = datacache[2]

        output = subprocess.check_output(['md5sum', datacache_path + datacache]).decode('latin-1')
        self.assertEqual(output.splitlines()[0].split()[0], hashlib.md5("test".encode('utf-8')).hexdigest())

        version = subprocess.check_output(['ls', datacache_path + datacache]).decode('latin-1').split('/')[6].split('#')[0]

        # Directory checks
        data = {}
        pattern = r"/0/4"
        for k, v in db.get_range(b"", b"\xff"): 
            if re.search(pattern, k.decode()):
                byte_fields = v.split(b'\x00')
                byte_fields = [x for x in byte_fields if x]
                hex_fields = [field.decode('utf-8', errors='ignore') for field in byte_fields] 
                fields = [re.sub(r'[^\x20-\x7e]|\\x[0-9a-fA-F]{2}', '', field) for field in hex_fields] 
                self.assertEqual(fields[fields.index('attrsCount') + 1], '0')
                self.assertEqual(fields[fields.index('blockID') + 1], '0')
                self.assertEqual(fields[fields.index('bucketName+') + 1], bucketID) # has a + in field name?
                self.assertEqual(fields[fields.index('deleteMarker') + 1], '0')
                self.assertEqual(fields[fields.index('dirty') + 1], '1')
                self.assertEqual(fields[fields.index('globalWeight') + 1], '0')
                self.assertEqual(fields[fields.index('hosts') + 1], 'http://127.0.0.1:8000')
                self.assertEqual(fields[fields.index('invalid') + 1], '0')
                self.assertEqual(fields[fields.index('objName') + 1], 'test.txt')
                self.assertEqual(fields[fields.index('size') + 1], '4')
                self.assertEqual(fields[fields.index('objSize') + 1], '0')
                self.assertEqual(fields[fields.index('version') + 1], version)
                """
                Empty fields:
                - acl
                - creationTime
                - displayName
                - etag
                - userId
                """

    @classmethod
    def tearDownClass(cls):
        print("D4NFilterTest teardown.")
        del db[b'':b'\xFF']

if __name__ == '__main__':

    unittest.main()

