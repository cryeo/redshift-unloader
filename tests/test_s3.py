import boto3
import unittest
import tempfile
import os

from moto import mock_s3

from redshift_unloader.s3 import S3
from redshift_unloader.credential import Credential


class TestS3(unittest.TestCase):
    ACCESS_KEY_ID = 'test_access_key'
    SECRET_ACCESS_KEY = 'test_secret_key'
    BUCKET = 'test_bucket'
    REGION = 'ap-northeast-1'

    def setUp(self):
        self.mock_s3 = mock_s3()
        self.mock_s3.start()

        self.mocked_session = boto3.session.Session(aws_access_key_id=self.ACCESS_KEY_ID,
                                                    aws_secret_access_key=self.SECRET_ACCESS_KEY,
                                                    region_name=self.REGION)
        self.mocked_s3 = self.mocked_session.resource('s3')
        self.mocked_s3.create_bucket(Bucket=self.BUCKET)
        self.mocked_bucket = self.mocked_s3.Bucket(self.BUCKET)

        self.mocked_bucket.Object('path/object1').put(Body='object1'.encode())
        self.mocked_bucket.Object('path/to/object2').put(Body='object2'.encode())
        self.mocked_bucket.Object('path/to/object3').put(Body='object3'.encode())
        self.mocked_bucket.Object('path/to/some/object4').put(Body='object4'.encode())

        self.credential = Credential(access_key_id=self.ACCESS_KEY_ID, secret_access_key=self.SECRET_ACCESS_KEY)
        self.s3 = S3(self.credential, bucket=self.BUCKET, region=self.REGION)

    def tearDown(self):
        self.mock_s3.stop()

    def test_uri(self):
        self.assertEqual(self.s3.uri('/path/to'), f's3://{self.BUCKET}/path/to')
        self.assertEqual(self.s3.uri('/path/to/'), f's3://{self.BUCKET}/path/to/')
        self.assertEqual(self.s3.uri('path/to'), f's3://{self.BUCKET}/path/to')
        self.assertEqual(self.s3.uri('path/to/'), f's3://{self.BUCKET}/path/to/')

    def test_list(self):
        self.assertListEqual(self.s3.list(''),
                             ['path/object1', 'path/to/object2', 'path/to/object3', 'path/to/some/object4'])
        self.assertListEqual(self.s3.list('path/to'), ['path/to/object2', 'path/to/object3', 'path/to/some/object4'])
        self.assertListEqual(self.s3.list('path/to/some/'), ['path/to/some/object4'])
        self.assertListEqual(self.s3.list('not/exist/path'), [])

    def test_delete(self):
        self.s3.delete(['path/to/object2', 'path/to/some/object4'])
        self.assertListEqual(self.s3.list(''), ['path/object1', 'path/to/object3'])

    def test_download(self):
        temp_file = os.path.join(tempfile.gettempdir(), next(tempfile._get_candidate_names()))
        self.s3.download(key='path/to/object2', filename=temp_file)
        with open(temp_file, 'r') as f:
            self.assertEqual(f.read(), 'object2')
