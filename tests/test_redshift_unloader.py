import unittest

from unittest import mock
from mock import call

from redshift_unloader import RedshiftUnloader


class TestRedshiftUnloader(unittest.TestCase):
    HOST = 'test.redshift.com'
    PORT = 5439
    USER = 'user'
    PASSWORD = 'password'
    DATABASE = 'database'
    S3_BUCKET = 'test_bucket'
    REGION = 'ap-northeast-1'
    ACCESS_KEY_ID = 'test_access_key'
    SECRET_ACCESS_KEY = 'test_secret_key'

    @mock.patch('redshift_unloader.redshift_unloader.S3')
    @mock.patch('redshift_unloader.redshift_unloader.Redshift')
    def setUp(self, mock_redshift, mock_s3):
        self.redshift = mock_redshift.return_value
        self.s3 = mock_s3.return_value

        self.unloader = RedshiftUnloader(host=self.HOST, port=self.PORT, user=self.USER, password=self.PASSWORD,
                                         database=self.DATABASE, s3_bucket=self.S3_BUCKET,
                                         access_key_id=self.ACCESS_KEY_ID, secret_access_key=self.SECRET_ACCESS_KEY,
                                         region=self.REGION)

    def tearDown(self):
        pass

    @mock.patch('builtins.open')
    @mock.patch('shutil.copyfileobj')
    @mock.patch('shutil.rmtree')
    @mock.patch('os.mkdir')
    @mock.patch('tempfile.gettempdir')
    def test_unload(self, mock_gettempdir, mock_mkdir, mock_rmtree, mock_copyfileobj, mock_open):
        temp_dir = '/tmp'
        mock_gettempdir.return_value = temp_dir

        query = 'some_query'
        filename = '/path/to/output'

        session_id = 'session_id'

        s3_path = "tmp/redshift-unloader/session_id/"
        local_path = f"{temp_dir}/{session_id}"

        s3_uri = f's3://bucket/{s3_path}'
        objects = ['object1', 'object2']

        s3_keys = list(map(lambda x: f'{s3_path}{x}', objects))
        local_files = list(map(lambda x: f'{local_path}/{x}', objects))

        with mock.patch.object(self.unloader,
                               '_RedshiftUnloader__generate_session_id',
                               return_value=session_id):
            self.redshift.get_columns.return_value = ['column1', 'column2']
            self.s3.list.return_value = s3_keys
            self.s3.uri.side_effect = lambda x: f's3://bucket{x}'

            self.unloader.unload(query, filename)

            self.unloader._RedshiftUnloader__redshift.get_columns.assert_called_once_with(query)

            self.unloader._RedshiftUnloader__s3.uri.assert_called_once_with(f"/{s3_path}")
            self.unloader._RedshiftUnloader__redshift.unload.assert_called_once_with(query, s3_uri, gzip=True,
                                                                                     parallel=True,
                                                                                     delimiter=',',
                                                                                     null_string='',
                                                                                     add_quotes=True,
                                                                                     escape=True,
                                                                                     allow_overwrite=True)

            self.unloader._RedshiftUnloader__s3.list.assert_called_once_with(s3_path)
            mock_mkdir.assert_called_once_with(local_path, 0o700)

            self.assertEqual(self.unloader._RedshiftUnloader__s3.download.call_count, len(s3_keys))

            self.assertEqual(mock_open.call_count, len(s3_keys) + 1)
            self.assertListEqual(mock_open.call_args_list,
                                 [call(filename, 'wb')] + list(map(lambda x: call(x, 'rb'), local_files)))
            self.assertEqual(mock_copyfileobj.call_count, len(s3_keys))

            self.unloader._RedshiftUnloader__s3.delete.assert_called_once_with(s3_keys)

            mock_rmtree.asset_called_once_with(local_path)

    @mock.patch('uuid.uuid4')
    def test__generate_session_id(self, mock_uuid4):
        self.unloader._RedshiftUnloader__generate_session_id()

        mock_uuid4.assert_called_once()

    def test__generate_path(self):
        method = self.unloader._RedshiftUnloader__generate_path

        self.assertEqual(method('/tmp/path/to', 'session_id'), '/tmp/path/to/session_id')
        self.assertEqual(method('/tmp/path/to/', 'session_id'), '/tmp/path/to/session_id')
        self.assertEqual(method('tmp/path/to', 'session_id'), 'tmp/path/to/session_id')
        self.assertEqual(method('tmp/path/to/', 'session_id'), 'tmp/path/to/session_id')
        self.assertEqual(method('/tmp/path/to', 'session_id', '/'), '/tmp/path/to/session_id/')
        self.assertEqual(method('/tmp/path/to/', 'session_id', '/'), '/tmp/path/to/session_id/')
