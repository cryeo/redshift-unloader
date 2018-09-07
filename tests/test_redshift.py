import unittest

from unittest import mock
from mock import PropertyMock
from collections import namedtuple

from redshift_unloader.credential import Credential
from redshift_unloader.redshift import Redshift


class TestRedshift(unittest.TestCase):
    HOST = 'test.redshift.com'
    PORT = 5439
    USER = 'user'
    PASSWORD = 'password'
    DATABASE = 'database'
    ACCESS_KEY_ID = 'test_access_key'
    SECRET_ACCESS_KEY = 'test_secret_key'

    @mock.patch('psycopg2.connect')
    def setUp(self, mock_connect):
        self.mock_cursor = mock_connect.return_value.cursor.return_value

        self.credential = Credential(access_key_id=self.ACCESS_KEY_ID, secret_access_key=self.SECRET_ACCESS_KEY)
        self.redshift = Redshift(host=self.HOST, port=self.PORT, user=self.USER, password=self.PASSWORD,
                                 database=self.DATABASE, credential=self.credential)

    def tearDown(self):
        pass

    def test_get_columns(self):
        query = "SELECT * FROM some_table WHERE date_column >= '2018-01-01'"

        Column = namedtuple('Column', ('name'))

        description = PropertyMock()
        description.return_value = [Column(name='column1'), Column(name='column2')]

        type(self.mock_cursor).description = description

        actual = self.redshift.get_columns(query)
        expected = ['column1', 'column2']

        self.assertListEqual(actual, expected)
        self.mock_cursor.execute.assert_called_once()

    def test_unload(self):
        query = "SELECT * FROM some_table WHERE date_column >= '2018-01-01'"
        s3_uri = "s3://some-bucket/path/to/"

        self.redshift.unload(query, s3_uri)

        expected_query = " ".join([
            "UNLOAD ('SELECT * FROM some_table WHERE date_column >= \\'2018-01-01\\'')",
            "TO 's3://some-bucket/path/to/'",
            "ACCESS_KEY_ID 'test_access_key'",
            "SECRET_ACCESS_KEY 'test_secret_key'",
            "PARALLEL ON"
        ])

        self.mock_cursor.execute.assert_called_once_with(expected_query)

    def test__escaped_query(self):
        query = "SELECT * FROM some_table WHERE date_column >= '2018-01-01'"

        actual = self.redshift._Redshift__escaped_query(query)
        expected = "SELECT * FROM some_table WHERE date_column >= \\'2018-01-01\\'"

        self.assertEqual(actual, expected)

    def test__generate_get_columns_sql(self):
        query = "SELECT * FROM some_table"

        actual = self.redshift._Redshift__generate_get_columns_sql(query)
        expected = "WITH query AS (SELECT * FROM some_table) SELECT * FROM query LIMIT 0"

        self.assertEqual(actual, expected)

    def test__generate_unload_sql(self):
        query = "SELECT * FROM some_table"
        s3_uri = "s3://some-bucket/path/to/"
        options = {
            'MANIFEST': None,
            'DELIMITER': "','",
            'ENCRTYPED': None,
            'GZIP': None,
            'ADDQUOTES': None,
            'NULL': "''",
            'ESCAPE': None,
            'ALLOWOVERWRITE': None,
            'PARALLEL': 'ON',
            'MAXFILESIZE': '1GB'
        }

        actual = self.redshift._Redshift__generate_unload_sql(query, s3_uri, self.credential, options)
        expected = ' '.join([
            "UNLOAD ('SELECT * FROM some_table')",
            "TO 's3://some-bucket/path/to/'",
            "ACCESS_KEY_ID 'test_access_key'",
            "SECRET_ACCESS_KEY 'test_secret_key'",
            "MANIFEST",
            "DELIMITER ','",
            "ENCRTYPED",
            "GZIP",
            "ADDQUOTES",
            "NULL ''",
            "ESCAPE",
            "ALLOWOVERWRITE",
            "PARALLEL ON",
            "MAXFILESIZE 1GB"
        ])

        self.assertEqual(actual, expected)
