import unittest

from redshift_unloader.credential import Credential


class TestCredential(unittest.TestCase):
    ACCESS_KEY_ID = 'test_access_key'
    SECRET_ACCESS_KEY = 'test_secret_key'

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_to_dict(self):
        credential = Credential(access_key_id=self.ACCESS_KEY_ID, secret_access_key=self.SECRET_ACCESS_KEY)

        expected = {
            "ACCESS_KEY_ID": self.ACCESS_KEY_ID,
            "SECRET_ACCESS_KEY": self.SECRET_ACCESS_KEY
        }

        self.assertEqual(credential.to_dict(), expected)
