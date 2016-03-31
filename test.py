import unittest
from agcrypt import AESCipher
import io


class TestStringMethods(unittest.TestCase):

    def test_encryption(self):
        crypt = AESCipher('testkeyblubb')
        in_data = io.BytesIO(b'some initial binary data: \x00\x01')
        encr_data = io.BytesIO(b'')
        decr_data = io.BytesIO(b'')

        crypt.encrypt(in_data, encr_data)
        crypt.decrypt(encr_data, decr_data)

        self.assertEqual(in_data.getvalue(), decr_data.getvalue())


if __name__ == '__main__':
    unittest.main()
