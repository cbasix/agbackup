import unittest
from agcrypt import AESCipher
from agmain import Agbackup
import io
import tempfile


class TestStringMethods(unittest.TestCase):

    def test_encryption(self):
        crypt = AESCipher('testkeyblubb')
        in_data = io.BytesIO(b'some initial binary data: \x00\x01')
        encr_data = io.BytesIO(b'')
        decr_data = io.BytesIO(b'')

        crypt.encrypt(in_data, encr_data)
        crypt.decrypt(encr_data, decr_data)

        self.assertEqual(in_data.getvalue(), decr_data.getvalue())

    def test_tar(self):
        output_file = tempfile.TemporaryFile()

        Agbackup._make_tarfile(output_file=output_file, source_dir='important_folder')
        # output_file.close()

        # input_file = open('example.tar.gz', 'rb')
        output_file.seek(0)
        Agbackup._extract_tarfile(output_folder='important_extracted', source_file=output_file)

    def test_combined(self):
        output_file = tempfile.TemporaryFile()

        Agbackup._make_tarfile(output_file=output_file, source_dir='important_folder')
        # output_file.close()

        crypt = AESCipher('testkeyblubb')
        encr_data = tempfile.TemporaryFile()
        decr_data = tempfile.TemporaryFile()

        crypt.encrypt(output_file, encr_data)
        crypt.decrypt(encr_data, decr_data)

        # input_file = open('example.tar.gz', 'rb')
        decr_data.seek(0)
        Agbackup._extract_tarfile(output_folder='important_extracted', source_file=decr_data)



if __name__ == '__main__':
    unittest.main()
