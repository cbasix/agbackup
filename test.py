import unittest
from agcrypt import AESCipher
from agmain import Agbackup
import io
import tempfile
from datetime import datetime


class TestStringMethods(unittest.TestCase):

    def test_encryption(self):
        crypt = AESCipher('testkeyblubb')
        in_data = io.BytesIO(b'some initial binary data: \x00\x01')
        encr_data = io.BytesIO(b'')
        decr_data = io.BytesIO(b'')

        crypt.encrypt(in_data, encr_data)
        crypt.decrypt(encr_data, decr_data)

        self.assertNotEqual(in_data.getvalue(), encr_data.getvalue())
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

    def test_dict_latest(self):
        d = {
            "s2difjsodfjaposiejghasodf": {"date": 2, },
            "s1difjsodfjaposiejghasodf": {"date": 1, },
            "s4difjsodfjaposiejghasodf": {"date": 4, },
            "s3difjsodfjaposiejghasodf": {"date": 3, },
        }

        self.assertEqual("s4difjsodfjaposiejghasodf", Agbackup.get_latest_from_dict(d, 'date')[0])

    def test_dict_latest2(self):
        d = {
            "s2difjsodfjaposiejghasodf": {"date": datetime(2012,2,1,5,8),},
            "s1difjsodfjaposiejghasodf": {"date": datetime(2011,2,1,5,8),},
            "s4difjsodfjaposiejghasodf": {"date": datetime(2014,2,1,5,8),},
            "s3difjsodfjaposiejghasodf": {"date": datetime(2013,2,1,5,8),},
        }

        self.assertEqual("s4difjsodfjaposiejghasodf", Agbackup.get_latest_from_dict(d, 'date')[0])
        self.assertEqual({"date": datetime(2014,2,1,5,8),}, Agbackup.get_latest_from_dict(d, 'date')[1])



if __name__ == '__main__':
    unittest.main()
