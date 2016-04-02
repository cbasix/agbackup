import hashlib
from Crypto.Random import random
from Crypto.Cipher import AES
import struct


class AESCipher(object):

    def __init__(self, key):
        self.chunksize = 24 * 1024
        self.key = hashlib.sha256(key.encode()).digest()

    def encrypt(self, in_file, out_file):
            """ Encrypts a file using AES/CBC using the
                given key.

                key:
                    The encryption key - a string that must be
                    either 16, 24 or 32 bytes long. Longer keys
                    are more secure.

                in_file:
                    The input file

                out_file:
                    The file to write to.

            """

            # iv = b''.join(bytes(random.randint(0, 0xFF)) for i in range(16))
            iv = b''+random.getrandbits(16*8).to_bytes(16, byteorder='big')
            encryptor = AES.new(self.key, AES.MODE_CBC, iv)
            # get file size
            in_file.seek(0, 2)
            filesize = in_file.tell()  # os.path.getsize(in_filename)
            in_file.seek(0)

            out_file.write(struct.pack('<Q', filesize))
            out_file.write(iv)

            while True:
                chunk = in_file.read(self.chunksize)
                if len(chunk) == 0:
                    break
                elif len(chunk) % 16 != 0:
                    chunk += b'\x00' * (16 - len(chunk) % 16)

                out_file.write(encryptor.encrypt(chunk))

    def decrypt(self, in_file, out_file):
        """ Decrypts a file using AES/CBC using the
            given key. Parameters are similar to encrypt
        """
        in_file.seek(0)
        origsize = struct.unpack('<Q', in_file.read(struct.calcsize('<Q')))[0]
        iv = in_file.read(16)
        decryptor = AES.new(self.key, AES.MODE_CBC, iv)


        while True:
            chunk = in_file.read(self.chunksize)
            if len(chunk) == 0:
                break
            out_file.write(decryptor.decrypt(chunk))

        out_file.truncate(origsize)
        out_file.flush()

