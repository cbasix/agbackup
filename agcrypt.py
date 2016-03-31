import hashlib
from Crypto.Random import random
from Crypto.Cipher import AES
import struct


chunk_size = 8192


class AESCipher(object):

    def __init__(self, key):
        self.chunksize = 24 * 1024
        # self.chunksize = 4 * 1024
        self.key = hashlib.sha256(key.encode()).digest()
        # self.iv = hashlib.sha256(self.key).digest()[:AES.block_size]

    # def encrypt(self, raw):
    #     raw = self._pad(raw)
    #     iv = Random.new().read(AES.block_size)
    #     cipher = AES.new(self.key, AES.MODE_CBC, iv)
    #     return base64.b64encode(iv + cipher.encrypt(raw))
    #
    # def decrypt(self, enc):
    #     enc = base64.b64decode(enc)
    #     iv = enc[:AES.block_size]
    #     cipher = AES.new(self.key, AES.MODE_CBC, iv)
    #     return self._unpad(cipher.decrypt(enc[AES.block_size:])).decode('utf-8')
    #
    # def encrypt(self, in_file, out_file):
    #     cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
    #     while True:
    #         chunk = in_file.read(chunk_size)
    #         if len(chunk) == 0:
    #             break
    #         elif len(chunk) % 16 != 0:
    #             chunk += b' ' * (16 - len(chunk) % 16)
    #         out_file.write(cipher.encrypt(chunk))
    #
    #     in_file.seek(0)
    #     out_file.seek(0)
    #
    #     return # base64.b64encode(iv + cipher.encrypt(raw))
    #
    # def decrypt(self, in_file, out_file):
    #     # enc = base64.b64decode(enc)
    #     # iv = enc[:AES.block_size]
    #     cipher = AES.new(self.key, AES.MODE_CBC, self.iv)
    #
    #     while True:
    #         chunk = in_file.read(chunk_size)
    #         if len(chunk) == 0:
    #             break
    #         out_file.write(cipher.decrypt(chunk))
    #
    #     in_file.fseek(0)
    #     out_file.fseek(0)
    #
    #     return # self._unpad(

    def encrypt(self, in_file, out_file):
            """ Encrypts a file using AES (CBC mode) with the
                given key.

                key:
                    The encryption key - a string that must be
                    either 16, 24 or 32 bytes long. Longer keys
                    are more secure.

                in_filename:
                    Name of the input file

                out_filename:
                    If None, '<in_filename>.enc' will be used.

                chunksize:
                    Sets the size of the chunk which the function
                    uses to read and encrypt the file. Larger chunk
                    sizes can be faster for some files and machines.
                    chunksize must be divisible by 16.
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
        """ Decrypts a file using AES (CBC mode) with the
            given key. Parameters are similar to encrypt_file
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


    # def _pad(self, s):
    #     return s + (self.bs - len(s) % self.bs) * chr(self.bs - len(s) % self.bs)
    #
    # @staticmethod
    # def _unpad(s):
    #     return s[:-ord(s[len(s)-1:])]

