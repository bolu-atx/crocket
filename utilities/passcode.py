from base64 import b64decode, b64encode
from hashlib import sha256
from Crypto import Random
from Crypto.Cipher import AES


class AESCipher(object):
    def __init__(self, key):
        self.block_size = 32
        self.key = sha256(key.encode()).digest()

    def encrypt(self, passcode):
        """
        Encrypt passcode with key.
        Arguments:
            passcode (str):
        """

        padded_passcode = self._pad(passcode)

        iv = Random.new().read(AES.block_size)
        cipher = AES.new(self.key, AES.MODE_CBC, iv)

        return b64encode(iv + cipher.encrypt(padded_passcode))

    def decrypt(self, encrypted_passcode):
        """
        Decrypt encrypted passcode with key.
        Arguments:
            encrypted_passcode (str):
        """

        decoded_encrypted_passcode = b64decode(encrypted_passcode)

        iv = decoded_encrypted_passcode[:AES.block_size]
        cipher = AES.new(self.key, AES.MODE_CBC, iv)

        return self._unpad(cipher.decrypt(decoded_encrypted_passcode[AES.block_size:])).decode('utf-8')

    def _pad(self, decrypted):
        """
        Pad passcode.
        """

        return decrypted + (self.block_size - len(decrypted) % self.block_size) * chr(
            self.block_size - len(decrypted) % self.block_size)

    @staticmethod
    def _unpad(decrypted):
        """
        Unpad passcode.
        """

        return decrypted[:-ord(decrypted[len(decrypted) - 1:])]
