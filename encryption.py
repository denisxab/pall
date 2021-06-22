#########################################
from hashlib import scrypt

from Cryptodome.Cipher import AES
from Cryptodome.Random import get_random_bytes

from file import TxtFile


class AsynchronousEncryption:
    """
    ) Шифрование строк
    ) Дешифрование строк

    nonce: bytes  # Нужен для дешифровки
    tag: bytes  # Проверка подлинности
    ciphertext: bytes  # Зашифрованные данные
    """

    def __init__(self, keys: str):
        self.keys = keys

    def encodeAES(self, text: str) -> dict:  # +
        if type(text) != str:
            text = str(text)
        salt = get_random_bytes(AES.block_size)
        private_key = scrypt(self.keys.encode(), salt=salt, n=2 ** 14, r=8, p=1, dklen=32)
        cipher = AES.new(private_key, AES.MODE_EAX)
        nonce = cipher.nonce  # Ключ
        ciphertext, tag = cipher.encrypt_and_digest(text.encode("utf-8"))
        return {"nonce": nonce, "ciphertext": ciphertext, "tag": tag, "salt": salt}

    def decodeAES(self, enc_dict: dict) -> str:  # +
        if len(enc_dict.items()) == 4:
            for x in enc_dict.values():
                if type(x) != bytes:
                    raise TypeError("Словать должен иметь тип значений bytes")
        else:
            raise IndexError("Словать должен иметь размер 4")

        private_key = scrypt(self.keys.encode(), salt=enc_dict["salt"], n=2 ** 14, r=8, p=1, dklen=32)
        cipher = AES.new(private_key, AES.MODE_EAX, nonce=enc_dict["nonce"])
        try:
            plaintext = cipher.decrypt_and_verify(enc_dict["ciphertext"], enc_dict["tag"])
            return plaintext.decode("utf-8")
        except ValueError:
            raise ValueError("Неверный ключ")


class AsynchronousEncryptionFile:
    """
    зависимость (AsynchronousEncryption,TxtFile)
    - сохранить в файл закодированные данные по ключу
    - считать из файла закодированные данные по ключу
    """

    def __init__(self, nameFile: str, keys: str):
        self.nameFile = nameFile
        self.keys = keys

        self.AsE = AsynchronousEncryption(self.keys)
        self._nonce = TxtFile("0{}".format(nameFile))
        self._ciphertext = TxtFile("1{}".format(nameFile))
        self._tag = TxtFile("2{}".format(nameFile))
        self._salt = TxtFile("3{}".format(nameFile))

    def encodeAES_and_writeFile(self, text: str):  # +
        # кодирование и запись
        res = self.AsE.encodeAES(text)
        self._nonce.writeBinaryFile(res["nonce"])
        self._ciphertext.writeBinaryFile(res["ciphertext"])
        self._tag.writeBinaryFile(res["tag"])
        self._salt.writeBinaryFile(res["salt"])

    def readJson_and_decodeAES(self) -> str:  # +
        # Цтение и декодирование
        return self.AsE.decodeAES(
            {"nonce": self._nonce.readBinaryFile(), "ciphertext": self._ciphertext.readBinaryFile(),
             "tag": self._tag.readBinaryFile(), "salt": self._salt.readBinaryFile()})

    def encodeAES_and_appendFile(self, text: str):
        res = self.readJson_and_decodeAES()
        text = res + text
        # кодирование и дозапись
        res = self.AsE.encodeAES(text)
        self._nonce.writeBinaryFile(res["nonce"])
        self._ciphertext.writeBinaryFile(res["ciphertext"])
        self._tag.writeBinaryFile(res["tag"])
        self._salt.writeBinaryFile(res["salt"])

    def deleteFile(self):
        self._nonce.deleteFile()
        self._ciphertext.deleteFile()
        self._tag.deleteFile()
        self._salt.deleteFile()


if __name__ == '__main__':
    key = "denis2000denis2000"

    crypt = AsynchronousEncryptionFile("my.txt", key)
    crypt.encodeAES_and_writeFile("Раз два Три")

    ncrypt = AsynchronousEncryptionFile("my.txt", key)
    print(ncrypt.readJson_and_decodeAES())
    ncrypt.deleteFile()

    # Шифрование
    # tmpE = AsynchronousEncryption(key)
    # enс = tmpE.encodeAES("Привет миры")
    # # Дешифрование
    # tmpD = AsynchronousEncryption("denis2000denis2000")
    # res = tmpD.decodeAES(enс)
    # print(res)
