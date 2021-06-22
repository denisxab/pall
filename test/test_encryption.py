import unittest
from os import listdir

from encryption import AsynchronousEncryption, AsynchronousEncryptionFile


class TestAsynchronousEncryption(unittest.TestCase):
    # Этот метод запускаетсья ПЕРЕД каждой функции теста
    def setUp(self) -> None:
        self.key = "denis2000denis2000"
        self.Test_crypto = AsynchronousEncryption(self.key)
        self.data = (123123, "2132131231", "qdosQWDSAd1", "офЫВЙЦ123ASDf", 123.12312, -123, -12.123)

    def test_encodeAES_and_decodeAES(self):
        # Провекра коректности шифрования и дешивроания данных

        encode: list = []
        for d in self.data:
            encode.append(self.Test_crypto.encodeAES(d))

        res: list = []
        for e in encode:
            res.append(self.Test_crypto.decodeAES(e))

        for d, r in zip(self.data, res):
            self.assertEqual(str(d), r)

    def test_FalsePassword_decodeAES(self):
        # Проверка дешифрования на неправильный данные

        encode: list = [{"nonce": "123", "ciphertext": 123123, "tag": -123, "salt": "asideЫВsa"},
                        {"nonce": [123123], "ciphertext": 123123, "tag": -123, "salt": "asideЫВsa"},
                        {"nonce": 123123, "ciphertext": {123: 123}, "tag": -123, "salt": "asideЫВsa"}]

        # Проверка дешифрования на неправильынй тип входных данных
        for e in encode:
            self.assertRaises(TypeError, self.Test_crypto.decodeAES, e)

        # Проверка на неправильную длинну входных данных
        self.assertRaises(IndexError, self.Test_crypto.decodeAES,
                          {"nonce": 123123, "ciphertext": {123: 123}, "tag": -123})

        # Проверка дешифрования на неправильное заначение данные
        self.assertRaises(ValueError, self.Test_crypto.decodeAES,
                          {"nonce": bytes(10), "ciphertext": bytes(110), "tag": bytes(4), "salt": bytes(102)})


class TestAsynchronousEncryptionFile(unittest.TestCase):
    def setUp(self) -> None:
        self.test_key = "denis2000denis2000"
        self.name = "testCrypto.txt"

        self.Test_cryptoFile = AsynchronousEncryptionFile(self.name, self.test_key)
        self.data = (123123, "2132131231", "qdosQWDSAd1", "офЫВЙЦ123ASDf", 123.12312, -123, -12.123)
        self.testText = "2132131231"

    def test_encodeAES_and_saveFile_and_readJson_and_decodeAES(self):
        # Проверка одиноковой записи и чтения закодированных данных
        for d in self.data:
            self.Test_cryptoFile.encodeAES_and_writeFile(d)
            self.assertEqual(str(d), self.Test_cryptoFile.readJson_and_decodeAES())

    def test_readJson_and_decodeAES(self):
        # Проверка чтения закодированных данных с неправильным ключом
        self.Test_cryptoFile.encodeAES_and_writeFile(self.testText)
        errortoken = "22222222"
        neoclassic = AsynchronousEncryptionFile(self.name, errortoken)
        self.assertRaises(ValueError, neoclassic.readJson_and_decodeAES)

    def test_deleteFile(self):
        # Проверка удаленя всех закодированных файлов
        self.Test_cryptoFile.encodeAES_and_writeFile(str(self.data[0]))
        list_file: int = len(listdir(self.Test_cryptoFile._nonce.route()[:(-1 * len(self.name)) - 2:]))
        self.Test_cryptoFile.deleteFile()
        self.assertEqual(len(listdir(self.Test_cryptoFile._nonce.route()[:(-1 * len(self.name)) - 2:])), list_file - 4)

    def test_deleteFile_Empty(self):
        # Проверка удаления закодированных файлов если они не существуют
        res = True
        self.Test_cryptoFile.encodeAES_and_writeFile(str(self.data[0]))
        try:
            self.Test_cryptoFile.deleteFile()
            self.Test_cryptoFile.deleteFile()
            self.Test_cryptoFile.deleteFile()
        except FileNotFoundError:
            res = False
        self.assertEqual(res, True)

    def test_readJson_and_decodeAES_Empty(self):
        # Проверка чтения закодированных файлов если они не существуют
        self.assertRaises(ValueError, self.Test_cryptoFile.readJson_and_decodeAES)

    def test_encodeAES_and_appendFile(self):
        # Провекра дозаписи зашифрованных данных

        for d in self.data:
            self.Test_cryptoFile.encodeAES_and_writeFile(str(d))
            self.Test_cryptoFile.encodeAES_and_appendFile(str(d))
            tmp = str(d) + str(d)
            self.assertEqual(tmp, self.Test_cryptoFile.readJson_and_decodeAES())
            self.Test_cryptoFile.deleteFile()

    def __del__(self):
        self.Test_cryptoFile.deleteFile()


if __name__ == '__main__':
    unittest.main()
