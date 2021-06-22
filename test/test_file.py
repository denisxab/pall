import unittest
from os.path import getsize
from typing import List, Tuple, Dict, Set
from file import TxtFile, Json

a = 1021


class TestFile(unittest.TestCase):

    # Этот метод запускаетсья ПЕРЕД каждой функции теста
    def setUp(self) -> None:
        self.nameFile = "test.txt"
        self.testClassFile = TxtFile(self.nameFile)
        self.testClassFile.deleteFile()
        self.testClassFile.createFileIfDoesntExist()

        # Данные для теста
        self.test_str: str = "ninja cjj,output На двух языках 1#1^23 !23№эЭ123'"

    def test_sizeFile(self):
        # Првоекра определение размера файла
        self.testClassFile.writeFile(self.test_str)
        self.assertEqual(self.testClassFile.sizeFile(), getsize(self.testClassFile.nameFile))

    def test_deleteFile_and_checkExistenceFile(self):
        # Проверка удаление файла
        self.assertEqual(self.testClassFile.checkExistenceFile(), True)
        self.testClassFile.deleteFile()
        self.assertEqual(self.testClassFile.checkExistenceFile(), False)

    def test_writeFile(self):
        # Првоекра записи в файл
        self.testClassFile.writeFile(self.test_str)
        self.assertEqual(self.test_str, self.testClassFile.readFile())

    def test_appendFile(self):
        # Проверка дозaписи в файл
        test_str: str = self.test_str
        self.testClassFile.writeFile(test_str)
        self.testClassFile.appendFile(test_str)
        test_str += test_str
        self.assertEqual(test_str, self.testClassFile.readFile())

    def test_readBinaryFile_and_writeBinaryFile(self):
        # Проверкп записи и чтения в двоичном режиме
        self.testClassFile.writeBinaryFile(self.test_str.encode())
        self.assertEqual(self.test_str.encode(), self.testClassFile.readBinaryFile())

    def test_appendBinaryFile(self):
        # Проверка дозаписи в двоичном режиме
        tests: str = self.test_str
        self.testClassFile.writeBinaryFile(tests.encode())
        self.testClassFile.appendBinaryFile(tests.encode())
        tests += tests
        self.assertEqual(tests.encode(), self.testClassFile.readBinaryFile())

    # Этот метод запускаетсья ПОСЛЕ каждой функции теста
    def tearDown(self):
        pass

    def __del__(self):
        self.testClassFile.deleteFile()


class TestJson(unittest.TestCase):

    # Этот метод запускаетсья ПЕРЕД каждой функции теста
    def setUp(self) -> None:
        self.testClassJson = Json("test.json")
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()

        # Данные для теста
        self.testlist: Tuple[List, Tuple, Dict, Set] = (
            [1, 2.1, -1, -2.1, "1", "\t", "Qwe", "Фыв"],
            (1, 2.1, -1, -2.1, "1", "\t", "Qwe", "Фыв"),
            # все ключи словаря переводяться в тип строки
            {12: 2, 1: 1, 1.2: 1.3, 13: 1.2, 4.2: 1, -12: 1, 41: -23, -23.1: -2.2, -232.2: 1,
             "Qwe": 1, 15: "Qwe", -21: "Qwe", 12.3: "DewW", -11: "wasd", "quests": -123},
            {1, 2.1, -1, -2.1, "1", "\t", "Qwe", "Фыв"}
        )

    def test_sizeFile(self):
        # Првоекра определение размера файла
        self.testClassJson.writeJsonFile(self.testlist[:-1:])  # все кроме set
        self.assertEqual(self.testClassJson.sizeFile(), getsize(self.testClassJson.nameFile))

    def test_deleteFile_and_checkExistenceFile(self):
        # Проверка удаление файла
        self.assertEqual(self.testClassJson.checkExistenceFile(), True)
        self.testClassJson.deleteFile()
        self.assertEqual(self.testClassJson.checkExistenceFile(), False)

    def test_writeJsonFile_and_readJsonFile(self):
        # Првоекра записи в файл разных структур данных
        # List
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        temples: List = self.testlist[0]
        self.testClassJson.writeJsonFile(temples)
        self.assertEqual(temples, self.testClassJson.readJsonFile())

        # Tuple
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        temples: Tuple = self.testlist[1]
        self.testClassJson.writeJsonFile(temples)
        self.assertEqual(temples, self.testClassJson.readJsonFile())

        # Dict
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        temples: Dict = {str(k): v for k, v in self.testlist[2].items()}  # все ключи должны быть типа str
        self.testClassJson.writeJsonFile(temples)
        self.assertEqual(temples, self.testClassJson.readJsonFile())

        # Set
        temples: Set = self.testlist[3]
        self.testClassJson.writeJsonFile(temples, lang="ru")
        self.assertEqual({str(k) for k in temples}, self.testClassJson.readJsonFile())

    def test_appendJsonListFile(self):
        # Првоекра дозаписи в файл разных структур данных
        # List

        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: List = self.testlist[0]
        self.testClassJson.writeJsonFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers += tempers
        self.assertEqual(tempers, self.testClassJson.readJsonFile())

        # Tuple
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: Tuple = self.testlist[1]
        self.testClassJson.writeJsonFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers += tempers
        self.assertEqual(tempers, self.testClassJson.readJsonFile())

        # Dict
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: Dict = {str(k): v for k, v in self.testlist[2].items()}  # все ключи должны быть типа str
        self.testClassJson.writeJsonFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers.update(tempers)
        self.assertEqual(tempers, self.testClassJson.readJsonFile())

        # Set
        tempers: Set = self.testlist[3]
        self.testClassJson.writeJsonFile(tempers, lang="ru")
        self.testClassJson.appendJsonListFile(tempers)
        tempers.update(tempers)
        self.assertEqual({str(k) for k in tempers}, self.testClassJson.readJsonFile())

    # Этот метод запускаетсья ПОСЛЕ каждой функции теста
    def tearDown(self):
        pass

    def __del__(self):
        self.testClassJson.deleteFile()


if __name__ == '__main__':
    unittest.main()

# self.assertRaises(TypeError,self.testClass.sums,"123123")
# self.assertRaises(TypeError, self.testClass.sums, [123])
