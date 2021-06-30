import unittest
from os.path import getsize
from typing import List, Tuple, Dict, Set

from file import TxtFile, CsvFile, JsonFile, PickleFile


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
        self.testClassJson = JsonFile("test.json")
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
        self.testClassJson.writeFile(self.testlist[:-1:])  # все кроме set
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
        self.testClassJson.writeFile(temples)
        self.assertEqual(temples, self.testClassJson.readFile())

        # Tuple
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        temples: Tuple = self.testlist[1]
        self.testClassJson.writeFile(temples)
        self.assertEqual(temples, self.testClassJson.readFile())

        # Dict
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        temples: Dict = {str(k): v for k, v in self.testlist[2].items()}  # все ключи должны быть типа str
        self.testClassJson.writeFile(temples)
        self.assertEqual(temples, self.testClassJson.readFile())

        # Set
        temples: Set = self.testlist[3]
        self.testClassJson.writeFile(temples, lang="ru")
        self.assertEqual({str(k) for k in temples}, self.testClassJson.readFile())

    def test_appendJsonListFile(self):
        # Првоекра дозаписи в файл разных структур данных
        # List

        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: List = self.testlist[0]
        self.testClassJson.writeFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers += tempers
        self.assertEqual(tempers, self.testClassJson.readFile())

        # Tuple
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: Tuple = self.testlist[1]
        self.testClassJson.writeFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers += tempers
        self.assertEqual(tempers, self.testClassJson.readFile())

        # Dict
        self.testClassJson.deleteFile()
        self.testClassJson.createFileIfDoesntExist()
        tempers: Dict = {str(k): v for k, v in self.testlist[2].items()}  # все ключи должны быть типа str
        self.testClassJson.writeFile(tempers)
        self.testClassJson.appendJsonListFile(tempers)
        tempers.update(tempers)
        self.assertEqual(tempers, self.testClassJson.readFile())

        # Set
        tempers: Set = self.testlist[3]
        self.testClassJson.writeFile(tempers, lang="ru")
        self.testClassJson.appendJsonListFile(tempers)
        tempers.update(tempers)
        self.assertEqual({str(k) for k in tempers}, self.testClassJson.readFile())

    # Этот метод запускаетсья ПОСЛЕ каждой функции теста
    def tearDown(self):
        pass

    def __del__(self):
        self.testClassJson.deleteFile()


class TestCsvFile(unittest.TestCase):

    def setUp(self):
        self.cvs_file = CsvFile("test.csv")

    def test_init_(self):
        # Реакция на некоректное им файла
        self.assertRaises(ValueError, CsvFile, "test.txt")

    def test_writeFile_and_readFile(self):
        # Проверка записи и чтения данных cvs файла
        self.cvs_file.writeFile(
            [[1, 23, 41, 5],
             [21, 233, 46, 35],
             [13, 233, 26, 45],
             [12, 213, 43, 56]], FlagDataConferToStr=True, header=("Даннык", "Data", "Числа", "Num"))

        #  Тест на чтение Cvs файла
        self.assertEqual(self.cvs_file.readFile(),
                         [['Даннык', 'Data', 'Числа', 'Num'], ['1', '23', '41', '5'], ['21', '233', '46', '35'],
                          ['13', '233', '26', '45'], ['12', '213', '43', '56']])

        #  Тест на чтение cvs файла с убранами заголовками
        self.assertEqual(self.cvs_file.readFile(miss_get_head=True),
                         [['1', '23', '41', '5'], ['21', '233', '46', '35'],
                          ['13', '233', '26', '45'], ['12', '213', '43', '56']])

        # Тест на личит чтнеия
        self.assertEqual(self.cvs_file.readFile(limit=2),
                         [['Даннык', 'Data', 'Числа', 'Num'], ['1', '23', '41', '5']])

        #  Тест на привышающий лимит чтения
        self.assertEqual(self.cvs_file.readFile(limit=1123),
                         [['Даннык', 'Data', 'Числа', 'Num'], ['1', '23', '41', '5'], ['21', '233', '46', '35'],
                          ['13', '233', '26', '45'], ['12', '213', '43', '56']])

        #  Тест на чтение в обратном порядке
        self.assertEqual(self.cvs_file.readFileRevers(),
                         [['12', '213', '43', '56'], ['13', '233', '26', '45'], ['21', '233', '46', '35'],
                          ['1', '23', '41', '5'], ['Даннык', 'Data', 'Числа', 'Num']])

        # Тест на лимит чтени в обратном порядке
        self.assertEqual(self.cvs_file.readFileRevers(limit=2), [['12', '213', '43', '56'], ['13', '233', '26', '45']])

        #  Тест на привышающий лимит чтения в обратном порядке
        self.assertEqual(self.cvs_file.readFileRevers(limit=111),
                         [['12', '213', '43', '56'], ['13', '233', '26', '45'], ['21', '233', '46', '35'],
                          ['1', '23', '41', '5'], ['Даннык', 'Data', 'Числа', 'Num']])

        self.cvs_file.deleteFile()

    def test_appendFile(self):
        # проверка дозаписи в файл
        self.cvs_file.deleteFile()

        # Провекра записи с флагом FlagDataConferToStr
        self.cvs_file.writeFile(
            [[1, 23, 41, 5],
             [21, 233, 46, 35],
             [13, 233, 26, 45],
             [12, 213, 43, 56]], FlagDataConferToStr=True, header=("Даннык", "Data", "Числа", "Num"))

        self.cvs_file.appendFile([['2323', '23233', '23']])

        self.assertEqual(self.cvs_file.readFile(),
                         [['Даннык', 'Data', 'Числа', 'Num'], ['1', '23', '41', '5'], ['21', '233', '46', '35'],
                          ['13', '233', '26', '45'], ['12', '213', '43', '56'], ['2323', '23233', '23']])

    def test_ordinary(self):
        # Тест записи Однмерного массива

        self.cvs_file.deleteFile()
        self.cvs_file.writeFile([123, 123, 222, 1, 312, 223, 2], FlagDataConferToStr=True)
        self.cvs_file.writeFile([123, 123, 222, 1, 2], FlagDataConferToStr=True)
        self.cvs_file.writeFile([123, 123, '222', 1], FlagDataConferToStr=True)
        self.cvs_file.writeFile([123, 222, 1, 2])

        self.cvs_file.appendFile([123, 123, 222, 1, 312, 223, 2], FlagDataConferToStr=True)
        self.cvs_file.appendFile([123, 123, 222, 1, 2], FlagDataConferToStr=True)
        self.cvs_file.appendFile([123, 123, '222', 1], FlagDataConferToStr=True)
        self.cvs_file.appendFile([123, 222, 1, 2])

        # Тест записи дмумерного массива
        self.cvs_file.deleteFile()
        self.cvs_file.writeFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, 222, 1, 3312, 223, 2],
             ], FlagDataConferToStr=True)
        self.cvs_file.writeFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, '222', 1, 3312, 223, 2],
             ], FlagDataConferToStr=True)

        self.cvs_file.writeFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, 222, 1, 3312, 223, 2],
             ])

        self.cvs_file.appendFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, 222, 1, 3312, 223, 2],
             ], FlagDataConferToStr=True)
        self.cvs_file.appendFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, '222', 1, 3312, 223, 2],
             ], FlagDataConferToStr=True)

        self.cvs_file.appendFile(
            [[123, 123, 222, 1, 312, 223, 2],
             [4123, 1233, 222, 1, 3312, 223, 2],
             ])

        # Тест Записи Float
        self.cvs_file.writeFile([123.12, 123.43, 222.2, 1.5, 31.2, 22.3, 2.5], FlagDataConferToStr=True)
        self.assertEqual(
            self.cvs_file.readFile(),
            [['123.12', '123.43', '222.2', '1.5', '31.2', '22.3', '2.5']])

        # Тест записи комберированно
        self.cvs_file.writeFile([12, 123.43, 'Hello Привет', '1.5', 31.2, 22.3, 2.5], FlagDataConferToStr=True),
        self.assertEqual(
            self.cvs_file.readFile(),
            [['12', '123.43', 'Hello Привет', '1.5', '31.2', '22.3', '2.5']])

        # Тест Записи Float
        self.cvs_file.writeFile([123.12, 123.43, 222.2, 1.5, 31.2, 22.3, 2.5]),
        self.assertEqual(self.cvs_file.readFile(),
                         [['123.12', '123.43', '222.2', '1.5', '31.2', '22.3', '2.5']])

        #
        # Тест записи комберированно
        self.cvs_file.writeFile([12, 123.43, 'Hello Привет', '1.5', 31.2, 22.3, 2.5]),
        self.assertEqual(self.cvs_file.readFile(),
                         [['12', '123.43', 'Hello Привет', '1.5', '31.2', '22.3', '2.5']])

    def __del__(self):
        self.cvs_file.deleteFile()


class TestPickleFile(unittest.TestCase):

    def setUp(self):
        self.name_file = "test_pickle.pkl"
        self.pk = PickleFile(self.name_file)
        self.pk.deleteFile()

    def test_writeFile_and_readFile(self):
        # Проверка записи данных
        test_data = [
            (1, 2, 3, 4),
            [12, 23, 221],
            ["1231", 12, (2, 22)],
            {213123, 123213},
            {'s1': '213'},
        ]

        for td in test_data:
            self.pk.writeFile(td)
            self.assertEqual(self.pk.readFile(), td)
            self.pk.deleteFile()


if __name__ == '__main__':
    unittest.main()
