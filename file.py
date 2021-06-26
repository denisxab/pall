import csv
import os
from json import dump, load
from os import remove
from os.path import getsize, abspath, exists
from typing import List, Tuple, Dict, Set, Union


class File:
    """
    )Утилиты
    - удаление файла
    - проверка существаования файла
    - проверка существаования файла и если его нет то создание
    - путь к файлу
    - размер файла
    - проверка разрешения открытия файла
    """

    def __init__(self, nameFile: str):
        self.nameFile: str = nameFile
        self.createFileIfDoesntExist()

    def createFileIfDoesntExist(self):  # +
        # Создать файл если его нет
        if not exists(self.nameFile):
            open(self.nameFile, "w+").close()

    def checkExistenceFile(self) -> bool:  # +
        # Проверить существование файла
        return True if exists(self.nameFile) else False

    def deleteFile(self):  # +
        # Удаление файла
        if self.checkExistenceFile():
            remove(self.route())

    def sizeFile(self) -> int:  # +
        # Размер файла в байтах
        return getsize(self.nameFile)

    def route(self) -> str:  # +
        # Путь к файлу
        return abspath(self.nameFile)


class CsvFile(File):
    def __init__(self, nameFile: str):
        tmp = nameFile.split(".")
        if any((len(tmp) != 2, tmp[1] != "csv")):
            raise ValueError("Файл должен иметь разшерение .csv")

        File.__init__(self, nameFile)

    def readFile(self,
                 encoding: str = "utf-8",
                 newline: str = "",
                 limit: int = None,
                 miss_get_head=False
                 ) -> List[List[str]]:  # +
        """
        :param limit: ограничения чтения строк
        :param miss_get_head: # Пропустить чтение заголвка
        :param encoding: open()
        :param newline: open()
        :return:
        """
        res = []
        with open(self.nameFile, "r", encoding=encoding, newline=newline) as f:
            if limit:  # Лимит чтения строк
                reader = csv.reader(f)
                try:
                    for row in range(limit):
                        res.append(reader.__next__())
                except StopIteration:
                    pass
            else:
                res = list(csv.reader(f))

            if miss_get_head:  # Провустить заголовок
                return res[1::]
            return res

    def readFileRevers(self,
                       limit: int = None,
                       encoding: str = "utf-8",
                       newline: str = ""
                       ) -> List[List[str]]:
        def reversed_lines(file):
            "Generate the lines of file in reverse order."
            part = ''
            for block in reversed_blocks(file):
                for c in reversed(block):
                    if c == '\n' and part:
                        yield part[::-1]
                        part = ''
                    part += c
            if part: yield part[::-1]

        def reversed_blocks(file, blocksize=4096):
            "Generate blocks of file's contents in reverse order."
            file.seek(0, os.SEEK_END)
            here = file.tell()
            while 0 < here:
                delta = min(blocksize, here)
                here -= delta
                file.seek(here, os.SEEK_SET)
                yield file.read(delta)

        res = []
        with open(self.nameFile, "r", encoding=encoding, newline=newline) as f:

            if limit:  # Лимит чтения строк
                for row in csv.reader(reversed_lines(f)):
                    if limit:
                        res.append(row)
                        limit -= 1
                    else:
                        break
            else:
                for row in csv.reader(reversed_lines(f)):
                    res.append(row)
        return res

    def writeFile(self, data: List[List[Union[str, int]]],
                  header: tuple = None,
                  FlagDataConferToStr: bool = False,
                  encoding: str = "utf-8",
                  newline: str = ""
                  ):  # +
        """
        :param data:
        :param header: Эти даныне будут заголовками
        :param FlagDataConferToStr: Первеодит все данные в формат str
        :param encoding: open()
        :param newline: open()
        """
        with open(self.nameFile, "w", encoding=encoding, newline=newline) as f:
            writer = csv.writer(f)
            if header:  # Запись заголовка
                writer.writerow(header)
            if FlagDataConferToStr:
                data = [[str(n) for n in m] for m in data]
            writer.writerows(data)

    def appendFile(self, data: List[List[Union[str, int]]],
                   FlagDataConferToStr: bool = False,
                   encoding: str = "utf-8",
                   newline: str = ""
                   ):  # +
        with open(self.nameFile, "a", encoding=encoding, newline=newline) as f:
            writer = csv.writer(f)
            if FlagDataConferToStr:
                data = [[str(n) for n in m] for m in data]
            writer.writerows(data)


class TxtFile(File):
    """
    )Открытьвать текстового файла в текстовом и БИНАРНОМ виде на
    - чтение
    - запись
    - дозапись стандартную
    """

    def __init__(self, nameFile: str):
        tmp = nameFile.split(".")
        if any((len(tmp) != 2, tmp[1] != "txt")):
            raise ValueError("Файл должен иметь разшерение .txt")

        File.__init__(self, nameFile)

    def readFile(self) -> str:  # +
        with open(self.nameFile, "r") as f:
            return f.read()

    def readBinaryFile(self) -> bytes:  # +
        with open(self.nameFile, "rb") as f:
            return f.read()

    def writeFile(self, data: str):  # +
        with open(self.nameFile, "w") as f:
            f.write(data)

    def writeBinaryFile(self, data: Union[bytes, memoryview]):  # +
        with open(self.nameFile, "wb") as f:
            f.write(data)

    def appendFile(self, data: str):  # +
        with open(self.nameFile, "a") as f:
            f.write(data)

    def appendBinaryFile(self, data: bytes):  # +
        with open(self.nameFile, "ab") as f:
            f.write(data)


class JsonFile(File):
    """
    )Открывать json файлы на
    - чтение
    - запись
    - дозапись массива
    """

    def __init__(self, nameFile: str):
        tmp = nameFile.split(".")
        if len(tmp) != 2 or tmp[1] != "json":
            raise ValueError("Файл должен иметь разшерение .json")

        File.__init__(self, nameFile)

    def readJsonFile(self) -> Union[List, Tuple, Dict, Set]:  # +
        with open(self.nameFile, "r") as read_file:
            tmp = load(read_file)
            if tmp[0] == "any":
                return tmp[1]
            elif tmp[0] == "set":
                return set(tmp[1].keys())
            elif tmp[0] == "tuple":
                return tuple(tmp[1])

    def writeJsonFile(self, data: Union[List, Tuple, Dict, Set], lang: str = "en"):  # +
        fun_en = lambda data_lambda: dump(data_lambda, write_file)
        fun_ru = lambda data_lambda: dump(data_lambda, write_file, ensure_ascii=False)

        if type(data) == tuple:
            # Запись Tuple в виде List
            with open(self.nameFile, "w") as write_file:
                if lang == "en":
                    fun_en(["tuple", data])
                elif lang == "ru":
                    fun_ru(["tuple", data])

        elif type(data) == set:
            # Запись Set в виде Dict
            with open(self.nameFile, "w") as write_file:
                if lang == "en":
                    fun_en(["set", {d: '' for d in data}])
                elif lang == "ru":
                    fun_ru(["set", {d: '' for d in data}])
        else:
            # List,Dict
            with open(self.nameFile, "w") as write_file:
                if lang == "en":
                    fun_en(["any", data])
                elif lang == "ru":
                    fun_ru(["any", data])

    def appendJsonListFile(self, data: Union[List, Tuple, Dict, Set], lang: str = "en"):  # +
        tmp_data = self.readJsonFile()
        if type(data) == type(tmp_data):
            if type(data) == tuple or type(data) == list:
                # Tuple List
                self.writeJsonFile(tmp_data + data, lang)

            elif type(data) == set or type(data) == dict:
                # Dict Set
                tmp_data.update(data)
                self.writeJsonFile(tmp_data, lang)
        else:
            raise TypeError("Тип даннных в файле и тип входных данных раличны")



if __name__ == '__main__':
    pass
