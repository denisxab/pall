import sqlite3
from os import remove
from os.path import exists, abspath
from typing import List, Tuple, Dict, Union


class SqlLite:
    """
    - Запись данных в БД
    - Чтение данных Из БД

    NULL — значение NULL
    INTEGER — числовые значения. Целые числа хранятся в 1, 2, 3, 4, 6 и 8 байтах в зависимости от величины
    REAL — числа с плавающей точкой, например, 3.14, число Пи
    TEXT — текстовые значения. Могут храниться в кодировке UTF-8, UTF-16BE или UTF-16LE
    BLOB — бинарные данные. Для хранения изображений и файлов

    """

    def __init__(self, name_db: str) -> None:  # +
        self.connection = None
        # Проверка того что разшерение db
        tmp = name_db.split(".")
        if len(tmp) != 2 or tmp[1] != "db":
            raise NameError("Файл должен иметь разшерение .db")

        self.name_db = name_db
        self.connection = sqlite3.connect(self.name_db)
        self.cursor = self.connection.cursor()
        self.header_db: dict = dict()  # Тут храниться типы столбцов таблциы

    def __del__(self):  # +
        if self.connection:
            self.SaveDb()
            self.connection.close()
            self.connection = None

    def SaveDb(self):  # +
        # Запись изменений курсора в БД
        self.connection.commit()

    def DeleteTable(self, name_table: str):  # +
        # Удалить таблицу если она существует
        self.cursor.execute("DROP TABLE IF EXISTS {0};".format(name_table))
        self.header_db = dict()

    def DeleteDb(self):  # +
        self.__del__()
        if exists(self.name_db):
            remove(abspath(self.name_db))

    def CreateTable(self, name_table: str, data: Union[str, Dict]):  # +

        # Конвертация типов в str
        res: str = ""
        if type(data) == str:
            res = data
            # Конвертировать SQL запросв в header_db
            for d in res[1:-1:].split(","):

                k, v = d.strip().split(" ")
                if v in ["text", "TEXT"]:
                    v = str
                elif v in ["INTEGER", "integer"]:
                    v = int
                elif v in ["REAL", "real"]:
                    v = float
                elif v in ["BLOB", "blob"]:
                    v = bytes
                elif v in ["NULL", "null"]:
                    v = None
                else:
                    raise TypeError(
                        "Указан не верный тип данных выбирете TEXT;INTEGER;REAL;BLOB;NULL\n{0}:{1}".format(k, v))
                self.header_db[k] = v

        elif type(data) == dict:
            # Конвертация словаря в SQL запрос
            res += "("
            for k, v in data.items():
                res += str(k)
                if v == str:
                    res += " TEXT"
                elif v == int:
                    res += " INTEGER"
                elif v == float:
                    res += " REAL"
                elif v == bytes:
                    res += " BLOB"
                elif v is None:
                    res += " NULL"
                else:
                    raise TypeError(
                        "Указан не верный тип данных выбирете str;int;float;None;bytes\n{0}:{1}".format(k, v))
                res += ', '

            res = res[:-2:]
            res += ");"
            self.header_db = data

        # Создание таблицы
        self.cursor.execute("CREATE TABLE {0} {1}".format(name_table, res))

    def ExecuteDb(self, name_table: str, data: Union[List, Tuple, str]):  # +

        res: str = ""
        if type(data) == str:
            res = data

        # Конвертация типов в str
        elif type(data) in [tuple, list]:

            if len(data) != len(self.header_db):
                raise IndexError("Разное колличество столбцов таблицы и входных данных")

            res += "("
            for t in data:
                res += r"'{0}', ".format(t)

            res = res[:-2:]
            res += ");"

        else:
            raise TypeError("Допустимые типы List, Tuple, str")

        # Запись в курсор
        self.cursor.execute("INSERT INTO {0} VALUES {1}".format(name_table, res))

    def GetDb(self, name_table: str) -> list:  # +
        # Получить данные из таблицы
        return [row for row in self.cursor.execute('SELECT * FROM {0}'.format(name_table))]


if __name__ == '__main__':
    pass
    # name_db = 'example.db'
    # name_table = "stocks"
    # sq = SqlLite(name_db)
    #
    # sq.DeleteTable(name_table)
    # sq.CreateTable(name_table, {"name_file":str,"size_file":int})
    #
    # for nf in listdir("."):
    #     if len(nf.split(".")) == 2 and nf.split(".")[1] == "py":
    #         sq.ExecuteDb(name_table,(nf, getsize(nf)))
    #
    # print(sq.GetDb(name_table))
