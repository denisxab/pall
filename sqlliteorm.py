import sqlite3
from os import remove
from os.path import exists, abspath
from typing import List, Tuple, Dict, Union


class SqlName:
    PK: str = "PRIMARY KEY"  # Должны содержать ункальные значения
    NN: str = "NOT NULL"  # Все столбцы по умолчанию будт заполнены Null
    NND = lambda default: "NOT NULL DEFAULT {0}".format(
        default)  # Все столбцы будут по умолчанию заполнены указанными значениями
    IDAUTO: str = "PRIMARY KEY AUTOINCREMENT"  # Авто заполение строки. подходит для id


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
                if v in ["TEXT", "text"]:
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
            count_primary_key = False
            # Конвертация словаря в SQL запрос
            res += "("
            for k, v in data.items():
                res += str(k)

                if type(v) == tuple and len(v) == 2:  # Комбенированная запись с параметрами столбца

                    if v[1] == SqlName.PK or v[1] == SqlName.IDAUTO:  # Проверка уникальности Primary Key в таблицы
                        if not count_primary_key:
                            count_primary_key = True
                        else:
                            raise LookupError("{0} имеет больше одного primary key".format(name_table))

                    tmp = " {0}".format(v[1])
                    values = v[0]
                else:  # Обыная запись столбца
                    tmp = ''
                    values = v

                if values == str:
                    res += " TEXT"
                elif values == int:
                    res += " INTEGER"
                elif values == float:
                    res += " REAL"
                elif values == bytes:
                    res += " BLOB"
                elif values is None:
                    res += " NULL"
                else:
                    raise TypeError(
                        "Указан не верный тип данных выбирете str;int;float;None;bytes\n{0}:{1}".format(k, v))

                res += "{0}{1}".format(tmp, ", ")

            res = res[:-2:]
            res += ");"
            self.header_db = data

        # Создание таблицы
        self.cursor.execute("CREATE TABLE IF NOT EXISTS {0} {1}".format(name_table, res))

    def ExecuteDb(self, name_table: str, data: Union[List, Tuple, str, bytes]):  # +

        if type(data) == str:
            if data in "bytes":
                raise TypeError("Нельзя отпраять BLOB в формате строки. Воспользуйтесь добавление данных через list")
            # Запись в курсор
            self.cursor.execute("INSERT INTO {0} VALUES {1}".format(name_table, data))

        # Конвертация типов в str
        elif type(data) == tuple or type(data) == list:
            if len(data) != len(self.header_db):
                raise IndexError("Разное колличество столбцов таблицы и входных данных")

            res: str = "({}".format("?, " * len(data))[:-2:]
            res += ");"
            self.cursor.execute("INSERT INTO {0} VALUES {1}".format(name_table, res), data)

        else:
            raise TypeError("Допустимые типы List, Tuple, str")

    def GetDb(self, name_table: str) -> list:  # +
        """
        Конвертация bytes в обьекта SQl BLOB и обратно
        a = Binary(b"101101")
        print(a.tobytes())
        """
        # Получить данные из таблицы
        self.cursor.execute('SELECT * FROM {0}'.format(name_table))
        return self.cursor.fetchall()  # [row for row in self.cursor.execute('SELECT * FROM {0}'.format(name_table))]


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
