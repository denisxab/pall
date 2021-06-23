import sqlite3
from os import remove
from os.path import exists, abspath
from typing import List, Tuple, Dict, Union


class SqlName:
    PK: str = "PRIMARY KEY"  # Должны содержать ункальные значения
    NN: str = "NOT NULL"  # Все столбцы по умолчанию будт заполнены Null
    NND = lambda default=None: "NOT NULL DEFAULT {0}".format(
        default)  # Все столбцы будут по умолчанию заполнены указанными значениями
    IDAUTO: str = "PRIMARY KEY AUTOINCREMENT"  # Авто заполение строки. подходит для id


def toDictSql(sql_request: str) -> dict:
    """
    :param sql_request: Sql запрос
    :return: словарь который поддерживает класс SqlLite
    """
    res_dict: dict = {}
    # Конвертировать SQL запросв в header_db
    for d in sql_request[1:-1:].split(","):

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

        res_dict[k] = v

    return res_dict


def toSqlRequest(data: dict) -> str:
    """
    :param data: словарь по стандартам класса SqlLite
    :return: SQl запрос
    """
    count_primary_key = False
    # Конвертация словаря в SQL запрос
    res = "("
    for k, v in data.items():
        res += str(k)

        if type(v) == tuple and len(v) == 2:  # Комбенированная запись с параметрами столбца

            if v[1] == SqlName.PK or v[1] == SqlName.IDAUTO:  # Проверка уникальности Primary Key в таблицы
                if not count_primary_key:
                    count_primary_key = True
                else:
                    raise LookupError("Запрос имеет больше одного primary key")

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
    return res


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

    def __init__(self, name_dbf: str) -> None:  # +
        self.connection = None
        # Проверка того что разшерение db
        tmp = name_dbf.split(".")
        if len(tmp) != 2 or tmp[1] != "db":
            raise NameError("Файл должен иметь разшерение .db")

        self.name_db = name_dbf
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

    def DeleteTable(self, name_tables: str):  # +
        # Удалить таблицу если она существует
        self.cursor.execute("DROP TABLE IF EXISTS {0};".format(name_tables))
        self.header_db = dict()

    def DeleteDb(self):  # +
        self.__del__()
        if exists(self.name_db):
            remove(abspath(self.name_db))

    def CreateTable(self, name_tables: str, data: Union[str, Dict]):  # +
        # Конвертация типов в str
        res: str = ""
        if type(data) == str:
            res = data
            self.header_db = toDictSql(res)

        elif type(data) == dict:
            res = toSqlRequest(data)
            self.header_db = data

        # Создание таблицы
        self.cursor.execute("CREATE TABLE IF NOT EXISTS {0} {1}".format(name_tables, res))

    def ExecuteDb(self, name_tables: str, data: Union[str,
                                                      List[Union[str, bytes, int, float]],
                                                      Tuple[Union[str, bytes, int, float]],
                                                      Dict[str, Union[str, bytes, int, float]]]):  # +

        if type(data) == str:
            if data.find("bytes") != -1:
                raise TypeError("Нельзя отпраять BLOB в формате строки. Воспользуйтесь добавление данных через list")
            self.cursor.execute(
                "INSERT INTO {0} {1} VALUES {2}".format(name_tables, tuple(self.header_db.keys()), data))

        else:
            res: str = "({}".format("?, " * len(data))[:-2:]
            res += ");"

            # Конвертация типа в dict в SQL запрос
            if type(data) == dict:
                if tuple(data.keys() - self.header_db.keys()):
                    raise IndexError("Именя переданного столбца неуществует")

                self.cursor.execute("INSERT INTO {0} {1} VALUES {2}".format(name_tables, tuple(data.keys()), res),
                                    tuple(data.values()))

            # Конвертация типов list, tuple в SQL запрос
            elif type(data) == tuple or type(data) == list:
                if len(data) != len(self.header_db):
                    raise IndexError("Разное колличество столбцов таблицы и входных данных")

                self.cursor.execute(
                    "INSERT INTO {0} {1} VALUES {2}".format(name_tables, tuple(self.header_db.keys()), res),
                    data)
        self.SaveDb()

    def ExecuteManyDb(self, name_tables: str, data: List[Union[List, Tuple]]):  # +
        if type(data) != list:
            raise TypeError("Должен быть тип List")

        res: str = "({}".format("?, " * len(data))[:-2:]
        res += ");"

        self.cursor.executemany(
            "INSERT INTO {nt} {name_arg} VALUES {values}".format(nt=name_tables, name_arg=tuple(self.header_db.keys()),
                                                                 values=res), data)
        self.SaveDb()

    def GetTable(self, name_tables: str) -> list:  # +
        """
        Конвертация bytes в обьекта SQl BLOB и обратно
        a = Binary(b"101101")
        print(a.tobytes())
        """
        # Получить данные из таблицы
        self.cursor.execute('SELECT * FROM {0}'.format(name_tables))
        return self.cursor.fetchall()

    def GetColumne(self, name_tables: str, name_column: str) -> list:
        # Получить данные из таблицы
        self.cursor.execute('SELECT {0} FROM {1}'.format(name_column, name_tables))
        return self.cursor.fetchall()

    def SearchTable(self, name_tables: str, name_column: str, questions: str) -> bool:
        self.cursor.execute(
            'SELECT {nc} FROM {nt} WHERE {nc} = {q}'.format(nc=name_column, nt=name_tables, q=questions))

        return True if self.cursor.fetchone() != None else False

#https://www.youtube.com/watch?v=gm0p517EG7o
if __name__ == '__main__':
    name_db = 'example.db'
    name_table = "stocks"
    sq = SqlLite(name_db)
    sq.DeleteTable(name_table)

    sq.CreateTable(name_table, {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
    print(sq.header_db)
    print(toSqlRequest(sq.header_db))

    sq.ExecuteDb(name_table, {"name": "Denis", "old": 21})
    sq.ExecuteDb(name_table, {"name": "Katy", "old": 23, "sex": 1})
    sq.ExecuteDb(name_table, {"name": "Svetha", "old": 24})
    print(sq.GetTable(name_table))

    print(sq.GetColumne(name_table, "name"))
    print(sq.SearchTable(name_table, "old", "212"))
