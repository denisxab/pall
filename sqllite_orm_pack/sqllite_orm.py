import sqlite3
from os import remove
from os.path import exists, abspath
from re import findall
from sqlite3 import Binary
from typing import List, Tuple, Dict, Union
from file_pack.file import TxtFile
from sqllite_orm_pack.sqlmodules import *


class SqlLiteQrm:

    __slots__ = ("__name_db","__header_table")

    def __init__(self, name_dbf: str) -> None:  # +

        tmp = name_dbf.split(".")  # Проверка того что разшерение db
        if len(tmp) != 2 or tmp[1] != "db":
            raise NameError("Файл должен иметь разшерение .db")

        self.__name_db = name_dbf
        self.__header_table: Dict[
            str, Dict[str, tuple]] = self.__update_header_table()  # Тут храниться типы столбцов таблциы




    @property
    def name_db(self):
        return self.__name_db

    @name_db.setter
    def name_db(self, next_name_db: str):
        self.__name_db = next_name_db
        self.__header_table: Dict[
            str, Dict[str, tuple]] = self.__update_header_table()  # Тут храниться типы столбцов таблциы

    @property
    def header_table(self):
        return self.__header_table

    @staticmethod
    def __CheckBlob(data: List[Union[List[Union[str, bytes, Binary, int, float]], Tuple]]) -> \
            List[Union[List[Union[str, bytes, Binary, int, float]], Tuple]]:
        if type(data[0]) == tuple:
            raise TypeError("Нельзя использовать параметер CheckBLOB на структуре данных tuple. используйте list")
        return [[Binary(item2) if type(item2) == bytes else item2 for item2 in item] for item in data]

    @staticmethod
    def __print_table(NameTable: str,
                      head: str,
                      data_list: List[Union[list, tuple]],
                      width_table: int = 5,
                      ) -> str:
        res: str = ""
        head = head.split(", ")
        for i, p in enumerate(head):
            head[i] = p.center(width_table)
        head = '¦'.join(head)
        lain = "+{0}+".format("-" * len(head))

        res += lain + "\n"
        res += "|{}|\n".format(NameTable.center(len(lain) - 2))
        res += lain + "\n"
        res += "|{0}|\n".format(head)

        for p in data_list:
            res += lain + "\n"
            p = list(p)
            for i, d in enumerate(p):
                p[i] = str(d).center(width_table)

            res += "|{}|\n".format('¦'.join(p))
        res += lain + "\n"

        return res

    def __update_header_table(self) -> Dict[str, Dict[str, tuple]]:  # +
        # Получение схемы всех таблиц в бд
        res: Dict[str, Dict[str, tuple]] = {}

        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            for x in self.ListTables():
                meta = cursor.execute("PRAGMA table_info({0})".format(x))

                """
                (номер имни, имя, тип данных,NOT NULL, значение по умолчанию, PRIMARY KEY )
                """
                res[x] = {}
                for r in meta:

                    type_name = r[2]

                    res_type = None
                    if type_name == "TEXT":
                        res_type = str
                    elif type_name == "INTEGER":
                        res_type = int
                    elif type_name == "REAL":
                        res_type = float
                    elif type_name == "BLOB":
                        res_type = bytes

                    res_nn: str = ""
                    if r[3] == 1:
                        res_nn = " NOT NULL"

                    if r[4]:  # != None:
                        res_nn += " DEFAULT {0}".format(r[4])

                    if r[5] == 1:
                        res_nn = " PRIMARY KEY"

                    res[x].update({r[1]: f"{toTypeSql(res_type)}{res_nn}"})

        return res

    # Информация о БД
    def ListTables(self) -> List[str]:  # +
        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            return [x[0] for x in cursor.fetchall() if x[0] != 'sqlite_sequence']

    def HeadTable(self, NameTable, width_table: int = 10) -> str:
        res = self.__print_table(NameTable, ", ".join(self.header_table[NameTable].keys()),
                                 [[str(x) for x in self.header_table[NameTable].values()]], width_table)
        return res

    def GetTable(self, NameTable: str,
                 LIMIT: Tuple[int, int] = None,
                 FlagPrint: int = 0
                 ) -> list:  # +
        """
        Конвертация bytes в обьекта SQl BLOB и обратно
        a = Binary(b"101101")
        print(a.tobytes())
        """

        request: str = 'SELECT * FROM {0}'.format(NameTable)

        if LIMIT:
            request += " LIMIT {0} OFFSET {1}".format(LIMIT[0], LIMIT[1])

        # Получить данные из таблицы
        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.execute(request)
            res = cursor.fetchall()
            if FlagPrint:
                print(self.__print_table(NameTable, ", ".join(self.header_table[NameTable].keys()), res, FlagPrint))
            return res

    def GetColumn(self, NameTable: str, name_columns: str) -> list:  # +
        # Получить данные из столбца
        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.execute('SELECT {0} FROM {1}'.format(name_columns, NameTable))
            return [x[0] for x in cursor.fetchall()]

    # Удаление Данных
    def DeleteTable(self, NameTable: Union[str, List[str]]):  # +

        # Удалить несоклько таблиц
        if type(NameTable) == list:
            for item in NameTable:
                if self.header_table.get(item):
                    self.header_table.pop(item)
                with sqlite3.connect(self.__name_db) as connect:
                    connect.cursor().execute(f"DROP TABLE IF EXISTS {item}")  # Удалить таблицу если она существует
        else:
            if self.header_table.get(NameTable):
                self.header_table.pop(NameTable)
            with sqlite3.connect(self.__name_db) as connect:
                connect.cursor().execute(f"DROP TABLE IF EXISTS {NameTable}")  # Удалить таблицу если она существует

    def DeleteLineTable(self,
                        NameTable: str,
                        sqlWHERE: str = ""):
        """
        :param NameTable: Название таблицы
        :param sqlWHERE: Условие SQL полсе WHERE
        """
        request: str = "DELETE FROM {0}".format(NameTable)

        if sqlWHERE:
            request += " WHERE {0}".format(sqlWHERE)

        with sqlite3.connect(self.__name_db) as connect:
            connect.cursor().execute(request)

    # Работа с данными таблиц
    def CreateTable(self, NameTable: str, data: Union[str, Dict]):  # +
        # Конвертация типов в str
        request: str = f"CREATE TABLE IF NOT EXISTS {NameTable} "
        if type(data) == str:
            request += data

            dict_tmp: dict = {}
            for items in (x.strip() for x in data.replace("(", "").replace(")", "").split(",")):
                arr = items.split(' ')
                dict_tmp[arr[0]] = " ".join(arr[1::])

            self.header_table[NameTable] = dict_tmp

        elif type(data) == dict:
            # Если все данные сделаны как положено
            if list(filter(lambda x: True if type(x) == str else False, data.values())):
                request += "( "
                for k, v in data.items():
                    request += f"{k} {v}, "
                request = request[:-2:] + " )"
                self.header_table[NameTable] = data
            else:
                raise TypeError("Переданные данные не типа str")

        # Создание таблицы
        with sqlite3.connect(self.__name_db) as connect:
            connect.cursor().execute(request)

    def ExecuteTable(self, NameTable: str,
                     data: Union[str, int, float,
                                 List[Union[str, bytes, int, float]],
                                 Tuple,
                                 Dict[str, Union[str, bytes, int, float]]] = None,
                     sqlRequest: str = "", *,
                     CheckBLOB: bool = False
                     ):  # +
        """
        :param NameTable:
        :param data:
        :param sqlRequest:
        :param CheckBLOB: проверка стурктуры на анличие бинарных бинырных данных и перевод их  в sqlite3.Binary()
        :return:
        """

        # Создать тест на проверку одной запи
        request: str = "INSERT INTO {0}".format(NameTable)

        if sqlRequest:  # для вложенных запрсов
            request += " {0}".format(sqlRequest)
            data = None  # Сделать тест провреки вставки данных

        else:
            if type(data) in (int, float, str):  # Для SQL команд
                if type(data) == str and data.find("bytes") != -1:
                    raise TypeError(
                        "Нельзя отпраять BLOB в формате строки. Воспользуйтесь добавление данных через list")
                request += " ('{0}') VALUES ({1})".format("', '".join(self.header_table[NameTable].keys()), data)
                data = None

            else:  # Для структур данных
                res: str = ', '.join('?' * len(data))
                # Конвертация типа в dict в SQL запрос
                if type(data) == dict:
                    if tuple(data.keys() - self.header_table[NameTable].keys()):
                        raise IndexError("Именя переданного столбца неуществует")

                    request += " ('{0}') VALUES ({1})".format("', '".join(data.keys()), res)

                    if CheckBLOB:
                        data = list(data.values())
                        for index1, tup in enumerate(data):
                            if type(tup) == bytes:
                                data[index1] = Binary(tup)
                        data = tuple(data)
                    else:
                        data = tuple(data.values())

                # Конвертация типов list, tuple в SQL запрос
                elif type(data) == tuple or type(data) == list:
                    if len(data) != len(self.header_table[NameTable]):
                        raise IndexError("Разное колличество столбцов таблицы и входных данных")

                    request += " ('{0}') VALUES ({1})".format("', '".join(self.header_table[NameTable].keys()), res)

        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.execute(request, data) if data else cursor.execute(request)

    def ExecuteManyTable(self, NameTable: str,
                         data: List[Union[List[Union[str, bytes, Binary, int, float]], Tuple]],
                         head_data: Union[List[str], Tuple] = None, *,
                         CheckBLOB: bool = False
                         ):  # +
        """
        :param NameTable:
        :param data:
        :param CheckBLOB: проверка стурктуры на анличие бинарных бинырных данных и перевод их  в sqlite3.Binary()
        :param head_data: собственные заголовки
        :return:
        """
        if type(data) != list:
            raise TypeError("Должен быть тип List")

        # Для получения имен параметров name_head_data
        if not head_data:
            head_data = tuple(self.header_table[NameTable].keys())  # "', '".join(self.header_table[name_table].keys())

        # Нахождение в массиве данных типа bytes и перевод их через sqlite3.Binary()
        if CheckBLOB:
            data = self.__CheckBlob(data)

        res: str = ', '.join('?' * (len(head_data)))
        request: str = "INSERT INTO {nt} ('{name_arg}') VALUES ({values})".format(nt=NameTable,
                                                                                  name_arg="', '".join(head_data),
                                                                                  values=res)
        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.executemany(request, data)

    def ExecuteManyTableDict(self, NameTable: str, data: List[Dict]):  # +
        for dict_x in data:
            res: str = ', '.join('?' * len(dict_x))
            ae = "INSERT INTO {nt} {name_arg} VALUES ({values})".format(nt=NameTable,
                                                                        name_arg=tuple(dict_x.keys()),
                                                                        values=res)
            with sqlite3.connect(self.__name_db) as connect:
                cursor = connect.cursor()
                cursor.execute(ae, tuple(dict_x.values()))

    def UpdateColumne(self, NameTable: str,
                      name_column: Union[str, List[str]],
                      new_data: Union[str, bytes, int, float, List[Union[str, bytes, int, float]]],
                      sqlWHERE: str = "",
                      ):
        """
        Обновлени данных в стобцах
        :param NameTable: Название таблицы
        :param name_column: Название столбца котроый будет выбора
        :param new_data: Новое значение у столбцов
        :param sqlWHERE: Условие SQL полсе WHERE
        """

        request: str = "UPDATE {0} SET ".format(NameTable)

        if type(name_column) == list and type(new_data) == list:  # Нескольк столбцов на измнение
            if len(name_column) != len(new_data):
                raise IndexError("name_column != new_data")

            for n, d in zip(name_column, new_data):
                if type(n) == str:
                    request += "{0} = '{1}', ".format(n, d)
                else:
                    request += "{0} = {1}, ".format(n, d)
            request = request[:-2:]
        else:  # один столбцов на измнение
            request += "{0} = {1}".format(name_column, new_data)

        if sqlWHERE:
            request += " WHERE {0}".format(sqlWHERE)

        with sqlite3.connect(self.__name_db) as connect:
            connect.cursor().execute(request)

    # Поиск в таблице
    def Search(self,
               objectSelect: Select,
               *,
               FlagPrint: int = 0,
               ) -> Union[str, list]:

        request = objectSelect.Request

        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            cursor.execute(request)
            res = cursor.fetchall()

        if FlagPrint:
            sqlSelect = "".join(findall(r"SELECT ([\w, *()'\\]+) FROM", request)) #"".join(findall(r"SELECT ([\w, *]+) FROM", request))
            NameTable = "".join(findall(r"SELECT [\w, *()'\\]+ FROM ([\w]+)[A-Z]*", request))#"".join(findall(r"SELECT [\w, *]+ FROM ([\w]+)[A-Z]*", request))
            if sqlSelect == "*":
                sqlSelect = ", ".join(self.header_table[NameTable].keys())
            print(self.__print_table(NameTable, sqlSelect, res, FlagPrint))

        return res

    # Опреации над БД
    def SaveDbToFile(self, name_save_db):
        tm = TxtFile(name_save_db)
        tm.deleteFile()
        with sqlite3.connect(self.__name_db) as connect:
            for sql_request in connect.iterdump():
                tm.appendFile(sql_request)

    def ReadFileToDb(self, name_read_db):
        tm = TxtFile(name_read_db)
        with sqlite3.connect(self.__name_db) as connect:
            cursor = connect.cursor()
            # Это все для того чтобы пропустить эти строки которые возникают непонятно почему
            # DELETE FROM "sqlite_sequence";INSERT INTO "sqlite_sequence" VALUES('stocks',3);
            cursor.execute("BEGIN TRANSACTION;")
            tmp = False
            for text in tm.readFile().split(";"):
                if tmp:
                    cursor.execute(text)
                else:
                    if text.find("CREATE") != -1:
                        tmp = True
                        cursor.execute(text)

    def DeleteDb(self):  # +
        if exists(self.__name_db):
            remove(abspath(self.__name_db))


if __name__ == '__main__':
    pass
