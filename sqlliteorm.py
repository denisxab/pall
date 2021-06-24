import sqlite3
from os import remove
from os.path import exists, abspath
from typing import List, Tuple, Dict, Union


class SqlName:
    PK: str = "PRIMARY KEY"  # Должны содержать ункальные значения
    NN: str = "NOT NULL"  # Всегда должно быть заполенно
    NND = lambda default=None: "NOT NULL DEFAULT {0}".format(
        default)  # Все столбцы будут по умолчанию заполнены указанными значениями

    DEFAULT = lambda default=None: "DEFAULT {0}".format(default)

    IDAUTO: str = "PRIMARY KEY AUTOINCREMENT"  # Авто заполение строки. подходит для id


def toDictSql(sql_request: str) -> Dict[str, tuple]:
    # Зависит от SqlName
    """
    # Доделать оброботку дополнительных пармаетров

    :param sql_request: Sql запрос
    :return: словарь который поддерживает класс SqlLite
    """
    res_dict = {}
    # Конвертировать SQL запросв в header_db
    for d in sql_request[1:-1:].split(","):

        if d.find("TEXT") != -1:
            v = str
        elif d.find("INTEGER") != -1:
            v = int
        elif d.find("REAL") != -1:
            v = float
        elif d.find("BLOB") != -1:
            v = bytes
        elif d.find("NULL") != -1:
            v = None
        else:
            raise TypeError(
                "Указан не верный тип данных выбирете TEXT;INTEGER;REAL;BLOB;NULL\n{0}".format(d))

        two = ""
        # Сначало длинные слова потом короткие
        if d.find("PRIMARY KEY AUTOINCREMENT") != -1:
            two = "PRIMARY KEY AUTOINCREMENT"
        elif d.find("PRIMARY KEY") != -1:
            two = "PRIMARY KEY"
        #
        elif d.find("NOT NULL DEFAULT") != -1:
            two = "NOT NULL DEFAULT {0}".format(d.split(" ")[:-1:])
        elif d.find("DEFAULT") != -1:
            two = "DEFAULT"
        elif d.find("NOT NULL") != -1:
            two = "NOT NULL"

        res_dict[d.lstrip().split(" ")[0]] = (v, two)

    return res_dict


def toSqlRequest(data: Dict[str, Union[tuple, str]]) -> str:
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


# https://www.youtube.com/watch?v=Vj5n2TGq00o&list=PLA0M1Bcd0w8x4Inr5oYttMK6J47vxgv6J&index=4
class SqlLiteQrm:
    """
    - Запись данных в БД
    - Чтение данных Из БД
    """


    def __init__(self, name_dbf: str) -> None:  # +
        self.connection = None
        # Проверка того что разшерение db
        tmp = name_dbf.split(".")
        if len(tmp) != 2 or tmp[1] != "db":
            raise NameError("Файл должен иметь разшерение .db")

        self.name_db = name_dbf
        self.header_table: Dict[
            str, Dict[str, tuple]] = self.__update_header_table()  # Тут храниться типы столбцов таблциы

        # self.connection = sqlite3.connect(self.name_db)
        # self.cursor = self.connection.cursor()

    def __del__(self):  # +
        pass

    def __update_header_table(self) -> Dict[str, Dict[str, tuple]]:  # +
        # Получение схемы всех таблиц в бд
        res: Dict[str, Dict[str, tuple]] = {}

        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            for x in self.Tables():
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
                        res_nn = "NOT NULL"

                    if r[4] != None:
                        res_nn += "DEFAULT {0}".format(r[4])

                    if r[5] == 1:
                        res_nn = "PRIMARY KEY"

                    res[x].update({r[1]: (res_type, res_nn)})

        return res

    def DeleteTable(self, name_tables: str):  # +
        if self.header_table.get(name_tables):
            self.header_table.pop(name_tables)
        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute("DROP TABLE IF EXISTS {0};".format(name_tables))  # Удалить таблицу если она существует

    def DeleteDb(self):  # +
        if exists(self.name_db):
            remove(abspath(self.name_db))

    def CreateTable(self, name_tables: str, data: Union[str, Dict]):  # +

        # Конвертация типов в str
        res: str = ""
        if type(data) == str:
            res = data
            self.header_table[name_tables] = toDictSql(res)

        elif type(data) == dict:
            res = toSqlRequest(data)

            for k, v in data.items():
                data[k] = (v, "") if type(v) != tuple else v

            self.header_table[name_tables] = data

        # Создание таблицы
        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute("CREATE TABLE IF NOT EXISTS {0} {1}".format(name_tables, res))

    # Запись данных
    def ExecuteDb(self, name_tables: str, data: Union[str,
                                                      List[Union[str, bytes, int, float]],
                                                      Tuple,
                                                      Dict[str, Union[str, bytes, int, float]]]):  # +
        if type(data) == str:
            if data.find("bytes") != -1:
                raise TypeError("Нельзя отпраять BLOB в формате строки. Воспользуйтесь добавление данных через list")

            with sqlite3.connect(self.name_db) as connection:
                cursor = connection.cursor()
                cursor.execute("INSERT INTO {0} {1} VALUES {2}".format(name_tables,
                                                                       tuple(self.header_table[name_tables].keys()),
                                                                       data))

        else:
            res: str = ', '.join('?' * len(data))

            # Конвертация типа в dict в SQL запрос
            if type(data) == dict:
                if tuple(data.keys() - self.header_table[name_tables].keys()):
                    raise IndexError("Именя переданного столбца неуществует")

                with sqlite3.connect(self.name_db) as connection:
                    cursor = connection.cursor()
                    cursor.execute("INSERT INTO {0} {1} VALUES ({2})".format(name_tables, tuple(data.keys()), res),
                                   tuple(data.values()))

            # Конвертация типов list, tuple в SQL запрос
            elif type(data) == tuple or type(data) == list:
                if len(data) != len(self.header_table[name_tables]):
                    raise IndexError("Разное колличество столбцов таблицы и входных данных")

                with sqlite3.connect(self.name_db) as connection:
                    cursor = connection.cursor()
                    cursor.execute("INSERT INTO {0} {1} VALUES ({2})".format(name_tables, tuple(
                        self.header_table[name_tables].keys()), res), data)

    def ExecuteManyDb(self, name_tables: str, data: List[Union[List, Tuple]]):  # +
        if type(data) != list:
            raise TypeError("Должен быть тип List")

        res: str = ', '.join('?' * len(data))

        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.executemany(
                "INSERT INTO {nt} {name_arg} VALUES ({values})".format(nt=name_tables,
                                                                       name_arg=tuple(
                                                                           self.header_table[name_tables].keys()),
                                                                       values=res), data)

    def ExecuteManyDbDict(self, name_tables: str, data: List[Dict]):  # +

        for dict_x in data:
            res: str = ', '.join('?' * len(dict_x))
            ae = "INSERT INTO {nt} {name_arg} VALUES ({values})".format(nt=name_tables,
                                                                        name_arg=tuple(dict_x.keys()),
                                                                        values=res)
            with sqlite3.connect(self.name_db) as connection:
                cursor = connection.cursor()
                cursor.execute(ae, tuple(dict_x.values()))

    # Получения данных
    def GetTable(self, name_tables: str) -> list:  # +
        """
        Конвертация bytes в обьекта SQl BLOB и обратно
        a = Binary(b"101101")
        print(a.tobytes())
        """
        # Получить данные из таблицы
        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT * FROM {0}'.format(name_tables))
            return cursor.fetchall()

    def GetColumne(self, name_tables: str, name_column: str) -> list:  # +
        # Получить данные из столбца
        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute('SELECT {0} FROM {1}'.format(name_column, name_tables))
            return [x[0] for x in cursor.fetchall()]

    def Tables(self) -> List[str]:  # +
        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
            return [x[0] for x in cursor.fetchall() if x[0] != 'sqlite_sequence']

    # Поиск в БД
    def SearchTable(self, name_tables: str,
                    name_column_search: Union[str, Tuple],
                    sqlWHERE: str,
                    sqlORDER_BY: str = "",
                    sqlLIMIT: Union[int, Tuple[int, int]] = 0
                    ) -> list:  # +
        """
        :param name_tables: Название таблицы
        :param name_column_search: Название столбца котроый будет выбора
        :param sqlWHERE: Условие SQL полсе WHERE
        :param sqlORDER_BY: сортировка SQL полсе ORDER BY (- в начате означает обратный порядок DESC)
        :param sqlLIMIT: Лимит поиска
        :return: Список с найдеными столбцами name_column_search
        """
        """
        Получение данных из курсора
        cursor.fetchone() - получить только первую запись
        cursor.fetchmany() - получить только до указаонного колличества
        cursor.fetchall() - получить все записи
        for x in cursor - переберать по одному элементу(данные в виде итератора)      
        """
        if type(name_column_search) == tuple:
            name_column_search = ', '.join(name_column_search)

        request: str = 'SELECT {0} FROM {1} WHERE {2}'.format(name_column_search, name_tables, sqlWHERE)

        if sqlORDER_BY:
            if sqlORDER_BY[0] == '-':  # Сортировка  по убываниюы
                request += " ORDER BY {0} DESC".format(sqlORDER_BY[1::])

            else:  # Сортировка по возрастанию
                request += ' ORDER BY {0} ASC'.format(sqlORDER_BY)

        if sqlLIMIT:

            if type(sqlLIMIT) == tuple:
                request += " LIMIT {0} OFFSET {1}".format(sqlLIMIT[0], sqlLIMIT[1])
            else:
                request += " LIMIT {0}".format(sqlLIMIT)

        with sqlite3.connect(self.name_db) as connection:
            cursor = connection.cursor()
            cursor.execute(request)
            return cursor.fetchall()



if __name__ == '__main__':
    """
    
    
    """

    name_db = 'example.db'
    name_table = "stocks"
    sq = SqlLiteQrm(name_db)
    sq.DeleteTable(name_table)

    sq.CreateTable(name_table, {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
    sq.ExecuteDb(name_table, {"name": "Denis", "old": 21})
    sq.ExecuteDb(name_table, {"name": "Katy", "old": 221, "sex": 1})
    sq.ExecuteDb(name_table, {"name": "Mush", "old": 321, "sex": 21})
    sq.ExecuteDb(name_table, {"name": "Patio", "old": 231, "sex": 21})
    sq.ExecuteDb(name_table, {"name": "Svetha", "old": 24})
    print(sq.GetTable(name_table))

    print(sq.SearchTable(name_table, "name", "old == 21 and sex == 21"))  # И
    print(sq.SearchTable(name_table, "name", "old BETWEEN 10 and 21"))  # В пределах
    print(sq.SearchTable(name_table, "name", "old in (24,22)"))  # Содержиться В ()
    print(sq.SearchTable(name_table, "name", "old == 21 or sex == 1"))  # ИЛИ
    print(sq.SearchTable(name_table, "name", "old not in (21,24)"))  # Приставка НЕ

    print(sq.SearchTable(name_table, "*", "old > 20", "id"))  # И
    print(sq.SearchTable(name_table, "*", "old > 20", "id", 2))  # И
    print(sq.SearchTable(name_table, "*", "old > 20", "id", (4, 2)))  # И

    print()
