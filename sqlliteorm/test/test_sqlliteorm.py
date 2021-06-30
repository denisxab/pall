import unittest
from os import path, listdir
from os.path import getsize

from sqlliteorm.sqlliteorm import *


class TestSqlLite(unittest.TestCase):

    def __init__(self, methodName: str = ...) -> None:
        super().__init__(methodName)
        self.name_db = "test.db"
        self.name_table = "stocks"

    def setUp(self) -> None:
        self.sq = SqlLiteQrm(self.name_db)
        self.sq.DeleteTable(self.name_table)

    def test_error_name(self):
        # Проверка на неправильные имена базы данных
        self.assertRaises(NameError, SqlLiteQrm, 'example.txt')
        self.assertRaises(NameError, SqlLiteQrm, 'example')
        self.assertRaises(NameError, SqlLiteQrm, 'ex.am.pl.et')

    def test_ExecuteTable_dict(self):
        # Провекра дополнительынх параметоров к созданию таблицы
        # Провекра записи через dict имен
        test_header = {"id": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertEqual(self.sq.header_table[self.name_table],
                         {'id': 'INTEGER PRIMARY KEY', 'name': 'TEXT', 'old': 'INTEGER NOT NULL DEFAULT 5',
                          'salary': 'REAL NOT NULL'})
        self.sq.ExecuteTable(self.name_table, {"id": 1, "name": "Anton", "old": 30, "salary": 3000.11})
        self.sq.ExecuteTable(self.name_table, {"id": 2, "name": "Katy", "old": 22, "salary": 3200.23})
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Anton', 30, 3000.11),
                                                             (2, 'Katy', 22, 3200.23)])
        self.sq.DeleteTable(self.name_table)

        # Проверка записи с AUTOINCREMENT
        test_header = {"id": PrimaryKeyAutoincrement(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}

        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table],
                         {'id': 'INTEGER PRIMARY KEY AUTOINCREMENT', 'name': 'TEXT',
                          'old': 'INTEGER NOT NULL DEFAULT 5', 'salary': 'REAL NOT NULL'})
        self.sq.ExecuteTable(self.name_table, {"name": "Anton", "old": 30, "salary": 3000.33})
        self.sq.ExecuteTable(self.name_table, {"name": "Katy", "old": 22, "salary": 3200.54})
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Anton', 30, 3000.33), (2, 'Katy', 22, 3200.54)])
        self.sq.DeleteTable(self.name_table)

        # Проверка на неправильное имя столбца в передаче параметров dict
        test_header = {"id": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          {"id": 1, "ERORRRRRRRRR": "Anton", "ol222d": 30, "salary": 3000})
        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          {"id": 2, "ERORRR123132RRRRRR": "Katy", "old": 22, "salary": 3200})
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteManyTable(self):
        # Проверека запси list в Бд
        test_header = {"id": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}
        test_data_list = [
            (1, "Denis", 13, 23232.1223),
            (2, "Enis", 123, 5656.123),
            (3, "Renyi", 133, 365.4),
            (4, "Tennis", 132, 436.123),
        ]
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteManyTable(self.name_table, test_data_list)
        self.assertEqual(self.sq.GetTable(self.name_table), test_data_list)

    def test_CreateTable(self):
        # Првоерка создания таблицы
        test_header = {"date": toTypeSql(str), "trans": toTypeSql(str), "symbol": toTypeSql(str),
                       "qty": toTypeSql(float), "price": toTypeSql(float)}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertEqual(self.sq.header_table[self.name_table],
                         {'date': 'TEXT', 'trans': 'TEXT', 'symbol': 'TEXT', 'qty': 'REAL', 'price': 'REAL'})
        self.sq.DeleteTable(self.name_table)

        # Тест записи заголовка в виде SQL запроса
        self.sq.CreateTable(self.name_table, "(date TEXT, trans TEXT, symbol TEXT, qty REAL, price REAL)")
        self.assertEqual(self.sq.header_table[self.name_table],
                         {'date': 'TEXT', 'trans': 'TEXT', 'symbol': 'TEXT', 'qty': 'REAL', 'price': 'REAL'})

        self.sq.DeleteTable(self.name_table)

        # С доп параметрами
        self.sq.CreateTable(self.name_table, "(qty REAL PRIMARY KEY, date TEXT, trans TEXT, symbol TEXT, price REAL)")
        self.assertEqual(self.sq.header_table[self.name_table],
                         {'date': 'TEXT', 'trans': 'TEXT', 'symbol': 'TEXT', 'qty': 'REAL PRIMARY KEY',
                          'price': 'REAL'})

        self.sq.DeleteTable(self.name_table)

    def test_error_CreateTable(self):
        # Проверка создания таблицы с неправильными данными
        self.assertRaises(TypeError, toTypeSql, list)
        # # Если изменить test_header то нужно переписать test_header на sql запрос
        # self.assertRaises(TypeError, self.sq.CreateTable, self.name_table,
        #                   "(date text, trans text, symbol int, qty real, price real)")
        # Провекра Двойного создания Primary Key
        test_header = {"id": PrimaryKeyAutoincrement(int),
                       "url": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}
        self.assertRaises(sqlite3.OperationalError, self.sq.CreateTable, self.name_table, test_header)

    def test_ExecuteTable_and_GetTable(self):
        # Првоерка коректности записи данных в таблицу
        # Через dict

        test_header = {"date": toTypeSql(str), "trans": toTypeSql(str), "symbol": toTypeSql(str),
                       "qty": toTypeSql(float),
                       "price": toTypeSql(float)}
        test_data = ('2006-01-05', 'BUY', 'RAT', 100, 35.14)
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteTable(self.name_table, test_data)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # Через SQL запрос
        self.sq.CreateTable(self.name_table, {"id_like": toTypeSql(int)})
        self.sq.ExecuteTable(self.name_table, 2323)
        self.assertEqual(self.sq.GetTable(self.name_table)[0][0], 2323)
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteTable_Blob(self):
        # Проверка записи BLOB через
        # Tuple
        test_header = {"str": toTypeSql(str), "int": toTypeSql(int), "float": toTypeSql(float),
                       "bytes": toTypeSql(bytes)}
        test_data = ("text", 123, 122.32, b"1011")
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, test_data, CheckBLOB=True)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # List
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, list(test_data), CheckBLOB=True)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

        # Проверка попытки записи типа BLOB через строку -> должны быть ошибка TypeError
        # test_header = {"str": toTypeSql(str), "int": toTypeSql(int), "float": toTypeSql(float), "bytes": toTypeSql(bytes)}
        test_data = "('text', '123', '122.32', '{0}')".format(b"0101")
        self.sq.CreateTable(self.name_table, "(str TEXT, int INTEGER, float REAL, bytes BLOB)")

        self.assertEqual(self.sq.header_table[self.name_table],
                         {'bytes': 'BLOB', 'float': 'REAL', 'int': 'INTEGER', 'str': 'TEXT'})

        self.assertRaises(TypeError, self.sq.ExecuteTable, (self.name_table, test_data))
        self.sq.DeleteTable(self.name_table)

        # ExecuteManyTable запись BLOB и проврека CheckBLOB
        self.sq.CreateTable(self.name_table, {
            'car_id': PrimaryKeyAutoincrement(int),
            "model": toTypeSql(str),
            "price": toTypeSql(bytes)
        })
        car = [
            ["Audi", b'432'],
            ["Maer", b'424'],
            ["Skoda", b"122"]
        ]
        self.sq.ExecuteManyTable(self.name_table, car, head_data=("model", "price"), CheckBLOB=True)
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Audi', b'432'), (2, 'Maer', b'424'), (3, 'Skoda', b'122')])

    def test_error_ExecuteTable(self):
        # Проверка записи в таблицу неправильные данные
        test_header = {"date": toTypeSql(str), "trans": toTypeSql(str), "symbol": toTypeSql(str),
                       "qty": toTypeSql(float),
                       "price": toTypeSql(float)}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          ('2006-01-05', 'BUY', 'RAT', 100, 35.14, 100010001))  # Привышение длины tuple,list

        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          ('2006-01-05', 'BUY', 35.14, 100010001))  # Маленькая длины tuple,list

    def test_ExecuteTable_sqlRequest(self):
        # Проверка добовления данных ExecuteTable через ReturnSqlRequest
        self.sq.CreateTable(self.name_table,
                            {"id": toTypeSql(int),
                             "old": toTypeSql(int)
                             })
        test_table: str = 'test_table'
        self.sq.CreateTable(test_table,
                            {"old": toTypeSql(int)})

        self.sq.ExecuteManyTable(self.name_table,
                                 [[11, 24],
                                  [22, 31],
                                  [2312, 312],
                                  [231, 68],
                                  [344, 187]])

        resSQL = Select(self.name_table, "id").Where("id < 30").Request

        self.sq.ExecuteTable(test_table, sqlRequest=resSQL)
        self.assertEqual(self.sq.GetTable(test_table), [(11,), (22,)])

    def test_DeleteDb(self):
        # Провекра удаления Бд
        test_header = {"date": toTypeSql(str), "trans": toTypeSql(str), "symbol": toTypeSql(str),
                       "qty": toTypeSql(float), "price": toTypeSql(float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(path.exists(self.name_db), True)
        self.sq.DeleteDb()
        self.assertEqual(path.exists(self.name_db), False)

    def test_total(self):
        # Проверка записи в БД списка деректории
        self.sq.CreateTable(self.name_table, {"name_file": toTypeSql(str), "size_file": toTypeSql(int)})
        for nf in listdir("D:"):
            if len(nf.split(".")) == 2 and nf.split(".")[1] == "py":
                self.sq.ExecuteTable(self.name_table, (nf, getsize(nf)))
        self.assertEqual(self.sq.GetTable(self.name_table), [(nf, getsize(nf)) for nf in listdir("D:") if
                                                             len(nf.split(".")) == 2 and nf.split(".")[1] == "py"])

    def test_Search(self):
        # Проврека функции поиска Search obj_Select SqlDataClass

        # Проверка Формирование запросов
        self.assertEqual(Select("table", "id", "name", "address").Request, 'SELECT id, name, address FROM table')
        self.assertEqual(Select("table", "*").Request, 'SELECT * FROM table')
        self.assertEqual(Select("table", "id", "name", "address").Where("id == 10").Request,
                         'SELECT id, name, address FROM table WHERE id == 10')
        self.assertEqual(Select("table", "id", "name", "address").Where("id == 10").Limit(10).Request,
                         'SELECT id, name, address FROM table WHERE id == 10 LIMIT 10 OFFSET 0')
        self.assertEqual(Select("table", "id", "name", "address").Where("id == 10").Limit(10, 1).Request,
                         'SELECT id, name, address FROM table WHERE id == 10 LIMIT 10 OFFSET 1')
        self.assertEqual(Select("table", "id", "name", "address").Where("id == 10").GroupBy("id").Request,
                         'SELECT id, name, address FROM table WHERE id == 10 GROUP BY id')
        self.assertEqual(Select("table", "id", "name", "address").Where("id == 10").GroupBy("id", "name").Request,
                         'SELECT id, name, address FROM table WHERE id == 10 GROUP BY id, name')
        self.assertEqual(
            Select("table", "id", "name", "address").Where("id >= 10").GroupBy("id", "name").Limit(10).Request,
            'SELECT id, name, address FROM table WHERE id >= 10 GROUP BY id, name LIMIT 10 OFFSET 0')
        self.assertEqual(Select("table", "id", "name", "address").Where("id != 10").OrderBy("id").Request,
                         'SELECT id, name, address FROM table WHERE id != 10 ORDER BY id ASC')
        self.assertEqual(Select("table", "id", "name", "address").Where("id <= 10").OrderBy("id").Limit(5).Request,
                         'SELECT id, name, address FROM table WHERE id <= 10 ORDER BY id ASC LIMIT 5 OFFSET 0')
        self.assertEqual(
            Select("table", "id", "name", "address").Join("new_table", "id.table == id.new_table").Where(
                "id <= 10").OrderBy("id").Limit(5).Request,
            'SELECT id, name, address FROM table INNER JOIN new_table ON id.table == id.new_table WHERE id <= 10 ORDER BY id ASC LIMIT 5 OFFSET 0')

        self.assertRaises(ValueError, Select, "*")

        # Прворека поиска в тестовой бд
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old == 21")),
                         [('Denis',), ('Katy',)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name", "sex").Where("old == 21")),
                         [('Denis', '_'), ('Katy', '1')])

        self.assertEqual(self.sq.Search(Select(self.name_table, "*").Where("old == 21")),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 21, '1')])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old <= 21")),
                         [('Denis',), ('Katy',)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old < 21")), [])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old > 21")), [('Svetha',)])
        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old >= 21")),
                         [('Denis',), ('Katy',), ('Svetha',)])

        # Проверка FlagPrint Проверять вручнкю
        # self.sq.Search(obj_Select(self.name_table, "*"), FlagPrint=10)
        # self.sq.Search(obj_Select(self.name_table, "*").Where("old > 21"), FlagPrint=10)
        # self.sq.Search(obj_Select(self.name_table, "name"), FlagPrint=12)
        # self.sq.Search(obj_Select(self.name_table, "name").Where("old > 21"), FlagPrint=9)
        # self.sq.Search(obj_Select(self.name_table, "name", "old"), FlagPrint=11)

        self.sq.DeleteTable(self.name_table)

    def test_SearchColumn(self):
        # Проверка алгоритма поиска в таблицы
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old == 21")),
                         [('Denis',), ('Katy',)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name", "sex").Where("old == 21")),
                         [('Denis', '_'), ('Katy', '1')])

        self.assertEqual(self.sq.Search(Select(self.name_table, "*").Where("old == 21")),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 21, '1')])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old <= 21")),
                         [('Denis',), ('Katy',)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old < 21")), [])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old > 21")), [('Svetha',)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old >= 21")),
                         [('Denis',), ('Katy',), ('Svetha',)])

        self.sq.DeleteTable(self.name_table)

        # Двойное условие SearchColumn
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 21, "sex": 1},
                                                       {"name": "Mush", "old": 21, "sex": 21},
                                                       {"name": "Patio", "old": 21, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old == 21 and sex == 21")),
                         [('Mush',), ('Patio',)])  # И

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old BETWEEN 10 and 21")),
                         [('Denis',), ('Katy',), ('Mush',), ('Patio',)])  # В пределах

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old in (24,22)")),
                         [('Svetha',)])  # Содержиться В ()

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old == 21 or sex == 1")),
                         [('Denis',), ('Katy',), ('Mush',), ('Patio',)])  # ИЛИ

        self.assertEqual(self.sq.Search(Select(self.name_table, "name").Where("old not in (21,24)")),
                         [])  # Приставка НЕ
        self.sq.DeleteTable(self.name_table)

        # Сортировка ORDER BY
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.assertEqual(
            self.sq.Search(Select(self.name_table, "*").Where("old > 20").OrderBy("old")),
            [(1, 'Denis', 21, '_'),
             (5, 'Svetha', 24, '_'),
             (2, 'Katy', 221, '1'),
             (4, 'Patio', 231, '21'),
             (3, 'Mush', 321, '21')])

        # Сортировка ORDER BY ___ [DESC,ASC]

        self.assertEqual(
            self.sq.Search(Select(self.name_table, "*").Where("old > 20").OrderBy("id")),
            [(1, 'Denis', 21, '_'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
             (4, 'Patio', 231, '21'), (5, 'Svetha', 24, '_')])

        self.assertEqual(
            self.sq.Search(Select(self.name_table, "*").Where("old > 20").OrderBy("-id")),
            [(5, 'Svetha', 24, '_'), (4, 'Patio', 231, '21'), (3, 'Mush', 321, '21'),
             (2, 'Katy', 221, '1'), (1, 'Denis', 21, '_')])

        # LIMIT
        self.assertEqual(self.sq.Search(Select(self.name_table, "*").Where("old > 20").OrderBy("id").Limit(2)),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 221, '1')])  # До 2

        self.assertEqual(
            self.sq.Search(Select(self.name_table, "*").Where("old > 20").OrderBy("id").Limit(4, 2)),
            [(3, 'Mush', 321, '21'), (4, 'Patio', 231, '21'),
             (5, 'Svetha', 24, '_')])  # До 4 с интервалам 2

        self.assertEqual(
            Select(self.name_table, "*").Where("old > 20").OrderBy("id").Limit(4, 2).Request,
            'SELECT * FROM stocks WHERE old > 20 ORDER BY id ASC LIMIT 4 OFFSET 2')

    def test_ExecuteManyTable_and_sqlJOIN(self):
        # Проверка когда созданы ДВЕ таблицы и мы проверяем то что заголвки подобрны правильно для заполения

        self.sq.CreateTable(self.name_table, {"id": toTypeSql(int), "name": toTypeSql(str), "old": toTypeSql(int)})
        self.sq.ExecuteManyTable(self.name_table, [
            [0, "Denis", 21],
            [1, "Musha", 25],
            [2, "Dima", 33]
        ])
        # Проверка когда созданы ДВЕ таблицы и мы проверяем то что заголвки подобрны правильно для заполения

        self.sq.DeleteTable("new")
        self.sq.CreateTable("new", {"id": toTypeSql(int), "many": toTypeSql(int), "score": toTypeSql(int),
                                    "any": toTypeSql(float)})
        self.sq.ExecuteManyTable("new", [
            [0, 3323, 11, 1.1],
            [0, 21, 11, 1.1],
            [2, 223, 33, 1.1],
            [1, 21, 25, 1.1],
            [0, 3323, 11, 1.1],
            [2, 23, 33, 1.1],
            [1, 324, 25, 1.1],
            [0, 3323, 11, 1.1]
        ])

        self.assertEqual(self.sq.GetTable(self.name_table), [(0, 'Denis', 21), (1, 'Musha', 25), (2, 'Dima', 33)])
        self.assertEqual(self.sq.GetTable("new"),
                         [(0, 3323, 11, 1.1), (0, 21, 11, 1.1), (2, 223, 33, 1.1), (1, 21, 25, 1.1), (0, 3323, 11, 1.1),
                          (2, 23, 33, 1.1), (1, 324, 25, 1.1), (0, 3323, 11, 1.1)])

        # Провекра LIMIT у GetTable
        self.assertEqual(self.sq.GetTable("new", LIMIT=(3, 0)),  # Переделать лимит
                         [(0, 3323, 11, 1.1), (0, 21, 11, 1.1), (2, 223, 33, 1.1)])

        # Проверка sqlJOIN
        self.assertEqual(
            self.sq.Search(Select(self.name_table, f'{self.name_table}.name', "new.many").Join("new",
                                                                                               f"{self.name_table}.id =  new.id")),
            [('Denis', 21), ('Denis', 3323), ('Denis', 3323), ('Denis', 3323), ('Musha', 21),
             ('Musha', 324), ('Dima', 23), ('Dima', 223)])

        self.sq.DeleteTable(self.name_table)
        self.sq.DeleteTable("new")

        # Проверка записи list в ExecuteManyTable через второй агругемент с именем
        self.sq.CreateTable(self.name_table, {
            'car_id': PrimaryKeyAutoincrement(int),
            "model": toTypeSql(str),
            "price": toTypeSql(int)
        })

        car = [
            ("Audi", 432),
            ("Maer", 424),
            ("Skoda", 122)
        ]
        self.sq.ExecuteManyTable(self.name_table, car, head_data=("model", "price"))
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Audi', 432), (2, 'Maer', 424), (3, 'Skoda', 122)])

    def test_ExecuteManyTableDict(self):
        # Проверка записи ExecuteManyTableDict
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 21, '1'), (3, 'Svetha', 24, '_')])

    def test_ListTables_and___update_header_table(self):
        # Проверка получения списка таблиц
        test_header = {"id": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "old": NotNullDefault(int, 5),
                       "salary": NotNull(float)}

        self.sq.CreateTable(self.name_table, test_header)

        test_header = {"id": PrimaryKey(int),
                       "film": toTypeSql(str),
                       "old": toTypeSql(int),
                       "salary": toTypeSql(float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.CreateTable("test1", test_header)

        test_header = {"id": PrimaryKey(int),
                       "name": toTypeSql(str),
                       "happy": NotNullDefault(int, 5),
                       "salary": toTypeSql(float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.CreateTable("test2", test_header)

        self.assertEqual(self.sq.ListTables(), ['stocks', 'test1', 'test2'])

        # Проверка __update_header_table для новой таблицы
        sq_test = SqlLiteQrm(self.name_db)
        self.assertEqual(sq_test.ListTables(), ['stocks', 'test1', 'test2'])
        self.assertEqual(sq_test.header_table['test1'], {"id": PrimaryKey(int),
                                                         "film": toTypeSql(str),
                                                         "old": toTypeSql(int),
                                                         "salary": toTypeSql(float)})

    def test_GetColumn(self):
        # Проверка получения столбца
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )

        self.assertEqual(self.sq.GetColumn(self.name_table, "name"), ['Denis', 'Katy', 'Svetha'])
        self.assertEqual(self.sq.GetColumn(self.name_table, "old"), [21, 21, 24])

    def tests_UpdateColumne(self):
        # Проверка обновления данных
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99)
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 99, '_'), (2, 'Katy', 99, '1'), (3, 'Mush', 99, '21'),
                          (4, 'Patio', 99, '21'), (5, 'Svetha', 99, '_')])
        self.sq.DeleteTable(self.name_table)

        # Проверка обновления данных с WHERE
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99, "sex > 21")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 99, '_'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
                          (4, 'Patio', 231, '21'), (5, 'Svetha', 99, '_')])
        self.sq.DeleteTable(self.name_table)

        # Проверка обновления данных с WHERE и LIKE
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99, "name LIKE 'P%'")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
                          (4, 'Patio', 99, '21'), (5, 'Pvetha', 99, '_')])
        self.sq.DeleteTable(self.name_table)

        # Проверка изменения нескольких стобцов
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, ['old', 'sex'], [99, 88], "old > 21")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, '_'), (2, 'Katy', 99, '88'), (3, 'Mush', 99, '88'),
                          (4, 'Patio', 99, '88'), (5, 'Pvetha', 99, '88')])

        self.sq.DeleteTable(self.name_table)

    def test_DeleteLineTable(self):
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])
        self.sq.DeleteLineTable(self.name_table, "old > 25")
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Denis', 21, '_'), (5, 'Pvetha', 24, '_')])

    def test_AggregatingSearchColumne(self):
        # Тестирование агрегирующийх функций и групировок
        self.sq.CreateTable(self.name_table,
                            {"id": PrimaryKeyAutoincrement(int), "name": toTypeSql(str), "old": toTypeSql(int),
                             "sex": NotNullDefault(str, "_")})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Denis", "old": 24}])

        self.assertEqual(self.sq.Search(Select(self.name_table, CountSql("sex")).Where("old < 25")), [(2,)])

        self.assertEqual(self.sq.Search(Select(self.name_table, SumSql("old")).Where("old < 25")), [(45,)])

        self.assertEqual(self.sq.Search(Select(self.name_table, MaxSql("old")).Where("old < 25")), [(24,)])

        self.assertEqual(self.sq.Search(Select(self.name_table, MinSql("old")).Where("old < 25")), [(21,)])

        self.assertEqual(self.sq.Search(Select(self.name_table, AvgSql("old"))), [(163.6,)])

        # Проверка уникального столбца DISTINCT

        self.assertEqual(self.sq.Search(Select(self.name_table, CountSql("-name")).Where("old < 25")), [(1,)])
        # Проверка sqlGROUPBY
        self.assertEqual(
            self.sq.Search(Select(self.name_table, "name", SumSql("old")).GroupBy("name")),
            [('Denis', 45), ('Katy', 221), ('Mush', 321), ('Patio', 231)])

        self.assertEqual(self.sq.Search(Select(self.name_table, "*").GroupBy("sex", "name")),
                         [(2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'), (4, 'Patio', 231, '21'),
                          (1, 'Denis', 21, '_')])

    def test_HeadTable_and___print_table(self):
        # Проверять на глаз
        self.sq.CreateTable(self.name_table, {"id": toTypeSql(int), "name": toTypeSql(str), "old": toTypeSql(int)})
        self.sq.HeadTable(self.name_table, 1)
        # print(self.sq.HeadTable(self.name_table, 15)

    def test_Execute_ALL(self):
        # Проверка того чтоя не изменил строчку которая коректно подстовляет именна SQL запросы
        # request += " ('{0}') VALUES ({1})".format("', '".join(self.header_table[name_table].keys()), res)

        self.sq.CreateTable(self.name_table, {'id_user': toTypeSql(int)})
        data = [[56654639], [236880983], [248579230], [248968841], [388967786], [410456140], [77708645], [132058482],
                [136393787], [144827581], [155823759], [171079623], [526765559]]
        self.sq.ExecuteManyTable(self.name_table, data)

        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(56654639,), (236880983,), (248579230,), (248968841,), (388967786,), (410456140,),
                          (77708645,), (132058482,), (136393787,), (144827581,), (155823759,), (171079623,),
                          (526765559,)])

    def test_SaveDbToFile_ReadFileToDb(self):
        # # ExecuteManyTable запись BLOB и проврека CheckBLOB
        self.sq.CreateTable(self.name_table, {
            'car_id': PrimaryKeyAutoincrement(int),
            "model": toTypeSql(str),
            "price": toTypeSql(bytes)
        })
        car = [
            ["Audi", b'432'],
            ["Maer", b'424'],
            ["Skoda", b"122"]
        ]
        self.sq.ExecuteManyTable(self.name_table, car, head_data=("model", "price"), CheckBLOB=True)

        # тут появляеться запись
        # DELETE FROM "sqlite_sequence";INSERT INTO "sqlite_sequence" VALUES(\'stocks\',3);
        # Который вызвывает ошибку чтения данных из файла, функция ReadFileToDb обрабатывает эту ошибку
        # Если этой ошибку больше не будет то и функцию ReadFileToDb можно упростить
        # tmp = ""
        # with sqlite3.connect(self.name_db) as connection:
        #     for sql in connection.iterdump():
        #         tmp += sql
        # self.assertEqual(tmp,
        #                  'BEGIN TRANSACTION;DELETE FROM "sqlite_sequence";INSERT INTO "sqlite_sequence" VALUES(\'stocks\',3);CREATE TABLE stocks (car_id INTEGER PRIMARY KEY AUTOINCREMENT, model TEXT, price BLOB);INSERT INTO "stocks" VALUES(1,\'Audi\',X\'343332\');INSERT INTO "stocks" VALUES(2,\'Maer\',X\'343234\');INSERT INTO "stocks" VALUES(3,\'Skoda\',X\'313232\');COMMIT;')

        self.sq.SaveDbToFile("test_save.txt")
        self.sq.DeleteDb()
        self.sq.ReadFileToDb("test_save.txt")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Audi', b'432'), (2, 'Maer', b'424'), (3, 'Skoda', b'122')])

        tm = TxtFile("test_save.txt")
        tm.deleteFile()

    def __del__(self):
        self.sq.DeleteDb()


if __name__ == '__main__':
    unittest.main()
