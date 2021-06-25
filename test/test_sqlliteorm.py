import unittest
from os import path, listdir
from os.path import getsize
from sqlite3 import OperationalError

from sqlliteorm import SqlLiteQrm, SqlName


class TestSqlLite(unittest.TestCase):

    def setUp(self) -> None:
        self.name_db = "test.db"
        self.name_table = "stocks"
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
        test_header = {"id": (int, SqlName.PK),
                       "name": str,
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
        self.sq.CreateTable(self.name_table, test_header)

        for k, v in test_header.items():
            test_header[k] = (v, "") if type(v) != tuple else v

        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, {"id": 1, "name": "Anton", "old": 30, "salary": 3000.11})
        self.sq.ExecuteTable(self.name_table, {"id": 2, "name": "Katy", "old": 22, "salary": 3200.23})
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Anton', 30, 3000.11),
                                                             (2, 'Katy', 22, 3200.23)])
        self.sq.DeleteTable(self.name_table)

        # Проверка записи с AUTOINCREMENT
        test_header = {"id": (int, SqlName.IDAUTO),
                       "name": (str, ""),
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}

        for k, v in test_header.items():
            test_header[k] = (v, "") if type(v) != tuple else v

        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, {"name": "Anton", "old": 30, "salary": 3000.33})
        self.sq.ExecuteTable(self.name_table, {"name": "Katy", "old": 22, "salary": 3200.54})
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Anton', 30, 3000.33), (2, 'Katy', 22, 3200.54)])
        self.sq.DeleteTable(self.name_table)

        # Проверка на неправильное имя столбца в передаче параметров dict
        test_header = {"id": (int, SqlName.PK),
                       "name": str,
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          {"id": 1, "ERORRRRRRRRR": "Anton", "ol222d": 30, "salary": 3000})
        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          {"id": 2, "ERORRR123132RRRRRR": "Katy", "old": 22, "salary": 3200})
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteManyTable(self):
        # Проверека запси list в Бд
        test_header = {"id": (int, SqlName.PK),
                       "name": str,
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
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
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)

        for k, v in test_header.items():
            test_header[k] = (v, "") if type(v) != tuple else v

        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.DeleteTable(self.name_table)

        # Тест записи заголовка в виде SQL запроса
        self.sq.CreateTable(self.name_table, "(date TEXT, trans TEXT, symbol TEXT, qty REAL, price INTEGER)")
        self.assertEqual(self.sq.header_table[self.name_table],
                         {'date': (str, ''),
                          'trans': (str, ''),
                          'symbol': (str, ''),
                          'qty': (float, ''),
                          'price': (int, '')})

        self.sq.DeleteTable(self.name_table)

        # С доп параметрами
        self.sq.CreateTable(self.name_table, "(date TEXT, trans TEXT, symbol TEXT, qty REAL PRIMARY KEY, price REAL)")
        self.assertEqual(self.sq.header_table[self.name_table], {
            'date': (str, ''), 'trans': (str, ''),
            'symbol': (str, ''), 'qty': (float, "PRIMARY KEY"),
            'price': (float, '')
        })

        self.sq.DeleteTable(self.name_table)

    def test_error_CreateTable(self):
        # Проверка создания таблицы с неправильными данными
        test_header = {"date": str, "trans": list, "symbol": str, "qty": float, "price": float}
        self.assertRaises(TypeError, self.sq.CreateTable, self.name_table, test_header)
        # Если изменить test_header то нужно переписать test_header на sql запрос
        self.assertRaises(TypeError, self.sq.CreateTable, self.name_table,
                          "(date text, trans text, symbol int, qty real, price real)")
        # Провекра Двойного создания Primary Key
        test_header = {"id": (int, SqlName.IDAUTO),
                       "url": (int, SqlName.PK),
                       "name": str,
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
        self.assertRaises(LookupError, self.sq.CreateTable, self.name_table, test_header)

    def test_ExecuteTable_and_GetTable(self):
        # Првоерка коректности записи данных в таблицу
        # Через dict
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        test_data = ('2006-01-05', 'BUY', 'RAT', 100, 35.14)
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteTable(self.name_table, test_data)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # Через SQL запрос
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteTable(self.name_table, "('2006-01-05','BUY','RAT',100,35.14)")
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteTable_Blob(self):
        # Проверка записи BLOB через
        # Tuple
        test_header = {"str": str, "int": int, "float": float, "bytes": bytes}
        test_data = ("text", 123, 122.32, b"1011")
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, test_data)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # List
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteTable(self.name_table, list(test_data))
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

        # Проверка попытки записи типа BLOB через строку -> должны быть ошибка TypeError
        test_header = {"str": str, "int": int, "float": float, "bytes": bytes}
        test_data = "('text', '123', '122.32', '{0}')".format(b"0101")
        self.sq.CreateTable(self.name_table, "(str TEXT, int INTEGER, float REAL, bytes BLOB)")

        for k, v in test_header.items():
            test_header[k] = (v, "") if type(v) != tuple else v

        self.assertEqual(self.sq.header_table[self.name_table], test_header)

        self.assertRaises(TypeError, self.sq.ExecuteTable, (self.name_table, test_data))
        self.sq.DeleteTable(self.name_table)

    def test_error_ExecuteTable(self):
        # Проверка записи в таблицу неправильные данные
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          ('2006-01-05', 'BUY', 'RAT', 100, 35.14, 100010001))  # Привышение длины tuple,list

        self.assertRaises(IndexError, self.sq.ExecuteTable, self.name_table,
                          ('2006-01-05', 'BUY', 35.14, 100010001))  # Маленькая длины tuple,list

        self.assertRaises(OperationalError, self.sq.ExecuteTable, self.name_table,
                          "('2006-01-05','Вредное слово','BUY','RAT',100,35.14)")  # Привышение длины для str

        self.assertRaises(OperationalError, self.sq.ExecuteTable, self.name_table,
                          "('2006-01-05','Вредное слово',100,35.14)")  # Маленькая длины для str

    def test_DeleteDb(self):
        # Провекра удаления Бд
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(path.exists(self.name_db), True)
        self.sq.DeleteDb()
        self.assertEqual(path.exists(self.name_db), False)

    def test_total(self):
        # Проверка записи в БД списка деректории
        self.sq.CreateTable(self.name_table, {"name_file": str, "size_file": int})
        for nf in listdir("."):
            if len(nf.split(".")) == 2 and nf.split(".")[1] == "py":
                self.sq.ExecuteTable(self.name_table, (nf, getsize(nf)))
        self.assertEqual(self.sq.GetTable(self.name_table), [(nf, getsize(nf)) for nf in listdir(".") if
                                                             len(nf.split(".")) == 2 and nf.split(".")[1] == "py"])

    def test_SearchColumn(self):
        # Проверка алгоритма поиска в таблицы
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )

        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old == 21"), [('Denis',), ('Katy',)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, ("name", "sex"), sqlWHERE="old == 21"),
                         [('Denis', 'None'), ('Katy', '1')])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old == 21"),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 21, '1')])

        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old <= 21"), [('Denis',), ('Katy',)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old < 21"), [])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old > 21"), [('Svetha',)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old >= 21"),
                         [('Denis',), ('Katy',), ('Svetha',)])

        self.sq.DeleteTable(self.name_table)

        # Двойное условие SearchColumn
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 21, "sex": 1},
                                                       {"name": "Mush", "old": 21, "sex": 21},
                                                       {"name": "Patio", "old": 21, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old == 21 and sex == 21"),
                         [('Mush',), ('Patio',)])  # И
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old BETWEEN 10 and 21"),
                         [('Denis',), ('Katy',), ('Mush',), ('Patio',)])  # В пределах
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old in (24,22)"),
                         [('Svetha',)])  # Содержиться В ()
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old == 21 or sex == 1"),
                         [('Denis',), ('Katy',), ('Mush',), ('Patio',)])  # ИЛИ
        self.assertEqual(self.sq.SearchColumn(self.name_table, "name", sqlWHERE="old not in (21,24)"),
                         [])  # Приставка НЕ
        self.sq.DeleteTable(self.name_table)

        # Сортировка ORDER BY
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old > 20", sqlORDER_BY="old"),
                         [(1, 'Denis', 21, 'None'),
                          (5, 'Svetha', 24, 'None'),
                          (2, 'Katy', 221, '1'),
                          (4, 'Patio', 231, '21'),
                          (3, 'Mush', 321, '21')])

        # Сортировка ORDER BY ___ [DESC,ASC]
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old > 20", sqlORDER_BY="id"),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
                          (4, 'Patio', 231, '21'), (5, 'Svetha', 24, 'None')])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old > 20", sqlORDER_BY="-id"),
                         [(5, 'Svetha', 24, 'None'), (4, 'Patio', 231, '21'), (3, 'Mush', 321, '21'),
                          (2, 'Katy', 221, '1'), (1, 'Denis', 21, 'None')])

        # LIMIT
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old > 20", sqlORDER_BY="id", sqlLIMIT=2),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 221, '1')])  # До 2
        self.assertEqual(
            self.sq.SearchColumn(self.name_table, "*", sqlWHERE="old > 20", sqlORDER_BY="id", sqlLIMIT=(4, 2)),
            [(3, 'Mush', 321, '21'), (4, 'Patio', 231, '21'),
             (5, 'Svetha', 24, 'None')])  # До 4 с интервалам 2

    def test_ExecuteManyTable_and_sqlJOIN(self):
        # Проверка когда созданы ДВЕ таблицы и мы проверяем то что заголвки подобрны правильно для заполения

        self.sq.CreateTable(self.name_table, {"id": int, "name": str, "old": int})
        self.sq.ExecuteManyTable(self.name_table, [
            [0, "Denis", 21],
            [1, "Musha", 25],
            [2, "Dima", 33]
        ])
        # Проверка когда созданы ДВЕ таблицы и мы проверяем то что заголвки подобрны правильно для заполения

        self.sq.DeleteTable("new")
        self.sq.CreateTable("new", {"id": int, "many": int, "score": int, "any": float})
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
        self.assertEqual(self.sq.GetTable("new", 3), [(0, 3323, 11, 1.1), (0, 21, 11, 1.1), (2, 223, 33, 1.1)])

        # Проверка sqlJOIN
        self.assertEqual(self.sq.SearchColumn(self.name_table,
                                              sqlSelect=(f'{self.name_table}.name', "new.many"),
                                              sqlJOIN=SqlName.InnerJoin("new", f"{self.name_table}.id =  new.id")),
                         [('Denis', 21), ('Denis', 3323), ('Denis', 3323), ('Denis', 3323), ('Musha', 21),
                          ('Musha', 324), ('Dima', 23), ('Dima', 223)])

    def test_ExecuteManyTableDict(self):
        # Проверка записи ExecuteManyTableDict
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table,
                                     [{"name": "Denis", "old": 21},
                                      {"name": "Katy", "old": 21, "sex": 1},
                                      {"name": "Svetha", "old": 24}]
                                     )
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 21, '1'), (3, 'Svetha', 24, 'None')])

    def test_ListTables_and___update_header_table(self):
        # Проверка получения списка таблиц
        test_header = {"id": (int, SqlName.PK),
                       "name": (str),
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
        self.sq.CreateTable(self.name_table, test_header)

        test_header = {"id": (int, SqlName.PK),
                       "film": (str),
                       "old": (int),
                       "salary": (float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.CreateTable("test1", test_header)

        test_header = {"id": (int, SqlName.PK),
                       "name": (str),
                       "happy": (int, SqlName.NND(52)),
                       "salary": (float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.CreateTable("test2", test_header)

        self.assertEqual(self.sq.ListTables(), ['stocks', 'test1', 'test2'])

        # Проверка __update_header_table для новой таблицы
        sq = SqlLiteQrm(self.name_db)
        self.assertEqual(sq.ListTables(), ['stocks', 'test1', 'test2'])
        self.assertEqual(sq.header_table['test1'], {"id": (int, SqlName.PK),
                                                    "film": (str, ''),
                                                    "old": (int, ''),
                                                    "salary": (float, '')})

    def test_GetColumn(self):
        # Проверка получения столбца
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
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
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99)
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 99, 'None'), (2, 'Katy', 99, '1'), (3, 'Mush', 99, '21'),
                          (4, 'Patio', 99, '21'), (5, 'Svetha', 99, 'None')])
        self.sq.DeleteTable(self.name_table)

        # Проверка обновления данных с WHERE
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Svetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99, "sex > 21")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 99, 'None'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
                          (4, 'Patio', 231, '21'), (5, 'Svetha', 99, 'None')])
        self.sq.DeleteTable(self.name_table)

        # Проверка обновления данных с WHERE и LIKE
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, 'old', 99, "name", "P%")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'),
                          (4, 'Patio', 99, '21'), (5, 'Pvetha', 99, 'None')])
        self.sq.DeleteTable(self.name_table)

        # Проверка изменения нескольких стобцов
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])

        self.sq.UpdateColumne(self.name_table, ['old', 'sex'], [99, 88], "old > 21")
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 99, '88'), (3, 'Mush', 99, '88'),
                          (4, 'Patio', 99, '88'), (5, 'Pvetha', 99, '88')])

        self.sq.DeleteTable(self.name_table)

    def test_DeleteLineTable(self):
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Pvetha", "old": 24}])
        self.sq.DeleteLineTable(self.name_table, "old > 25")
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Denis', 21, 'None'), (5, 'Pvetha', 24, 'None')])

    def test_AggregatingSearchColumne(self):
        # Тестирование агрегирующийх функций и групировок
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyTableDict(self.name_table, [{"name": "Denis", "old": 21},
                                                       {"name": "Katy", "old": 221, "sex": 1},
                                                       {"name": "Mush", "old": 321, "sex": 21},
                                                       {"name": "Patio", "old": 231, "sex": 21},
                                                       {"name": "Denis", "old": 24}])

        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.count("sex"), sqlWHERE="old < 25"), [(2,)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.sum("old"), sqlWHERE="old < 25"), [(45,)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.max("old"), sqlWHERE="old < 25"), [(24,)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.min("old"), sqlWHERE="old < 25"), [(21,)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.avg("old")), [(163.6,)])

        # Проверка уникального столбца DISTINCT
        self.assertEqual(self.sq.SearchColumn(self.name_table, SqlName.count("-name"), sqlWHERE="old < 25"), [(1,)])
        # Проверка sqlGROUPBY
        self.assertEqual(self.sq.SearchColumn(self.name_table, ("name", SqlName.sum("old")), sqlGROUPBY="name"),
                         [('Denis', 45), ('Katy', 221), ('Mush', 321), ('Patio', 231)])
        self.assertEqual(self.sq.SearchColumn(self.name_table, "*", sqlGROUPBY=("sex", "name")),
                         [(2, 'Katy', 221, '1'), (3, 'Mush', 321, '21'), (4, 'Patio', 231, '21'),
                          (1, 'Denis', 21, 'None')])

    def __del__(self):
        self.sq.DeleteDb()


if __name__ == '__main__':
    unittest.main()
