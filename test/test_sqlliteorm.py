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

    def test_ExecuteDb_dict(self):
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
        self.sq.ExecuteDb(self.name_table, {"id": 1, "name": "Anton", "old": 30, "salary": 3000.11})
        self.sq.ExecuteDb(self.name_table, {"id": 2, "name": "Katy", "old": 22, "salary": 3200.23})
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
        self.sq.ExecuteDb(self.name_table, {"name": "Anton", "old": 30, "salary": 3000.33})
        self.sq.ExecuteDb(self.name_table, {"name": "Katy", "old": 22, "salary": 3200.54})
        self.assertEqual(self.sq.GetTable(self.name_table), [(1, 'Anton', 30, 3000.33), (2, 'Katy', 22, 3200.54)])
        self.sq.DeleteTable(self.name_table)

        # Проверка на неправильное имя столбца в передаче параметров dict
        test_header = {"id": (int, SqlName.PK),
                       "name": str,
                       "old": (int, SqlName.NND(5)),
                       "salary": (float, SqlName.NN)}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertRaises(IndexError, self.sq.ExecuteDb, self.name_table,
                          {"id": 1, "ERORRRRRRRRR": "Anton", "ol222d": 30, "salary": 3000})
        self.assertRaises(IndexError, self.sq.ExecuteDb, self.name_table,
                          {"id": 2, "ERORRR123132RRRRRR": "Katy", "old": 22, "salary": 3200})
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteManyDb(self):
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
        self.sq.ExecuteManyDb(self.name_table, test_data_list)
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

    def test_ExecuteDb_and_GetTable(self):
        # Првоерка коректности записи данных в таблицу
        # Через dict
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        test_data = ('2006-01-05', 'BUY', 'RAT', 100, 35.14)
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteDb(self.name_table, test_data)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # Через SQL запрос
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteDb(self.name_table, "('2006-01-05','BUY','RAT',100,35.14)")
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

    def test_ExecuteDb_Blob(self):
        # Проверка записи BLOB через
        # Tuple
        test_header = {"str": str, "int": int, "float": float, "bytes": bytes}
        test_data = ("text", 123, 122.32, b"1011")
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteDb(self.name_table, test_data)
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)
        # List
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_table[self.name_table], test_header)
        self.sq.ExecuteDb(self.name_table, list(test_data))
        self.assertEqual(self.sq.GetTable(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

        # Проверка попытки записи типа BLOB через строку -> должны быть ошибка TypeError
        test_header = {"str": str, "int": int, "float": float, "bytes": bytes}
        test_data = "('text', '123', '122.32', '{0}')".format(b"0101")
        self.sq.CreateTable(self.name_table, "(str TEXT, int INTEGER, float REAL, bytes BLOB)")

        for k, v in test_header.items():
            test_header[k] = (v, "") if type(v) != tuple else v

        self.assertEqual(self.sq.header_table[self.name_table], test_header)

        self.assertRaises(TypeError, self.sq.ExecuteDb, (self.name_table, test_data))
        self.sq.DeleteTable(self.name_table)

    def test_error_ExecuteDb(self):
        # Проверка записи в таблицу неправильные данные
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertRaises(IndexError, self.sq.ExecuteDb, self.name_table,
                          ('2006-01-05', 'BUY', 'RAT', 100, 35.14, 100010001))  # Привышение длины tuple,list

        self.assertRaises(IndexError, self.sq.ExecuteDb, self.name_table,
                          ('2006-01-05', 'BUY', 35.14, 100010001))  # Маленькая длины tuple,list

        self.assertRaises(OperationalError, self.sq.ExecuteDb, self.name_table,
                          "('2006-01-05','Вредное слово','BUY','RAT',100,35.14)")  # Привышение длины для str

        self.assertRaises(OperationalError, self.sq.ExecuteDb, self.name_table,
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
                self.sq.ExecuteDb(self.name_table, (nf, getsize(nf)))
        self.assertEqual(self.sq.GetTable(self.name_table), [(nf, getsize(nf)) for nf in listdir(".") if
                                                             len(nf.split(".")) == 2 and nf.split(".")[1] == "py"])

    def test_SearchTable(self):
        # Проверка алгоритма поиска в таблицы
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyDbDict(self.name_table,
                                  [{"name": "Denis", "old": 21},
                                   {"name": "Katy", "old": 21, "sex": 1},
                                   {"name": "Svetha", "old": 24}]
                                  )

        self.assertEqual(self.sq.SearchTable(self.name_table, "name", "old", "==", "21"), [('Denis',), ('Katy',)])
        self.assertEqual(self.sq.SearchTable(self.name_table, ("name", "sex"), "old", "==", "21"),
                         [('Denis', 'None'), ('Katy', '1')])
        self.assertEqual(self.sq.SearchTable(self.name_table, "*", "old", "==", "21"),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 21, '1')])

        self.assertEqual(self.sq.SearchTable(self.name_table, "name", "old", "<=", "21"), [('Denis',), ('Katy',)])
        self.assertEqual(self.sq.SearchTable(self.name_table, "name", "old", "<", "21"), [])
        self.assertEqual(self.sq.SearchTable(self.name_table, "name", "old", ">", "21"), [('Svetha',)])
        self.assertEqual(self.sq.SearchTable(self.name_table, "name", "old", ">=", "21"),
                         [('Denis',), ('Katy',), ('Svetha',)])

        # Провекра неправильных парамтров condition
        self.assertRaises(ValueError, self.sq.SearchTable, self.name_table, "name", "old", "===", "21")

    def test_ExecuteManyDbDict(self):
        # Проверка записи ExecuteManyDbDict
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyDbDict(self.name_table,
                                  [{"name": "Denis", "old": 21},
                                   {"name": "Katy", "old": 21, "sex": 1},
                                   {"name": "Svetha", "old": 24}]
                                  )
        self.assertEqual(self.sq.GetTable(self.name_table),
                         [(1, 'Denis', 21, 'None'), (2, 'Katy', 21, '1'), (3, 'Svetha', 24, 'None')])

    def test_Tables_and___update_header_table(self):
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
                       "hapy": (int, SqlName.NND(52)),
                       "salary": (float)}
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.CreateTable("test2", test_header)

        self.assertEqual(self.sq.Tables(), ['stocks', 'test1', 'test2'])

        # Проверка __update_header_table для новой таблицы
        sq = SqlLiteQrm(self.name_db)
        self.assertEqual(sq.Tables(), ['stocks', 'test1', 'test2'])
        self.assertEqual(sq.header_table['test1'], {"id": (int, SqlName.PK),
                                                    "film": (str, ''),
                                                    "old": (int, ''),
                                                    "salary": (float, '')})

    def test_GetColumne(self):
        # Проверка получения столбца
        self.sq.CreateTable(self.name_table,
                            {"id": (int, SqlName.IDAUTO), "name": str, "old": int, "sex": (str, SqlName.NND())})
        self.sq.ExecuteManyDbDict(self.name_table,
                                  [{"name": "Denis", "old": 21},
                                   {"name": "Katy", "old": 21, "sex": 1},
                                   {"name": "Svetha", "old": 24}]
                                  )

        self.assertEqual(self.sq.GetColumne(self.name_table, "name"), ['Denis', 'Katy', 'Svetha'])
        self.assertEqual(self.sq.GetColumne(self.name_table, "old"), [21, 21, 24])

    def __del__(self):
        self.sq.DeleteDb()


if __name__ == '__main__':
    unittest.main()
