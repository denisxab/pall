import unittest
from os import path, listdir
from os.path import getsize
from sqlite3 import OperationalError

from sqlliteorm import SqlLite


class TestSqlLite(unittest.TestCase):

    def setUp(self) -> None:
        self.name_db = "test.db"
        self.name_table = "stocks"
        self.sq = SqlLite(self.name_db)
        self.sq.DeleteTable(self.name_table)

    def test_error_name(self):
        # Проверка на неправильные имена базы данных
        self.assertRaises(NameError, SqlLite, 'example.txt')
        self.assertRaises(NameError, SqlLite, 'example')
        self.assertRaises(NameError, SqlLite, 'ex.am.pl.et')

    def test_CreateTable(self):
        # Првоерка создания таблицы
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)
        self.assertEqual(self.sq.header_db, test_header)
        self.sq.DeleteTable(self.name_table)

        # Если изменить test_header то нужно переписать test_header на sql запрос
        # (date text, trans text, symbol text, qty real, price real)
        self.sq.CreateTable(self.name_table, "(date text, trans text, symbol text, qty real, price real)")
        self.assertEqual(self.sq.header_db, test_header)
        self.sq.DeleteTable(self.name_table)

    def test_error_CreateTable(self):
        # Проверка создания таблицы с неправильными данными
        test_header = {"date": str, "trans": list, "symbol": str, "qty": float, "price": float}
        self.assertRaises(TypeError, self.sq.CreateTable, self.name_table, test_header)
        # Если изменить test_header то нужно переписать test_header на sql запрос
        self.assertRaises(TypeError, self.sq.CreateTable, self.name_table,
                          "(date text, trans text, symbol int, qty real, price real)")

    def test_ExecuteDb_and_GetDb(self):
        # Првоерка записи данных в таблицу
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        test_data = ('2006-01-05', 'BUY', 'RAT', 100, 35.14)
        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteDb(self.name_table, test_data)
        self.assertEqual(self.sq.GetDb(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

        self.sq.CreateTable(self.name_table, test_header)
        self.sq.ExecuteDb(self.name_table, "('2006-01-05','BUY','RAT',100,35.14)")
        self.assertEqual(self.sq.GetDb(self.name_table)[0], test_data)
        self.sq.DeleteTable(self.name_table)

    def test_error_ExecuteDb(self):
        # Проверка записи в таблицу неправильные данные
        test_header = {"date": str, "trans": str, "symbol": str, "qty": float, "price": float}
        self.sq.CreateTable(self.name_table, test_header)

        self.assertRaises(IndexError, self.sq.ExecuteDb, self.name_table,
                          ('2006-01-05', 'BUY', 'RAT', 100, 35.14, 100010001))
        self.assertRaises(OperationalError, self.sq.ExecuteDb, self.name_table,
                          "('2006-01-05','Вредное слово','BUY','RAT',100,35.14)")
        self.assertRaises(TypeError, self.sq.ExecuteDb, self.name_table,
                          {'2006-01-05', 'BUY', 'RAT', 100, 35.14, 100010001})

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
        self.assertEqual(self.sq.GetDb(self.name_table), [(nf, getsize(nf)) for nf in listdir(".") if
                                                          len(nf.split(".")) == 2 and nf.split(".")[1] == "py"])

    def __del__(self):
        self.sq.DeleteDb()


if __name__ == '__main__':
    unittest.main()
