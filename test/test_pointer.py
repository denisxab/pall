import unittest

from pointer import Ptr


class MyTestCase(unittest.TestCase):

    @unittest.skip("")
    def test_something(self):
        def a(arg: Ptr):
            arg.data = 10
            print(f"Функция\t\t\t\t\tid {id(arg)}")

        def plus(arg: Ptr):
            arg.data = [5, 6, 7, 8]
            print(f"Функция\t\t\t\t\t\t\t\tid {id(arg)}")

        test_pt = Ptr([1, 2, 3, 4])
        tmp_id = id(test_pt)
        self.assertEqual(test_pt.data, [1, 2, 3, 4])
        self.assertEqual(id(test_pt), tmp_id)
        print(f'Значение до: {test_pt.data}\t\t\tid {id(test_pt)}')
        plus(test_pt)
        print(f'Значение После: {test_pt.data}\t\tid {id(test_pt)}')
        self.assertEqual(test_pt.data, [5, 6, 7, 8])
        self.assertEqual(id(test_pt), tmp_id)

        test_pt = Ptr()
        tmp_id = id(test_pt)

        self.assertEqual(test_pt.data, None)
        self.assertEqual(id(test_pt), tmp_id)
        print(f'Значение до: {test_pt.data}\t\tid {id(test_pt)}')
        a(test_pt)
        print(f'Значение После: {test_pt.data}\t\tid {id(test_pt)}')
        self.assertEqual(test_pt.data, 10)
        self.assertEqual(id(test_pt), tmp_id)

        # print()

    def test__eq__(self):
        test_pt = Ptr(12321)
        test_pt2 = Ptr(12321)
        print((test_pt == test_pt2))

        self.assertEqual(test_pt, test_pt2)


if __name__ == '__main__':
    unittest.main()
