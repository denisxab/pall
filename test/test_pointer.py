import unittest

from pointer import Ptr


class MyTestCase(unittest.TestCase):

    def test_something(self):
        def a(arg: Ptr):
            arg.pt = 10
            # print(f"Функция\t\t\t\t\tid {id(arg)}")

        def plus(arg: Ptr):
            arg.pt = [5, 6, 7, 8]
            # print(f"Функция\t\t\t\t\t\t\t\tid {id(arg)}")

        test_pt = Ptr([1, 2, 3, 4])
        tmp_id = id(test_pt)
        self.assertEqual(test_pt.pt, [1, 2, 3, 4])
        self.assertEqual(id(test_pt), tmp_id)
        plus(test_pt)
        self.assertEqual(test_pt.pt, [5, 6, 7, 8])
        self.assertEqual(id(test_pt), tmp_id)

        # print(f'Значение до: {test_pt.pt}\t\t\tid {id(test_pt)}')
        # print(f'Значение После: {test_pt.pt}\t\tid {id(test_pt)}')

        test_pt = Ptr()
        tmp_id = id(test_pt)
        # print(f'Значение до: {test_pt.pt}\t\tid {id(test_pt)}')
        self.assertEqual(test_pt.pt, None)
        self.assertEqual(id(test_pt), tmp_id)
        a(test_pt)
        self.assertEqual(test_pt.pt, 10)
        self.assertEqual(id(test_pt), tmp_id)
        # print(f'Значение После: {test_pt.pt}\t\tid {id(test_pt)}')
        # print()


if __name__ == '__main__':
    unittest.main()
