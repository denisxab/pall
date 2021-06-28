"""
Модификаторы данных
"""


def definition(TypeColumn):
    if TypeColumn == str:
        res = "TEXT"
    elif TypeColumn == int:
        res = "INTEGER"
    elif TypeColumn == float:
        res = "REAL"
    elif TypeColumn == bytes:
        res = "BLOB"
    elif TypeColumn is None:
        res = "NULL"
    else:
        raise TypeError(f"Указан не верный тип данных выбирете str;int;float;None;bytes\n{TypeColumn}")
    return res


# Должны содержать ункальные значения
PrimaryKey = lambda TypeColumn: definition(TypeColumn) + " PRIMARY KEY"
# Всегда должно быть заполенно
NotNull = lambda TypeColumn: definition(TypeColumn) + " NOT NULL"
# Все столбцы будут по умолчанию заполнены указанными значениями
NotNullDefault = lambda TypeColumn, default: definition(TypeColumn) + f" NOT NULL DEFAULT {default}"
# Значение по умолчанию
Default = lambda TypeColumn, default: definition(TypeColumn) + " DEFAULT {0}".format(default)
# Авто заполение строки. подходит для id
PrimaryKeyAutoincrement = lambda TypeColumn: definition(TypeColumn) + " PRIMARY KEY AUTOINCREMENT"
toTypeSql = lambda TypeColumn: definition(TypeColumn)

"""
Агрегирущие функции
"""
CountSql = lambda arg: "count(DISTINCT {0})".format(arg[1::]) if arg[0] == "-" else "count({0})".format(arg)
SumSql = lambda arg: "sum(DISTINCT {0})".format(arg[1::]) if arg[0] == "-" else "sum({0})".format(arg)
AvgSql = lambda arg: "avg(DISTINCT {0})".format(arg[1::]) if arg[0] == "-" else "avg({0})".format(arg)
MinSql = lambda arg: "min(DISTINCT {0})".format(arg[1::]) if arg[0] == "-" else "min({0})".format(arg)
MaxSql = lambda arg: "max(DISTINCT {0})".format(arg[1::]) if arg[0] == "-" else "max({0})".format(arg)

# Union[str, Tuple] выбор столбцов
select = lambda *sel: ', '.join(sel)

# Union[str, Tuple] слиять таблицы
InnerJoin = lambda name_table, ON: "INNER JOIN {0} ON {1}".format(name_table, ', '.join(ON)) if type(
    ON) == tuple else "INNER JOIN {0} ON {1}".format(name_table, ON)

# Union[str, Tuple] слиять таблицы даже если они не равны
LeftJoin = lambda name_table, ON: "LEFT JOIN {0} ON {1}".format(name_table, ', '.join(ON))

# str
where = lambda condition: " WHERE {0}".format(condition)

# str
order_by = lambda order: " ORDER BY {0} DESC".format(order[1::]) if order[0] == '-' else ' ORDER BY {0} ASC'.format(
    order)
# str
group_by = lambda *group: " GROUP BY {0}".format(', '.join(group)) if list(
    filter(lambda it: True if it else False, group)) else ""

# int, int
limit = lambda lim, offset=0: " LIMIT {0} OFFSET {1}".format(lim, offset) if lim else ""


class ObjSelect:
    """
    Задача: Составлние SQl запроса поиска в БД через методы классов
    !!!: Не рекомендуетсья использовать в многопоточном режиме (хотя и в нем работает нормально) используйте SearchColumn
    """
    reqSql: str = ""  # Общая переменна которая измемнятьс в obj_Select

    select = lambda sql_select, name_table: 'SELECT {0} FROM {1}'.format(', '.join(sql_select), name_table)

    group_by = lambda group: " GROUP BY {0}".format(', '.join(group)) if list(
        filter(lambda it: True if it else False, group)) else ""

    where = lambda condition: " WHERE {0}".format(condition)

    limit = lambda lim, offset=0: " LIMIT {0} OFFSET {1}".format(lim, offset) if lim else ""

    order_by = lambda order: " ORDER BY {0} DESC".format(order[1::]) if order[0] == '-' else ' ORDER BY {0} ASC'.format(
        order)

    def __init__(self, name_table, *select_arg):
        if name_table == "*":
            raise ValueError(f"{name_table} не может иметь название *")

        ObjSelect.reqSql = ""
        ObjSelect.reqSql += ObjSelect.select(select_arg, name_table)
        self.reqSql = ObjSelect.reqSql

    class Join:
        def __init__(self, name_table: str, ON: str, leftJoin: bool = False):

            if not leftJoin:
                ObjSelect.reqSql += "INNER JOIN {0} ON {1}".format(name_table, ', '.join(ON)) \
                    if type(ON) == tuple \
                    else "INNER JOIN {0} ON {1}".format(name_table, ON)

            else:
                ObjSelect.reqSql += "LEFT JOIN {0} ON {1}".format(name_table, ', '.join(ON))

            self.reqSql = ObjSelect.reqSql

        class OrderBy:  # +
            def __init__(self, sqlORDER_BY):
                ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                self.reqSql = ObjSelect.reqSql

            class Limit:
                def __init__(self, end: int, offset: int = 0):
                    ObjSelect.reqSql += ObjSelect.limit(end, offset)
                    self.reqSql = ObjSelect.reqSql

        class Limit:  # +
            def __init__(self, end: int, offset: int = 0):
                ObjSelect.reqSql += ObjSelect.limit(end, offset)
                self.reqSql = ObjSelect.reqSql

        class GroupBy(Limit, OrderBy):
            def __init__(self, *name_column):
                super().__init__(None)
                ObjSelect.reqSql += ObjSelect.group_by(name_column)
                self.reqSql = ObjSelect.reqSql

            class OrderBy:
                def __init__(self, sqlORDER_BY):
                    ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                    self.reqSql = ObjSelect.reqSql

                class Limit:
                    def __init__(self, end: int, offset: int = 0):
                        ObjSelect.reqSql += ObjSelect.limit(end, offset)
                        self.reqSql = ObjSelect.reqSql

        class Where(GroupBy, Limit, OrderBy):
            def __init__(self, sqlWhere: str):
                super().__init__(None)
                ObjSelect.reqSql += ObjSelect.where(sqlWhere)
                self.reqSql = ObjSelect.reqSql

            class OrderBy:  # +
                def __init__(self, sqlORDER_BY):
                    ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                    self.reqSql = ObjSelect.reqSql

                class Limit:
                    def __init__(self, end: int, offset: int = 0):
                        ObjSelect.reqSql += ObjSelect.limit(end, offset)
                        self.reqSql = ObjSelect.reqSql

            class Limit:  # +
                def __init__(self, end: int, offset: int = 0):
                    ObjSelect.reqSql += ObjSelect.limit(end, offset)
                    self.reqSql = ObjSelect.reqSql

            class GroupBy(Limit, OrderBy):
                def __init__(self, *name_column):
                    super().__init__(None)
                    ObjSelect.reqSql += ObjSelect.group_by(name_column)
                    self.reqSql = ObjSelect.reqSql

                class OrderBy:
                    def __init__(self, sqlORDER_BY):
                        ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                        self.reqSql = ObjSelect.reqSql

                    class Limit:
                        def __init__(self, end: int, offset: int = 0):
                            ObjSelect.reqSql += ObjSelect.limit(end, offset)
                            self.reqSql = ObjSelect.reqSql

    class OrderBy:  # +
        def __init__(self, sqlORDER_BY):
            ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
            self.reqSql = ObjSelect.reqSql

        class Limit:
            def __init__(self, end: int, offset: int = 0):
                ObjSelect.reqSql += ObjSelect.limit(end, offset)
                self.reqSql = ObjSelect.reqSql

    class Limit:  # +
        def __init__(self, end: int, offset: int = 0):
            ObjSelect.reqSql += ObjSelect.limit(end, offset)
            self.reqSql = ObjSelect.reqSql

    class GroupBy(Limit, OrderBy):  # +
        def __init__(self, *name_column):
            super().__init__(None)
            ObjSelect.reqSql += ObjSelect.group_by(name_column)
            self.reqSql = ObjSelect.reqSql

        class OrderBy:
            def __init__(self, sqlORDER_BY):
                ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                self.reqSql = ObjSelect.reqSql

            class Limit:
                def __init__(self, end: int, offset: int = 0):
                    ObjSelect.reqSql += ObjSelect.limit(end, offset)
                    self.reqSql = ObjSelect.reqSql

    class Where(GroupBy, Limit, OrderBy):
        def __init__(self, sqlWhere: str):
            super().__init__(None)
            ObjSelect.reqSql += ObjSelect.where(sqlWhere)
            self.reqSql = ObjSelect.reqSql

        class OrderBy:  # +
            def __init__(self, sqlORDER_BY):
                ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                self.reqSql = ObjSelect.reqSql

            class Limit:
                def __init__(self, end: int, offset: int = 0):
                    ObjSelect.reqSql += ObjSelect.limit(end, offset)
                    self.reqSql = ObjSelect.reqSql

        class Limit:  # +
            def __init__(self, end: int, offset: int = 0):
                ObjSelect.reqSql += ObjSelect.limit(end, offset)
                self.reqSql = ObjSelect.reqSql

        class GroupBy(Limit, OrderBy):
            def __init__(self, *name_column):
                super().__init__(None)
                ObjSelect.reqSql += ObjSelect.group_by(name_column)
                self.reqSql = ObjSelect.reqSql

            class OrderBy:
                def __init__(self, sqlORDER_BY: str):
                    ObjSelect.reqSql += ObjSelect.order_by(sqlORDER_BY)
                    self.reqSql = ObjSelect.reqSql

                class Limit:
                    def __init__(self, end: int, offset: int = 0):
                        ObjSelect.reqSql += ObjSelect.limit(end, offset)
                        self.reqSql = ObjSelect.reqSql
