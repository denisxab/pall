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


class Select:

    def __init__(self, name_table, *select_arg, req=None):

        self.Request: str = ""
        if req:
            self.Request += req

        if name_table:
            if name_table == "*":
                raise ValueError(f"{name_table} не может иметь название *")
            self.Request = 'SELECT {0} FROM {1}'.format(', '.join(select_arg), name_table)

    def Join(self, name_table: str, ON: str, leftJoin: bool = False):
        if not leftJoin:
            self.Request += " INNER JOIN {0} ON {1}".format(name_table, ', '.join(ON)) \
                if type(ON) == tuple \
                else " INNER JOIN {0} ON {1}".format(name_table, ON)
        else:
            self.Request += " LEFT JOIN {0} ON {1}".format(name_table, ', '.join(ON))
        return Select("", "", req=self.Request)

    def GroupBy(self, *name_column):
        self.Request += " GROUP BY {0}".format(', '.join(name_column)) if list(
            filter(lambda it: True if it else False, name_column)) else ""
        return Select("", "", req=self.Request)

    def OrderBy(self, NameColumn: str):
        self.Request += f" ORDER BY {NameColumn[1::]} DESC" if NameColumn[0] == '-' else f' ORDER BY {NameColumn} ASC'
        return Select("", "", req=self.Request)

    def Where(self, sqlWhere: str):
        self.Request += f" WHERE {sqlWhere}"
        return Select("", "", req=self.Request)

    def Limit(self, end: int, offset: int = 0):
        self.Request += f" LIMIT {end} OFFSET {offset}" if end else ""
        return Select("", "", req=self.Request)
