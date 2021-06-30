class Ptr:
    def __init__(self, data=None):
        self.data = data

    def __getattribute__(self, item):
        if item == "data":
            return self.__dict__[item][0]
        else:
            return super().__getattribute__(item)

    def __setattr__(self, key, value):
        if key == "data":
            self.__dict__[key] = [value]
        else:
            super().__setattr__(key, value)
            self.__dict__[key] = value

    def __eq__(self, other) -> bool:
        return self.data == other

    def __ne__(self, other) -> bool:
        return self.data != other

    def __le__(self, other):
        return self.data <= other

    def __lt__(self, other):
        return self.data < other

    def __gt__(self, other):
        return self.data > other

    def __ge__(self, other):
        return self.data >= other

    def __del__(self):
        self.__dict__['data'].clear()
