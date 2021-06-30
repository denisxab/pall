class Ptr:

    def __init__(self, data=None):
        self.pt = data

    def __getattribute__(self, item):
        if item == "pt":
            return self.__dict__[item][0]
        else:
            return super().__getattribute__(item)

    def __setattr__(self, key, value):
        if key == "pt":
            self.__dict__[key] = [value]
        else:
            super().__setattr__(key, value)
            self.__dict__[key] = value

    def __del__(self):
        self.__dict__['pt'].clear()


