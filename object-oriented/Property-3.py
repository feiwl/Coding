
class Property:
    def __init__(self, fget, fset=None):
        self.fget = fget
        self.fset = fset

    def __get__(self, instance, owner):
        if instance is not None:
            return self.fget(instance)
        return self

    def __set__(self, instance, value):
        if callable(self.fset):
            self.fset(instance, value)
        else:
            raise AttributeError()

    def setter(self, fn):
        self.fset = fn
        return self


class A:
    def __init__(self, data):
        self._data = data

    @Property # data = Property(data) => data = obj
    def data(self):
        return self._data

    @data.setter # data = data.setter(data) => data = fn
    def data(self, value):
        self._data = value


"""
1. data = Property(data) => data = obj, self.fget = data  return self
2. data = data.setter(data) => data = obj, self.fset = fn  return self
3. print(a.data) => __get__ => self.fget(instance, )
"""

a = A(100)
print(a.data)
a.data = 200
print(a.data)
