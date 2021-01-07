
class Property:
    """
    Object Decorator
    """
    def __init__(self, fget, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel

    def __get__(self, instance, owner):
        if instance is not None:
            return self.fget(instance)
        return self

    def __set__(self, instance, value):
        if callable(self.fset):
            self.fset(instance, value)
        return self

    def __delete__(self, instance):
        if callable(self.fdel):
            self.fdel(instance)
        raise AttributeError(self.fdel)

    def setter(self, value):
        self.fset = value
        return self

    def delete(self, fdel):
        self.fdel = fdel
        return self

class A:
    def __init__(self, data):
        self._data = data

    @Property
    def data(self): # data = Property(data) data = obj
        return self._data

    @data.setter
    def data(self, value): # data = data.setter(data) data = obj
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
