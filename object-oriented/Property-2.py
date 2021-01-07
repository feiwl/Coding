

class Property:
    def __init__(self, fget=None, fset=None, fdel=None, doc=None):
        self.fget = fget
        self.fset = fset
        self.fdel = fdel

    def __str__(self):
        return 'fget={}, gset={}, fdel={}'.format(self.fget, self.fset, self.fdel)

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return self.fget(instance)

    def __set__(self, instance, value):
        if self.fset is None:
            raise AttributeError("can't set attribute")
        self.fset(instance, value)

    def __delete__(self, instance):
        if self.fdel is None:
            raise AttributeError("can't delete attribute")
        self.fdel(instance)

    def setter(self, fset):
        if fset.__name__ != self.fget.__name__:
            return Property(fset=fset)
        self.fset = fset
        return self

    def deleter(self, fdel):
        if fdel.__name__ != self.fget.__name__:
            return Property(fdel=fdel)
        self.fdel = fdel
        return self

class A:
    def __init__(self, x):
        self._data = x

    @Property
    def data(self):
        return self._data

    @data.setter
    def data(self, value):
        self._data = value

a = A(100)
print(a.data)
a.data = 200
print(a.data)
