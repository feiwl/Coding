from functools import partial


class Property:
    def __init__(self, fn):
        self.fn = fn

    def __get__(self, instance, owner):
        print("__get__", self, instance, owner)
        return self.fn(instance)

    def __set__(self, instance, value):
        print('__set__', self, instance,  value)

    # def setter(self, *args, **kwargs):
    #     print('__set__', self, *args, **kwargs)
    #     return args, kwargs

class Person:
    def __init__(self, x):
        self.x = x

    @Property # nose = Property(nose)
    def nose(self):
        return self.x

    # @nose.setter  # nose = nose.setter(nose)
    # def nose(self, value):
    #     self.x = value

a = Person(10)

a.nose = 20




