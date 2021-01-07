

class Typed:
    def __init__(self, type):
        self.type = type

    def __get__(self, instance, owner):
        pass

    def __set__(self, instance, value):
        print('T.set', self, instance, value)
        if not isinstance(value, self.type):
            raise ValueError(value)

import inspect
class TypeAssert:
    def __init__(self, cls):
        self.cls = cls

    def __call__(self, *args, **kwargs):
        params = inspect.signature(self.cls).parameters
        print(params)
        for name, param in params.items():
            print(name, param.annotation)
            if param.annotation != param.empty:
                setattr(self.cls, name, Typed(param.annotation))
        return self.cls(*args, **kwargs)

@TypeAssert
class Person: # Person = TypeAssert(Person)
    # name = Typed(str)
    # age = Typed(int)

    def __init__(self, name:str, age:int):
        self.name = name
        self.age = age

Person('wudang', 2)




