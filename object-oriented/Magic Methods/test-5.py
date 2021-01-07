import time
import datetime
from functools import wraps

class TimeIt:
    """This is a Class"""
    def __init__(self, fn=None):
        if fn is not None:
            self.fn = fn
            wraps(fn)(self)

    def __enter__(self):
        self.start = datetime.datetime.now()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.delta = (datetime.datetime.now() - self.start).total_seconds()
        print('{} tool {}s. context'.format(self.fn.__name__, self.delta))
        pass

    def __call__(self, *args, **kwargs):
        self.start = datetime.datetime.now()
        ret = self.fn(*args, **kwargs)
        self.delta = (datetime.datetime.now() - self.start).total_seconds()
        print('{} took {}s. call'.format(self.fn.__name__, self.delta))
        return ret

@TimeIt
def add(x, y):
    """This is add function"""
    time.sleep(2)
    return x + y

class dispatcher:

    def reg(self, cmd, fn):
        if isinstance(cmd, str):
            setattr(type(self), cmd, fn)
        else:
            print('error')

    def run(self):
        while True:
            cmd = input("plz input command: ")
            if cmd.strip() == 'quit':
                return
            getattr(self, cmd.strip(), self.defaultfn)()

    def defaultfn(self):
        print("default function")


class Base:
    m = 6

# class A(Base):
#     n = 6
#     def __init__(self, x):
#         self.x = x
#
#     def __getattribute__(self, item):
#         raise AttributeError(item)
#         print('__getattribute__', item)
#
#     def __getattr__(self, item):
#         print('__getattr__', item)
#
#     def __setattr__(self, key, value):
#         print('__setattr__', key, value)
#
#     def __delattr__(self, item):
#         print('delattr')


# class A:
#     def __init__(self):
#         print('A.init')
#         self.a1 = 'a1'
#
#     def __get__(self, instance, owner):
#         print('A.__get__', self, instance, owner)
#         return self
#
#     def __set__(self, instance, value):
#         print('A.__set__', self, instance, value)
#
# class B:
#     x = A()
#
#     def __init__(self):
#         print('B.init')
#         self.x = A()
#
# print(B.x.a1)
# print()
#
# b = B()
# print(B.x)
# print(b.x.a1)


class Item:
    def __init__(self, **kwargs):
        self.__spec = kwargs

    def get(self):
        print(self.__spec.items())

# Item(mark='123',color='123',memory='4G').get()
import os

