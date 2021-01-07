import math
import json
import pickle
import msgpack

class Share:
    @property
    def area(self):
        raise NotImplementedError("基类未实现")

class Triangle(Share):
    def __init__(self,b,h):
        self.b = b
        self.h = h

    @property
    def area(self):
        return (self.b * self.h) / 2

class Rectangle(Share):
    def __init__(self, l, w):
        self.l = l
        self.w = w

    @property
    def area(self):
        return self.l * self.w

class Circular(Share):
    def __init__(self, r):
        self.r = r

    @property
    def area(self):
        return (self.r * self.r) * math.pi

class SerializeMixin:

    def dump(self, t = 'json'):
        if t == 'json':
            return json.dumps(self.__dict__)
        elif t == 'msgpack':
            return msgpack.dumps(self.__dict__)
        elif t == 'pickle':
            return pickle.dumps(self.__dict__)
        else:
            raise NotImplementedError('未实现的序列化')

class SerializableCircleMixin(SerializeMixin,Circular): pass

scm = SerializableCircleMixin(4)
print(scm.dump('json'))

