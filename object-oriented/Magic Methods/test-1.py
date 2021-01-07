import time
import datetime
from functools import wraps

class TimeIt:
    def __init__(self, fn):
        self._fn = fn
        wraps(fn)(self)

    def __enter__(self):
        print("Enter")
        self.start = datetime.datetime.now()
        return self

    def __call__(self, *args, **kwargs):
        print("Call__")
        return self._fn(*args, **kwargs)

    def __exit__(self, exc_type, exc_val, exc_tb):
        print("Exit")
        delta = (datetime.datetime.now() - self.start).total_seconds()
        print(delta)
        return

@TimeIt # add = TimeIt(add) -> add =  __call__(x,y)
def add(x, y):
    """The is a add function"""
    time.sleep(2)
    return x + y

# print(add(1,2))
print(add.__name__)
print(add.__doc__)
print(add.__dict__)



