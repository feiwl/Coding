

class SingleNode:
    def __init__(self, val, next=None):
        self.val = val
        self.next = next

    def __repr__(self):
        return str(self.val)

class LinkedList:
    def __init__(self):
        self.head = None
        self.tail = None

    def append(self, val):
        node = SingleNode(val)
        if self.tail is None:
            self.head = node
        else:
            self.tail.next = node
        self.tail = node

    def iternodes(self):
        current = self.head
        while current:
            yield current
            current = current.next

    def __hash__(self):
        return 1

    def __eq__(self, other): # 1. is 2. ==
        return True

class A:
    def __init__(self, x):
        self.x = x

    def __sub__(self, other):
        pass

    def __lt__(self, other):
        return self.x < other.x

    def __eq__(self, other):
        return self.x == other.x

    def __repr__(self):
        return str(self.x)

    def __iadd__(self, other):
        return A(self.x + other.x)

class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __eq__(self, other):
        if self is other:
            return True
        return self.x == other.x and self.y == other.y

    def __add__(self, other):
        tmpx = self.x + other.x
        tmpy = self.y + other.y
        return Point(tmpx, tmpy)

class Cart:
    def __init__(self):
        self.contains = list()

    def __len__(self):
        return len(self.contains)

    def __iter__(self):
        return iter(self.contains)

    def __getitem__(self, item):
        return self.contains[item]

    def __setitem__(self, key, value):
        self.contains[key] = value
        return self.contains

    def __repr__(self):
        return str(self.contains)


class Fibonacci_sequence:
    def __init__(self):
        self.nums = []

    def __call__(self, t) -> list:
        if isinstance(t, int) and t > 0 :
            while True:
                if t == 1:
                    self.nums.append(1)
                elif t == 2:
                    self.nums.extend([1, 1])
                elif t > 2:
                    if len(self.nums) == 0:
                        self.nums.extend([1, 1])
                    front = len(self.nums) - 2
                    current = len(self.nums) - 1
                    self.nums.append(self.nums[front]+self.nums[current])
                if len(self.nums) == t:
                    return self.nums
        else:
            raise Exception("Error > 0 and Type is Int")

    def __len__(self):
        return len(self.nums)

    def __getitem__(self, index):
        if index < 0:
            return None
        if index < len(self):
            return self.nums[index]

        self(index)

    def __iter__(self):
        return iter(self.nums)

open






