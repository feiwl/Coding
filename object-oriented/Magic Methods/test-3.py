from functools import total_ordering

@total_ordering
class A:
    def __init__(self, x):
        self.x = x

    def __eq__(self, other):
        return self.x == other.x

    def __lt__(self, other):
        return self.x < other.x

print(A(1) == A(2))
print(A(1) >= A(2))
print(A(1) <= A(2))
print(A(1) != A(2))


