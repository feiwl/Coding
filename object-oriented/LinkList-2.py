
class Node:
    def __init__(self, num, prev=None, next=None):
        self.prev = prev
        self.next = next
        self.num = num

    def __repr__(self):
        return str(self.num)

    def __str__(self):
        return str(self.num)

class LinkList:
    def __init__(self):
        self.head = None
        self.tail = None
        self.items = list()

    def add(self, num):
        node = Node(num)
        if self.tail is None:
            self.head = node
        else:
            self.tail.next = node
            node.prev = self.tail
        self.tail = node
        self.items.append(node)

    def iternodes(self, reverse=False):
        current = self.tail if reverse else self.head
        while current:
            yield current
            current = current.prev if reverse else current.next

    def __len__(self):
        return len(self.items)

    __iter__ = iternodes

    def __setitem__(self, key, value):
        self[key].num = value

    def __getitem__(self, index):
        for i, node in enumerate(self.iternodes(False if index >=0 else True), 0 if index >=0 else 1):
            if i == abs(index):
                return node

ll = LinkList()

ll.add(1)
ll.add(2)
ll.add(3)

ll[3] = 4
ll[1] = 5
ll[0] = 6
for i in enumerate(ll):
    print(i)



