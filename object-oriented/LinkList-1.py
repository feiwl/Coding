
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

    def add(self, num):
        node = Node(num)
        if self.tail is None:
            self.head = node
        else:
            self.tail.next = node
            node.prev = self.tail
        self.tail = node


    def __getitem__(self, item):
        if item >= 0:
            for k, current in enumerate(self):
                if k == item:
                    return current
            raise KeyError(item)

    def __iter__(self):
        current = self.head
        while current:
            yield current
            current = current.next

    def __setitem__(self, key, value):
        node = Node(value)
        if key >= 0:
            for k,current in enumerate(self):
                if k == key:
                    if current.prev is None:
                        node.next = current
                        current.prev = node
                        self.head = node
                        break
                    elif current.next is None:
                        prev = current.prev
                        node.prev = prev
                        node.next = current
                        prev.next = node
                        current.prev = node
                        break
                    else:
                        prev = current.prev
                        node.next = current
                        node.prev = prev
                        prev.next = node
                        current.prev = node
                        break
            else:
                self.add(value)



ll = LinkList()

ll.add(1)
ll.add(2)
ll.add(3)

ll[3] = 4
ll[1] = 5
ll[0] = 6
for i in enumerate(ll):
    print(i)



