
class Field:
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False):
        self.name = name
        if fieldname is None:
            self.fieldname = name
        else:
            self.fieldname = fieldname
        self.pk = pk
        self.unique = unique
        self.default = default
        self.nullable = nullable
        self.index = index

    def validate(self, value):
        raise NotImplementedError()

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        self.validate(value)
        instance.__dict__[self.name] = value

    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self.name)

    __repr__ = __str__

class IntField(Field):
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, auto_increment=False):
        self.auto_increment = auto_increment
        super().__init__(name, fieldname, pk, unique, default, nullable, index)

    def validate(self, value):
        if value is None:
            if self.pk:
                raise TypeError("".format(self.name, value))
            if not self.nullable:
                raise TypeError()
        else:
            if not isinstance(value, int):
                raise TypeError()

class StringField(Field):
    def __init__(self, length=32, name=None, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False):
        self.length = length
        super().__init__(name, fieldname, pk, unique, default, nullable, index)

    def validate(self, value):
        if value is None:
            if self.pk:
                raise TypeError("".format(self.name, value))
            if not self.nullable:
                raise TypeError()
        else:
            if not isinstance(value, str):
                raise TypeError()
            if len(value) > self.length:
                raise ValueError("{} is too long. value={}".format(self.name, value))

class Student:
    id = IntField('id')
    name = StringField(64, 'name')
    age = IntField('age')

    def __init__(self, id, name, age):
        self.id = id
        self.name = name
        self.age = age













