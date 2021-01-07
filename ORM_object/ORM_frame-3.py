import pymysql

class Field:
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False):
        self.name = name
        if fieldname:
            self.fieldname = fieldname
        else:
            self.fieldname = name
        self.pk = pk
        self.unique = unique
        self.default = default
        self.nullable = nullable
        self.index = index

    def validate(self, value):
        raise NotImplementedError

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        self.validate(value)
        instance.__dict__[self.name] = value

class IntField(Field):
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, auto_increment=False):
        self.auto_increment = auto_increment
        super().__init__(name, fieldname, pk, unique, default, nullable, index)

    def validate(self, value):
        if value is None:
            if self.pk :
                raise TypeError("{} is pk, not None".format(value))
            if not self.nullable:
                raise TypeError("{} required".format(self.name))
        else:
            if not isinstance(value, int):
                raise TypeError("{} should be integer".format(self.name))

class StringField(Field):
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, length=32):
        self.length = length
        super().__init__(name, fieldname, pk, unique, default, nullable, index)

    def validate(self, value):
        if value is None:
            if self.pk:
                raise TypeError("{} is pk, not None".format(value))
            if not self.nullable:
                raise TypeError("{} required".format(self.name))
        else:
            if not isinstance(value, str):
                raise TypeError("{} should be string.".format(value))
            if len(value) > self.length:
                raise ValueError('{} is too long.'.format(value))

class Session:
    def __init__(self, conn:pymysql.connections.Connection):
        self.conn = conn

    def execute(self, sql, *args):
        try:
            with self.conn as cursor:
                with cursor:
                    cursor.execute(sql, args)
                    self.conn.commit()
        except:
            self.conn.rollback()

    def __enter__(self):
        return self.conn.cursor()

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()

class Student:
    id = IntField('id', 'id', True, nullable=False, auto_increment=True)
    name = StringField('name', nullable=False, length=64)
    age = IntField('age')

    def __init__(self, id, name, age):
        self.id = id
        self.name = name
        self.age = age

    def save(self):
        pass