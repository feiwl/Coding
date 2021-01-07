import pymysql

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
        raise NotImplementedError

    def __get__(self, instance, owner):
        if instance is None:
            return self
        return instance.__dict__[self.name]

    def __set__(self, instance, value):
        self.validate(value)
        instance.__dict__[self.name] = value

class StringField(Field):
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, length=None):
        super().__init__(name, fieldname, pk, unique, default, nullable, index)
        self.length = length

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

class IntField(Field):
    def __init__(self, name, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, auto_increment=False):
        super().__init__(name, fieldname, pk, unique, default, nullable, index)
        self.auto_increment = auto_increment

    def validate(self, value):
        if value is None:
            if self.pk :
                raise TypeError("{} is pk, not None".format(value))
            if not self.nullable:
                raise TypeError("{} required".format(self.name))
        else:
            if not isinstance(value, int):
                raise TypeError("{} should be integer".format(self.name))

class Session:
    def __init__(self, conn:pymysql.connections.Connection):
        self.conn = conn

    def execute(self, query, *args):
        try:
            with self.conn as cursor:
                with cursor:
                    cursor.execute(query, args)
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

    def __str__(self):
        return "Student({}, {}, {})".format(self.id, self.name, self.age)

    __repr__ = __str__

    def save(self, session:Session):
        sql = "insert into student (id, name, age) values (%s,%s,%s)"
        with session as cursor:
            with cursor:
                cursor.execute(sql, (self.id, self.name, self.age))
