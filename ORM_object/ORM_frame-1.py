import pymysql

class Field:
    def __init__(self, name=None, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False):
        self.name = name
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

    def __str__(self):
        return "<{} {}>".format(self.__class__.__name__, self.name)

    __repr = __str__

class StringField(Field):
    def __init__(self, name=None, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, length=None):
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
    def __init__(self, name=None, fieldname=None, pk=False, unique=False, default=None, nullable=True, index=False, auto_increment=False):
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

class Session:  # 这是线程不安全的
    def __init__(self, conn:pymysql.connections.Connection):
        self.conn = conn
        self.cursor = None

    def execute(self, query, *args):
        if self.cursor is None:
            self.cursor = self.conn.cursor()
        self.cursor.execute(query, args)

    def __enter__(self):
        self.cursor = self.conn.cursor()   # 多线程调用一个对象会被覆盖
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):

        if exc_type:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.cursor.close()

class ModelMeta(type):
    def __new__(cls, name:str, bases, attrs:dict):

        # 解决表名的问题
        if attrs.get('__tablename__', None) is None:
            attrs['__tablename__'] = name.lower()

        mapping = {}
        primarykey = []
        for k,v in attrs.items():
            if isinstance(v, Field):
                mapping[k] = v
                if v.name is None:
                    v.name = k
                if v.fieldname is None:
                    v.fieldname = v.name
                if v.pk:
                    primarykey.append(v)
        attrs['__mapping__'] = mapping
        attrs['primarykey'] = primarykey

        return super().__new__(cls, name, bases, attrs)

class Model(metaclass=ModelMeta):pass

class Student(Model):
    __tablename__ = 'stu'
    id = IntField(pk=True, nullable=False, auto_increment=True)
    name = StringField('name', nullable=False, length=64)
    age = IntField()

    def __init__(self, id, name, age):
        self.id = id
        self.name = name
        self.age = age

    def __str__(self):
        return "Student({}, {}, {})".format(self.id, self.name, self.age)

    __repr__ = __str__


class Engine:
    def __init__(self, *args, **kwargs):
        self.conn = pymysql.connect(*args, **kwargs)

    def save(self, instance:Student): # insert into
        names = []
        values = []
        for k,v in instance.__mapping__.items():
            if isinstance(v, Field):
                names.append(k)
                values.append(instance.__dict__[k])

        sql = "insert into student (id, name, age) values (%s, %s, %s)"
        sql = "insert into {} ({}) values ({})".format(
            instance.__tablename__,
            ",".join(names),
            ",".join(['%s']*len(names))
        )
        print(sql)
        print(values)

        # with session:
        #   session.execute(sql, values)

s = Student(5, 'jerry', 20)
engine = Engine('192.168.10.101', 'root', '123456', 'test')
engine.save(s)
