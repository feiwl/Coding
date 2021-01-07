import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Enum, inspect
from sqlalchemy.orm import sessionmaker

# 实体基类
Base = declarative_base() # mapping

# 实体类
class Student(Base):
    __tablename__ = 'student'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(64), nullable=False)
    age = Column(Integer)

    def __repr__(self):
        return "<{} id:{} name:{} age:{}>".format(self.__class__.__name__, self.id, self.name, self.age)

    __str__ = __repr__

host = '192.168.10.101'
port = 3306
user = 'root'
password = '123456'
database = 'test'

conn_str = "mysql+pymysql://{}:{}@{}:{}/{}".format(user, password, host, port, database)
engine = sqlalchemy.create_engine(conn_str, echo=True)

# 创建表
# Base.metadata.create_all(engine)
# Base.metadata.drop_all(engine)

Session = sessionmaker() # class
session = Session(bind=engine)  # instance

def show(entity):
    ins = inspect(entity)
    print(ins.transient, ins.pending, ins.persistent, ins.deleted, ins.detached)

try:
    student = session.query(Student).get(3)
    # student = Student()
    # student.id = 2
    # student = Student()
    # student.id = 10
    # student.name = 'tom10'
    # student.age = 32
    # session.add(student)
    # print(student.id)
    #
    # session.commit()

    session.delete(student)
    show(student)
    session.flush()
    show(student)
    # session.commit()

    # print(student.id)
    # session.add(student)
    # student.age = 42
    # session.commit()
    # queryobj = session.query(Student).filter(Student.id == 2)
    # for x in queryobj:
    #     print(x)
    session.rollback()
    show(student)
except Exception as e:
    print(e)
    session.rollback()
finally:
    pass
