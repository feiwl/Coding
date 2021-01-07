import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Enum, inspect, ForeignKey
from sqlalchemy.orm import sessionmaker
from enum import Enum

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
    for x in emps:
        print(x)
    print('~~~~~~~~~~~~~~~~~~~', end='\n\n')


class MyEnum(Enum):
    M = 'M'
    F = 'F'

"""
CREATE TABLE `employees`(
  `emp_no` int(11) NOT NUILL,
  `birth_date` date NOT NULL,
  `first_name` varchar(14) NOT NULL,
  `last_name` varchar(16) NOT NULL,
  `gender` enum('M', 'F') NOT NULL,
  `hire_date` date NOT NULL,
  PRIMARY KEY (`emp_no`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

class Employee(Base):
    __tablename__ = 'employees'

    emp_no = Column(Integer, primary_key=True)
    birth_date = Column(Date, nullable=False)
    first_name = Column(String(14), nullable=False)
    last_name = Column(String(16), nullable=False)
    # gender = Column(MyEnum, nullable=False)
    hire_date = Column(Date, nullable=False)

    def __repr__(self):
        return "<{} emp_no:{} name:{}>".format(self.__class__.__name__, self.emp_no, "{}.{}".format(self.first_name, self.last_name))

    __str__ = __repr__

# 简单条件查询
emps = session.query(Employee).filter(Employee.emp_no > 10015)
show(emps)

# 与或非
from sqlalchemy import or_ , not_, and_

# AND 条件
emps = session.query(Employee).filter(Employee.emp_no > 10015).filter(Employee.gender == MyEnum.F)
show(emps)

emps = session.query(Employee).filter(and_(Employee.emp_no > 10015, Employee.gender == MyEnum.M))
show(emps)

# & 一定要注意&符号两边表达式都要加括号
emps = session.query(Employee).filter((Employee.emp_no > 10015) & (Employee.gender == MyEnum.M))
show(emps)

# OR 条件
emps = session.query(Employee).filter((Employee.emp_no > 10018) | (Employee.emp_no < 10003))
show(emps)

emps = session.query(Employee).filter(or_(Employee.emp_no > 10018, Employee.emp_no < 10003))
show(emps)

# Not
emps = session.query(Employee).filter(not_(Employee.emp_no < 10018))
show(emps)
# 一定要注意加括号
emps = session.query(Employee).filter(~(Employee.emp_no < 10018))
show(emps)

# in
emplist = [10010, 10015, 10018]
emps = session.query(Employee).filter(Employee.emp_no.in_(emplist))
show(emps)

# not in
emps = session.query(Employee).filter(~Employee.emp_no.in_(emplist))
show(emps)

# like
emps = session.query(Employee).filter(Employee.last_name.like('P%'))
show(emps)