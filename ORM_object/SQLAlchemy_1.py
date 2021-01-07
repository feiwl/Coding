import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import sessionmaker

Base = declarative_base()

class Teacher(Base):
    __tablename__ = 'teacher'

    id = Column(Integer, primary_key=True, nullable=False)
    name = Column(String(64), nullable=False)
    age = Column(Integer, nullable=False)

    def __repr__(self):
        return "<Teacher(classname={}, name={}, id={}, age={})>".\
            format(self.__class__.__name__, self.name, self.id, self.age)


host = '192.168.10.101'
port = 3306
user = 'root'
password = '123456'
database = 'test'

conn_str = "mysql+pymysql://{}:{}@{}:{}/{}".format(user, password, host, port, database)
engine = sqlalchemy.create_engine(conn_str, echo=True)

# Base.metadata.create_all(engine)
# Base.metadata.drop_all(engine)

Session = sessionmaker() # class
session = Session(bind=engine) # instance

try:
    # student = Student()
    student = Teacher(name = 'jerry')
    student.name = 'tom'
    student.age = 20

    session.add(student)  # pending
except Exception as e:
    session.rollback()
finally:
    pass