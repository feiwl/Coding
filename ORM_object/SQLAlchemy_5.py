from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, BigInteger, Boolean, Float, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import and_, or_, not_
from sqlalchemy import func
import sqlalchemy

host = '192.168.10.68'
user = 'nas'
port = 3306
password = 'Prism@123456'
database = 'marketdata'

conn_str = "mysql+pymysql://{}:{}@{}:{}/{}".format(
    user, password, host, port, database
)

engine = sqlalchemy.create_engine(conn_str, echo=True)
Base = declarative_base()

class Symbol(Base):
    __tablename__ = 'symbol'

    szWindCode = Column(String(15), index=True, nullable=False, primary_key=True)
    codeId = Column(Integer, nullable=False, autoincrement=True)

    def __repr__(self):
        return "<classname={} szWindCode={} codeId={}>".format(self.__class__.__name__, self.szWindCode, self.codeId)

    __str__ = __repr__

class Transaction(Base):
    __tablename__ = 'transaction'

    codeId = Column(Integer, ForeignKey('symbol.codeId', ondelete='CASCADE', onupdate='RESTRICT'), nullable=False, default=0, primary_key=True)
    nActionDay = Column(Date, nullable=False, default='0000-00-00', primary_key=True)
    nTime = Column(Integer, nullable=False, default=0, primary_key=True)
    nOpen = Column(Integer, default=None)
    nHigh = Column(Integer, default=None)
    nLow = Column(Integer, default=None)
    nMatch = Column(Integer, default=None)
    iVolume = Column(Integer, default=None)
    iTurnover = Column(BigInteger, default=None)
    nNumTrades = Column(Integer, default=None)
    barClose = Column(Boolean, default=None)
    S_DQ_PRECLOSE = Column(Integer, default=None)
    S_DQ_ADJFACTOR = Column(Float, default=None)
    HighLimit = Column(Integer, default=None)
    LowLimit = Column(Integer, default=None)

    tran_sym = relationship('Symbol')

    def __repr__(self):
        return "<classname={} codeId={} nAtctionDay={} " \
               "nTime={} nOpen={} nHigh={} nLow={} " \
               "nMatch={} iVolume={} iTurnover={} " \
               "nNumTrades={} barClose={} S_DQ_PRECLOSE={} " \
               "S_DQ_ADJFACTOR={} HighLimit={} LowLimit={}>".format(
            self.__class__.__name__, self.codeId, self.nActionDay,
            self.nTime, self.nOpen, self.nHigh, self.nLow, self.nMatch,
            self.iVolume, self.iTurnover, self.nNumTrades, self.barClose,
            self.S_DQ_PRECLOSE, self.S_DQ_ADJFACTOR, self.HighLimit,
            self.tran_sym, self.LowLimit
        )

    __str__ = __repr__

Session = sessionmaker(bind=engine)
session = Session()

def show(emps):
    for line in emps:
        print(line)

# simple query
# transactions = session.query(Transaction).get((1,'2019-01-02', '92500000'))
# print(transactions)

# simple condition
# emps = session.query(Transaction).filter(Transaction.codeId <= 5).filter(Transaction.nActionDay == '2019-01-02').filter(Transaction.nTime == 92500000)
# show(emps)

# emps = session.query(Transaction).filter((Transaction.codeId <=5), (Transaction.nActionDay == '2019-01-02'), (Transaction.nTime == 92500000))
# show(emps)

# AND condtion
# emps = session.query(Transaction).filter(and_(Transaction.codeId <=5, Transaction.nActionDay == '2019-01-02', Transaction.nTime == 92500000))
# show(emps)

# emps = session.query(Transaction).filter((Transaction.codeId <=5) & (Transaction.nActionDay == '2019-01-02') & (Transaction.nTime == 92500000))
# show(emps)

# OR condtion
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) | (Transaction.codeId <= 10),((Transaction.nActionDay == '2019-01-02') & (Transaction.nTime == 92500000)))
# show(emps)

# emps = session.query(Transaction).filter(or_(Transaction.codeId <= 5, Transaction.codeId <=10), ((Transaction.nActionDay == '2019-01-02') & (Transaction.nTime == '92500000')))
# show(emps)

# Not condtion
# emps = session.query(Transaction).filter((Transaction.codeId <= 5), (Transaction.nActionDay == '2019-01-02'), (Transaction.nTime == 92500000)).filter(not_(Transaction.codeId <= 1))
# show(emps)

# emplist = [1,2,5]
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).filter(Transaction.codeId.in_(emplist))
# show(emps)

# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).filter(Transaction.codeId.notin_(emplist))
# show(emps)

# sort sentence
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).order_by(Transaction.codeId.asc())
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).order_by(Transaction.codeId.desc())
# show(emps)

# sorts list sentenct
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).order_by(Transaction.codeId.desc(), Transaction.nActionDay)
# show(emps)

# piging
# emps = session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).limit(5).offset(0)
# show(emps)

# consumer method
# emps = session.query(Transaction).filter((Transaction.codeId <= 15) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000))
# print(len(list(emps)))
# print(emps.count())
# print(emps.all())
# print(emps.limit(1).one())

# delete by query
# session.query(Transaction).filter((Transaction.codeId <= 5) ,(Transaction.nActionday == '2019-01-02') ,(Transaction.nTime == 92500000)).delete()
# session.commit() # Delete if submitted

# Count, grouping
# query =  session.query(func.count(Transaction.codeId)).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000))
# show(query)
# print(query.one())
# print(query.scalar())

# max/min/avg
# print(session.query(func.max(Transaction.codeId)).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).scalar())
# print(session.query(func.min(Transaction.codeId)).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).scalar())
# print(session.query(func.avg(Transaction.codeId)).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).scalar())

# grouping
# print(session.query(func.count(Transaction.codeId)).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).group_by(Transaction.codeId).all())

# Association query
# results = session.query(Transaction, Symbol).filter(Transaction.codeId == Symbol.codeId).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).all()
# SELECT * FROM transaction, symbol where transaction.codeId = symbol.codeId and transaction.codeId <= 5 and transaction.nActionDay == '2019-01-02' and transaction.nTime == 92500000;
# for i in results:
#     print(i)

# Using join
# results  = session.query(Transaction).join(Symbol).filter(Transaction.codeId == Symbol.codeId).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000)).all()
# results = session.query(Transaction).join(Symbol, Transaction.codeId == Symbol.codeId).filter((Transaction.codeId <= 5) ,(Transaction.nActionDay == '2019-01-02') ,(Transaction.nTime == 92500000))
# # show(results)