import sqlalchemy
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, Date, Boolean, Float, ForeignKey, Index
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy import func
from sqlalchemy import or_, not_, and_

host = '192.168.10.68'
user = 'nas'
password = 'Prism@123456'
port = 3306
database = 'marketdata'

conn_str = "mysql+pymysql://{}:{}@{}:{}/{}".format(user, password, host, port, database)
engine = sqlalchemy.create_engine(conn_str, echo=True)

Base = declarative_base()

def show(emps):
    for x in emps:
        print(x)
    print('~~~~~~~~~', end='\n\n')

class symbol(Base):
    __tablename__ = 'symbol'

    szWindCode = Column(String(15), nullable=False, primary_key=True)
    codeId = Column(Integer, autoincrement=True, index=True)

    def __repr__(self):
        return "<class={} szWindCode={} codeId={}>".format(self.__class__.__name__, self.szWindCode, self.codeId)

class transaction(Base):
    __tablename__ = 'transaction'

    codeId = Column(Integer, ForeignKey('symbol.codeId', ondelete='CASCADE'),nullable=False, default=0, primary_key=True)
    nActionDay = Column(Date, nullable=False, default='0000-00-00', primary_key=True)
    nTime = Column(Integer, nullable=False, default=0, primary_key=True)
    nOpen = Column(Integer, nullable=True, default=None)
    nHigh = Column(Integer)
    nLow = Column(Integer)
    nMatch = Column(Integer)
    iVolume = Column(Integer)
    nNumTrades = Column(Integer)
    barClose = Column(Boolean)
    S_DQ_PRECLOSE = Column(Integer)
    S_DQ_ADJFACTOR = Column(Float)
    HighLimit = Column(Integer)
    LowLimit = Column(Integer)

    symbol_tran = relationship('symbol')

    def __repr__(self):
        return "<class={} codeId={} nActionDay={} " \
               "nTime={} nOpen={} nHigh={} nLow={} " \
               "nMatch={} iVolume={} nNumTrades={} " \
               "barClose={} S_DQ_PRECLOSE={} S_DQ_ADJFACTOR={} " \
               "HighLimit={} LowLimit={} symbol_tran={}>".format(
            self.__class__.__name__, self.codeId, self.nActionDay,
            self.nTime, self.nOpen, self.nHigh, self.nLow,
            self.nMatch, self.iVolume, self.nNumTrades,
            self.barClose, self.S_DQ_PRECLOSE, self.S_DQ_ADJFACTOR,
            self.HighLimit, self.LowLimit, self.symbol_tran

        )

    __str__ = __repr__

Session = sessionmaker(bind = engine)
session = Session()

try:
    tran = transaction
    # AND condition
    # emps = session.query(tran).filter((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 1))
    # emps = session.query(tran).filter(tran.nActionDay == '2019-01-02').filter(tran.nTime == 92500000).filter(tran.codeId == 1)

    # OR condition
    # emps = session.query(tran).filter(((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 1))
    #                                   | ((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 2)))
    # emps = session.query(tran).filter(or_(((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 1)),
    #                                   ((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 2))))

    # order_by condition
    # emps = session.query(tran).filter(tran.nActionDay == '2019-01-02').filter(tran.nTime == 92500000).filter(tran.codeId < 100).order_by(tran.codeId.desc()).limit(8).offset(10)
    # print(session.query(func.count(tran.nActionDay)).filter((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId < 50)).group_by(tran.codeId).all())

    # results = session.query(tran, symbol).filter(tran.codeId, symbol.codeId).filter((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId < 50)).all()
    # show(results)

    results = session.query(tran).join(symbol).filter((tran.nActionDay == '2019-01-02') & (tran.nTime == 92500000) & (tran.codeId == 1)).all()
    print(results)

except:
    session.rollback()
    raise

