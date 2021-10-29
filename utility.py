import datetime as dt
import calendar
import sqlalchemy as sa
import pandas as pd

CONN = "mssql+pymssql://readdnds:reader%402021@121.37.138.1:14331/dnds"


def read_sql(query):
    engine = sa.create_engine(CONN, connect_args={"charset": "GBK"})
    return pd.read_sql(query, con=engine)


def nearest_report_date(date):
    if len(date) == 10:
        date = date.replace("-", "")

    mm = int(date[4:6])
    year = int(date[0:4])
    last_year = year

    q = (mm // 3 + 1) * 3
    last = calendar.monthrange(last_year, q)[1]
    return dt.date(last_year, q, last).strftime("%Y%m%d")


def fd_basicinfo(security_ids, trade_dt=None):
    if not trade_dt:
        trade_dt = dt.datetime.today().strftime("%Y%m%d")
    else:
        trade_dt = trade_dt.replace("-", "")

    if isinstance(security_ids, str):
        sec_id_strs = security_ids.split(",")
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])

    engine = sa.create_engine(CONN, connect_args={"charset": "GBK"})
    query = f"""
    select SECURITYID, FDNAME, SNAMECOMP, FSYMBOL from TQ_FD_BASICINFO
    WHERE
        ISVALID = 1 AND
        SECURITYID in ({sec_id_strs}) AND
        (LIQUENDDATE >= '{trade_dt}' or LIQUENDDATE = '19000101')
    """
    return pd.read_sql(query, con=engine)


def fd_typeclass(security_ids, trade_dt=None):
    if not trade_dt:
        trade_dt = dt.datetime.today().strftime("%Y%m%d")
    else:
        trade_dt = trade_dt.replace("-", "")

    if isinstance(security_ids, str):
        sec_id_strs = security_ids.split(",")
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])

    engine = sa.create_engine(CONN, connect_args={"charset": "GBK"})
    query = f"""
    select SECURITYID, L1CODE, L1NAME, L2CODE, L2NAME from TQ_FD_TYPECLASS
    WHERE
        ISVALID = 1 AND
        SECURITYID in ({sec_id_strs})
    """
    return pd.read_sql(query, con=engine)
