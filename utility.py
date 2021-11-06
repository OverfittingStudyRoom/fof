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


def fd_basicinfo(security_ids=None, trade_dt=None):
    if not trade_dt:
        trade_dt = dt.datetime.today().strftime("%Y%m%d")
    else:
        trade_dt = trade_dt.replace("-", "")

    if isinstance(security_ids, str):
        sec_id_strs = security_ids
    elif not security_ids:
        sec_id_strs = None
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])
        
    if sec_id_strs:
        query = f"""
        select SECURITYID, FDNAME, SNAMECOMP, FSYMBOL, FDNATURE, INVESTSTYLE from TQ_FD_BASICINFO
        WHERE
            ISVALID = 1 AND
            SECURITYID in ({sec_id_strs}) AND
            (LIQUENDDATE >= '{trade_dt}' or LIQUENDDATE = '19000101')
        """
    else:
        query = f"""
        select SECURITYID, FDNAME, SNAMECOMP, FSYMBOL, FDNATURE, INVESTSTYLE from TQ_FD_BASICINFO
        WHERE
            ISVALID = 1 AND
            (LIQUENDDATE >= '{trade_dt}' or LIQUENDDATE = '19000101')
        """
    return read_sql(query)


def fd_typeclass(security_ids, trade_dt=None):
    if not trade_dt:
        trade_dt = dt.datetime.today().strftime("%Y%m%d")
    else:
        trade_dt = trade_dt.replace("-", "")

    if isinstance(security_ids, str):
        sec_id_strs = security_ids
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])

    query = f"""
    select SECURITYID, L1CODE, L1NAME, L2CODE, L2NAME, L3CODE, L3NAME from TQ_FD_TYPECLASS
    WHERE
        ISVALID = 1 AND
        SECURITYID in ({sec_id_strs})
    """
    return read_sql(query)


def fd_hshkiport(security_ids, report_dates_begin):

    if isinstance(security_ids, str):
        sec_id_strs = security_ids
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])

    query = f"""
    select SECODE as SECURITYID, INDCLASSCODE, INDUSTRYCODE, INDUSTRYNAME, REPORTDATE, MVALUE, ACCNETMKTCAP from TQ_FD_HSHKIPORT
    WHERE
        REPORTDATE >= '{report_dates_begin}' AND
        ISVALID = 1 AND
        INDCLASSCODE = '2102' AND
        SECODE in ({sec_id_strs})
    """
    return read_sql(query).sort_values("SECURITYID")


def fd_assetportfolio(security_ids, report_dates_begin):
    # 获取相关组合情况
    if isinstance(security_ids, str):
        sec_id_strs = security_ids
    else:
        sec_id_strs = ",".join(["'" + s + "'" for s in security_ids])

    query = f"""
    SELECT SECURITYID, REPORTDATE, EQUITYINVERTO from TQ_FD_ASSETPORTFOLIO
    WHERE
        REPORTDATE >= '{report_dates_begin}' AND
        ISVALID = 1 AND
        SECURITYID in ({sec_id_strs})
    """
    return read_sql(query).sort_values("SECURITYID")
