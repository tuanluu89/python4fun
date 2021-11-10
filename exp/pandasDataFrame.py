import pandas as pd
from typing import Union
from pandas import DataFrame
import matplotlib.pyplot as plt

from exp.sql_class import MsSql
from common.conf import LocalMsSql


def read_data(ob: MsSql):
    """
    :param ob: object of class MsSql
    :return:
    """
    conn = ob.conn()
    fact_sktd_query = """
        select 
            ma_kh cusId,
            ma_cn_pgd branch_code,
            du_no_cuoi_ky amount,
            t24_loan_code product_code,
            ngay_vay start_date,
            ngay_dao_han maturity_date
        from asset.dbo.fact_sktd
        where mis_date_d = '2021-08-03'
            and sector_id between 1000 and 1999
    """
    fact_sktd = pd.read_sql_query(sql=fact_sktd_query, con=conn)

    dim_ld_schedule_define_sql = """
        select * from asset.dbo.dim_ld_schedule_define
        where mis_date >= 20210701
    """

    dim_ld_schedule_define = pd.read_sql_query(sql=dim_ld_schedule_define_sql, con=conn)
    return fact_sktd, dim_ld_schedule_define


def convert_to_int(df, columns):
    df1 = df
    for i in columns:
        df1 = df1.astype({i: 'int64'})
    return df1

def populate_split_col(
        df: DataFrame,
        column: Union[str, tuple],
        key: str) -> DataFrame:
    """
    :param column: must be a string or a tuple
    :param df:
    :param key_to_split:
    :return:
    """
    _df = df.copy()
    if isinstance(column, str):
        columns = [column]
    elif isinstance(column, tuple):
        columns = column
    else:
        raise ValueError("column must be a scalar, tuple, or list thereof")
    for i in columns:
        _df[i] = _df[i].apply(lambda x: x.split(key))
    return _df


ob = MsSql(host=LocalMsSql.URL, user=LocalMsSql.UID, password=LocalMsSql.PWD)
fact_sktd, dim_ld_schedule_define = read_data(ob)
#
# df1 = dim_ld_schedule_define.rename(columns={'ID': 'id', 'AMOUNT': 'amount', 'DATE': 'date'})[['id', 'date', 'amount']].head(5)
# df2 = df1.transform(populate_split_col, column=('date', 'amount'), key='*')
# df2.explode(['date', 'amount


df1 = fact_sktd.groupby('branch_code').agg({'amount' : 'sum'}).reset_index()

num_bins = 50

fig, ax = plt.subplots()

n, bins, patches = ax.hist(df1['amount'], num_bins, density=True, color='Green', cumulative=False)
