import pandas as pd
from datetime import timedelta

from exp.sql_class import MsSql
from common.conf.env import LocalMsSql
from common.helper.config import config
from exp.lending_daily.preprocessor.lending_preprocessor import LendingPreprocessing

run_date = config.run_date-timedelta(days=1)
run_date = '2021-07-31'


def main():
    raw_lending_df = read_data()
    print(len(raw_lending_df.index))
    lending_df = raw_lending_df.transform(LendingPreprocessing.main_processor)
    print(len(lending_df.index))
    print(lending_df.columns)
    print(lending_df.dtypes)
    # print(len(lending_df.index))


def read_data():
    # create object MsSql linked to LocalMsSql database
    ob = MsSql(host=LocalMsSql.URL, user=LocalMsSql.UID, password=LocalMsSql.PWD)
    conn = ob.conn()
    raw_lending_sql = f"""
        select ma_cn_pgd, ma_kh, du_no_cuoi_ky, t24_loan_code
        from asset.dbo.fact_sktd
        where mis_date_d = '{run_date}'
            and sector_id between 1000 and 1999
    """
    raw_lending_df = pd.read_sql_query(sql=raw_lending_sql, con=conn)
    return raw_lending_df


if __name__ == '__main__':
    main()

jdbcDF = spark.read.format("jdbc") \
    .option("url", f"jdbc:sqlserver://localhost:1433;databaseName={database}") \
    .option("dbtable", "Employees") \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()