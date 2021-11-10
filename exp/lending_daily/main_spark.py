from pyspark.sql import SparkSession, functions as F
from common.spark.extension.dataframe_ext import DataFrameExt
from common.conf.env import sparkEnv, TestPath
from common.helper.config import config, RunDate
from datetime import datetime, timedelta

path = TestPath
env = sparkEnv
run_date = config.run_date-timedelta(days=63)
# run_date = '2021-07-31'
FACT_SKTD_CONDITION = """   (
                                (mis_date_d < '2021-01-01' 
                                    and (sector_id between 1000 and 1013 or 
                                        (sector_id = 1014 and T24_Loan_Code = 'Credit Card - Khong TSBD'))
                                ) or
                                (mis_date_d >= '2021-01-01' and sector_id between 1000 and 1999)
                            )
"""


def main(spark: SparkSession):
    fact_sktd = read_data(spark=spark, env=env, run_date=run_date)
    pass


def read_data(spark: SparkSession, env, run_date: RunDate):
    fact_sktd_query = f"""select * from {TestPath.FACT_SKTD}
                            where {FACT_SKTD_CONDITION} AND (mis_date_d >= '{run_date.begin_of_year()}'
                            or mis_date_d = '{run_date.end_of_last_year()}')
        """
    fact_sktd = query_from_sql(spark=spark, env=env, query=fact_sktd_query)
    return fact_sktd


def query_from_sql(spark, env, query):
    return (
        spark.read.format('jdbc')
        .options(**env.connectionProperties)
        .option('query', query)
        .load()
    )


def read_table_from_sql(spark, env, table_name):
    return (
        spark.read.format('jdbc')
        .options(**env.connectionProperties)
        .option('dbtable', table_name)
        .load()
    )

if __name__ == '__main__':
    _spark = SparkSession.builder.getOrCreate()
    main(_spark)