# from common.pandas.dataframe_ext import DataFrameExt
from pandas import DataFrame


class LendingPreprocessing:

    @staticmethod
    def rename_cols(df: DataFrame) -> DataFrame:
        """
        :param df:
        :return: rename some columns
        """
        mapping = {
            'ma_cn_pgd': 'branch_code',
            'du_no_cuoi_ky': 'eop',
            'ma_kh': 'cus_id',
            't24_loan_code': 'product_code'
        }
        return df.rename(columns=mapping)

    @staticmethod
    def cast_eop(df: DataFrame) -> DataFrame:
        """
        column eop needs to be converted from varchar to float
        :param df:
        :return:
        """
        df1 = df
        df1['eop'] = df1['eop'].astype('float')
        return df1

    @staticmethod
    def main_processor(df: DataFrame) -> DataFrame:
        return (
            df.transform(LendingPreprocessing.rename_cols)
            .transform(LendingPreprocessing.cast_eop)
        )
