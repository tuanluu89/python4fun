class LocalMsSql:
    URL = 'DESKTOP-N6GK91K'
    UID = 'sa'
    PWD = 'Gamap123'
    connectionProperties = {
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'url': 'jdbc:sqlserver://' + URL,
        'user': UID,
        'password': PWD
    }

class sparkEnv:
    # URL = 'DESKTOP-PLO876V:1433'
    URL = 'DESKTOP-N6GK91K'
    UID = 'sa'
    PWD = 'Gamap123'
    connectionProperties = {
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'url': 'jdbc:sqlserver://' + URL,
        'user': UID,
        'password': PWD
    }

class TestPath:
    COCODE_KHUVUC = 'asset.dbo.COCODE_KHUVUC'
    DIM_COMPANY = 'asset.dbo.DIM_COMPANY'
    FACT_SKTD = 'asset.dbo.fact_sktd'
    ABB_DITIZEN = 'raw.dbo.ABB_DITIZEN'
    CARD_ISSUED= 'fact.dbo.card_issued'
    BANCAS = 'fact.dbo.fwd_snapshot'


class AbBankSql:
    URL = '10.32.8.10,8899'
    UID = 'app_btc_view'
    PWD = 'App#2021btcvw'
    connectionProperties = {
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'url': 'jdbc:sqlserver://' + URL,
        'user': UID,
        'password': PWD
    }