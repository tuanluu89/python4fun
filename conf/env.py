class LocalMsSql:
    URL = 'DESKTOP-PLO876V,1433'
    UID = 'test'
    PWD = '123'
    connectionProperties = {
        'driver': 'com.microsoft.sqlserver.jdbc.SQLServerDriver',
        'url': 'jdbc:sqlserver://' + URL,
        'user': UID,
        'password': PWD
    }