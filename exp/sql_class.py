import pandas as pd
import pyodbc as pyodbc
# from conf.env import LocalMsSql
from exp.logging_class import AppLogger


class MsSql:
    """
    Sql class through with we can perform most of sql task using python
    :parameter:
        host: host url often like servername "," port
        user: user_name
        password:
        db: database name - default empty string ("")
    """

    def __init__(self, host, user, password, db="", driver="{SQL Server}"):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.driver = driver
        self.logger = AppLogger("logfile.txt")
        self.logger.log("info", "SQL object created")

    def conn(self):
        """
        :return: connection to MsSql server with provided connection info in __init__
        """
        try:
            return pyodbc.connect(
                driver=self.driver,
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password
            )
        except Exception as e:
            self.logger.log("error", f"connection error : {str(e)}")
            print(str(e))

    def db_list(self):
        """
        :return: show all database list
        """
        try:
            conn = self.conn()
            cursor = conn.cursor()
            q = """
                select name, database_id, create_date 
                from sys.databases
                """
            cursor.execute(q)
            print(cursor.fetchall())
            conn.close()
            self.logger.log("info", "DB list displayed")
        except Exception as e:
            conn.close()
            print(str(e))
            self.logger.log("error", f"db list error : {str(e)}")

    def select_db(self, db_name):
        """
        select a database
        :param db_name: database name
        :return:
        """
        self.db = db_name
        self.logger.log("info", f"{db_name} DB selected")

    def create_table(self, table_name, columns):
        """
        Function create_table is used to create a new table with schema design in columns
        :param table_name: table_name
        :param columns: columns names with data type
        :return: 
        """
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"CREATE TABLE {table_name} ({columns})")
            conn.close()
            self.logger.log("info", f"{table_name} table created with columns: {columns}")
        except Exception as e:
            conn.close()
            print(str(e))
            self.logger.log("error", f"{table_name} was not created due to {str(e)}")

    def columns(self, table_name):
        """
        :param table_name: table_name
        :return: the schema of the table
        """
        try:
            conn = self.conn()
            cursor = conn.cursor()
            cursor.execute(f"select * from information_schema.columns where table_name = {table_name}")
            conn.close()
            self.logger.log("info", f"columns names displayed")
        except Exception as e:
            conn.close()
            self.logger.log("error", f"columns name not displayed : {str(e)}")
    
#
# def main():
#     ob = MsSql(host=LocalMsSql.URL, user=LocalMsSql.UID, password=LocalMsSql.PWD)
#     ob.db_list()
#
#
# if __name__ == '__main__':
#     main()