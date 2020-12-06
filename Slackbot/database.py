import os

import pandas
import pymysql
from common.config import Config


class Database:

    def __init__(self):
        self.__config = Config()

        h, u = self.__config.get("DATABASE.HOST"), self.__config.get("DATABASE.USER")
        p, db = self.__config.get("DATABASE.PASSWORD"), self.__config.get("DATABASE.DB_NAME")
        self.connection = pymysql.connect(host=h, user=u, password=p, db=db)

    # execute insert query on table by using params(dictionary)
    def insert(self, data, tableName):
        if len(data) == 0: return True

        cursor = self.connection.cursor()
        try:
            cols = ",".join(["`{0}`".format(l) for l in list(data[0].keys())])
            for d in data:
                query = "insert into {0}({1}) values{2}".format(tableName, cols, tuple(d.values()))
                query = query.replace("None", "NULL")
                cursor.execute(query)
            cursor.close()
            self.connection.commit()
            return True
        except Exception as e:
            print("Exeception occured: {}".format(e))
            cursor.close()
            return False

    # execute stored procedure.
    def execute_sp(self, sp, params):
        cursor = self.connection.cursor()
        try:
            cursor.execute("CALL {0}()".format(sp))
            res = cursor.fetchone()
            cursor.close()
            self.connection.commit()
            return res[0] if len(res) > 0 else 0
        except Exception as e:
            print(e)
            cursor.close()
            return -1

    # save pandas dataset to table.
    def save(self, dataset, tableName):
        if len(dataset) == 0: return True

        dbConnection = self.connection.connect()
        try:
            frame = dataset.to_sql(tableName, dbConnection, if_exists='append', index=False)
            dbConnection.close()
            return True
        except ValueError as vx:
            print(vx)
            dbConnection.close()
            return False
        except Exception as ex:
            print(ex)
            dbConnection.close()
            return False

    # get table as pandas
    def get(self, query):
        dbConnection = self.connection.connect()
        try:
            frame = pandas.read_sql(query, dbConnection)
            dbConnection.close()
            return frame
        except Exception as ex:
            print(ex)
            dbConnection.close()
            return None

    # get one record from database in dictionary format.
    def getone(self, query):
        records = self.getall(query)
        return records[0] if len(records) > 0 else None

    # get list of records (dictionary format)
    def getall(self, query):
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        try:
            cursor.execute(query)
            res = cursor.fetchall()
            cursor.close()
            self.connection.commit()
            return res
        except Exception as ex:
            cursor.close()
            return []
