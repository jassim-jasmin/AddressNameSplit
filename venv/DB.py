import mysql.connector
import re
import time

class sqlDB:
    def defaultZillow(self):
        self.sqlHost = '192.168.15.10'
        self.sqlUserName = 'root'
        self.sqlPassword = 'softinc'

    def defaultLocal(self):
        self.sqlHost = '192.168.15.63'
        self.sqlUserName = 'root'
        self.sqlPassword = 'softinc'

    def connect(self):
        try:
            self.mydb = mysql.connector.connect(host=self.sqlHost, user=self.sqlUserName, password=self.sqlPassword)
            print(self.mydb)
        except Exception as e:
            print("Database connection error", e)

    def connectSchema(self,sqlSchema):
        try:
            self.mydb = mysql.connector.connect(host=self.sqlHost, user=self.sqlUserName, password=self.sqlPassword, database=sqlSchema)
            print(self.mydb)
        except Exception as e:
            print("Connection problem in schema",e)

    def createColumn(self, tableName, columnName):
        try:
            mycursor = self.mydb.cursor()
            sql = "ALTER TABLE " + tableName + " ADD COLUMN " + columnName + " LONGTEXT;"
            #print(sql)
            mycursor.execute(sql)
            print("Column " + columnName + " added")
        except Exception as e:
            print("Column not create", e)

    def count(self, tableName):
        try:
            mycursor = self.mydb.cursor()
            sql = "select count(*) from " + tableName
            mycursor.execute(sql)

            for c in mycursor:
                return c[0]
        except:
            print("Error in count fetching")

    def regexData(self, tableName, idColumnName, searchColumnName, sqlWhere, pyRe, gNo):
        try:
            mycursor = self.mydb.cursor()
            sql = "select " + idColumnName + "," + searchColumnName + " from " + tableName + " " + sqlWhere
            print(sql)
            mycursor.execute(sql)

            idAndPatternString = []

            for id, searchData in mycursor:
                rePattern = eval("re.compile(r'" + pyRe + "')")

                if searchData:
                    if rePattern.search(searchData):
                        idAndPatternString.append((rePattern.search(searchData).group(int(gNo)),id))
            return idAndPatternString
        excegit pt Exception as e:
            print(e)

    def insertField(self, tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo):
        try:
            data = self.regexData(tableName,idColumnName, searchColumnName, sqlWhere, pyRe, gNo)
            mycursor = self.mydb.cursor()
            print(data)
            mycursor.executemany("update " + tableName + " set " + insertColumnName + " = %s where " + idColumnName + " = %s", data)
        except Exception as e:
            print(e)

    def zillowUpdate(self, schemaName, tableName, idColumnName, searchColumnName,insertColumnName, sqlWhere, pyRe, gNo):
        try:
            self.defaultZillow()
            self.connectSchema(schemaName)
            self.createColumn(tableName, insertColumnName)
            self.insertField(tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo)
        except Exception as e:
            print(e)

        finally:
            print("Terminated")
            self.mydb.commit()
            self.mydb.close()


# obj = sqlDB()
#
# start = time.time()
# obj.zillowUpdate('testj', 'main_source_file', 'id', 'CurDeliveryAddr','new12', 'where CurDeliveryAddr is null', '(.*)', 1)
# end = time.time()
#
# print(end-start)