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
            #print(self.mydb)
        except Exception as e:
            print("Database connection error", e)

    def connectSchema(self,sqlSchema):
        try:
            self.mydb = mysql.connector.connect(host=self.sqlHost, user=self.sqlUserName, password=self.sqlPassword, database=sqlSchema)
            #print(self.mydb)
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
            mycursor.execute(sql)

            idAndPatternString = []
            data = dict()
            indexLimit = 50000
            index = -1
            dataIndex = -1
            for id, searchData in mycursor:
                index = index+1
                rePattern = eval("re.compile(r'" + pyRe + "')")

                if searchData:
                    if rePattern.search(searchData):
                        idAndPatternString.append((rePattern.search(searchData).group(int(gNo)),id))
                        if index%indexLimit == 0 or mycursor.rowcount == index:
                            dataIndex = dataIndex+1
                            data[dataIndex] = idAndPatternString
                            idAndPatternString = []
                            print(dataIndex,index)
            #print(data[0][1])
            return data
        except Exception as e:
            print(e)

    def insertField(self, tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo):
        try:
            mycursor = self.mydb.cursor()
            data = self.regexData(tableName,idColumnName, searchColumnName, sqlWhere, pyRe, gNo)
            for index,values in data.items():
                print("inserting")
                mycursor.executemany("update " + tableName + " set " + insertColumnName + " = %s where " + idColumnName + " = %s", values)
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