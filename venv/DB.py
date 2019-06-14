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
            return 1
            #print(self.mydb)
        except Exception as e:
            print("Connection problem in schema",e)
            return e

    def checkTable(self, tableName):
        try:
            sql = 'select * from ' + tableName
            mycursor = self.mydb.cursor(buffered=True)
            mycursor.execute(sql)
            return 1
        except Exception as e:
            return e

    def checkC(self,tableName, id):
        try:
            sql = 'select ' + id + ' from ' + tableName
            print(sql)
            mycursor = self.mydb.cursor(buffered=True)
            mycursor.execute(sql)
            return 1
        except Exception as e:
            print(e)
            status = id + ' is not a column name'
            return status

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
        except Exception as e:
            print("count", e)

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
                        #print(mycursor.rowcount,index)
                        if (index%indexLimit == 0) or (mycursor.rowcount-1 == index):
                            dataIndex = dataIndex+1
                            data[dataIndex] = idAndPatternString
                            idAndPatternString = []
            #print(index)
            return data
        except Exception as e:
            print("regexData",e)

    def insertData(self,tableName, idColumnName, fieldName, dataList):
        try:
            mycursor = self.mydb.cursor()
            mycursor.executemany("update " + tableName + " set " + fieldName  + " = %s where " + idColumnName + " = %s", dataList)
        except Exception as e:
            print(e)
        finally:
            self.mydb.commit()

    def insertField(self, tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo):
        try:
            mycursor = self.mydb.cursor()
            data = self.regexData(tableName,idColumnName, searchColumnName, sqlWhere, pyRe, gNo)
            for index,values in data.items():
                mycursor.executemany("update " + tableName + " set " + insertColumnName + " = %s where " + idColumnName + " = %s", values)
        except Exception as e:
            print("insertField",e)

        finally:
            print("DB commit")
            self.mydb.commit()

    def zillowUpdate(self, schemaName, tableName, idColumnName, searchColumnName,insertColumnName, sqlWhere, pyRe, gNo):
        try:
            self.defaultZillow()
            self.connectSchema(schemaName)
            #self.createColumn(tableName, insertColumnName)
            #print(tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo)
            self.insertField(tableName, idColumnName, searchColumnName, insertColumnName, sqlWhere, pyRe, gNo)

        except Exception as e:
            print("zillowUpdate",e)

    def getId(self, tableName, idFieldName, firstField, secondField, sqlCondition):
        try:
            sql = "select " + idFieldName + " from " + tableName + " where " + firstField + " " + sqlCondition + " and " + secondField + "_address_extract is null and " + secondField +"_name_extract is not null and " + firstField + "_name_extract is not null"
            cursor = self.mydb.cursor()
            cursor.execute(sql)

            idList = []
            for id in cursor.fetchall():
                idList.append(str(id[0]))
            return idList

        except Exception as e:
            print(e)

    def getConcatId(self, tableName, idFieldName, firstField, secondField, idList):
        try:
            idAndData = []
            for insertId in idList:
                sql = "select " + idFieldName + ", " + firstField + ", " + secondField + "_name_extract " + " from " + tableName + " where " + idFieldName + " = '" + insertId + "'"
                cursor = self.mydb.cursor()

                cursor.execute(sql)
                for id, firstData, secondData in cursor.fetchall():
                    coPattern = re.search(r'([cC][\/][oO0] .*)',firstData)
                    if coPattern:
                        co = coPattern.group(1)
                        print(co)
                        idAndData.append((co.lstrip() + ' ' + secondData.rstrip(), id))
            return idAndData
        except Exception as e:
            print(e)

    def replaceRow(self, tableName, idField, firstFieldName, secondField, idDataList):
        try:
            for values in idDataList:
                cursor = self.mydb.cursor()
                cursor.executemany(
                    "update " + tableName + " set " + firstFieldName + " = %s where " + idField + " = %s", values)
                cursor.executemany("update " + tableName + " set " + secondField + " = null where " + idField + " = %s",
                                   values[0])
        except Exception as e:
            print(e)





#
# start = time.time()
# obj.zillowUpdate('testj', 'main_source_file', 'id', 'CurDeliveryAddr','new12', 'where CurDeliveryAddr is null', '(.*)', 1)
# end = time.time()
#
# print(end-start)