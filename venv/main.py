import DB
import json
import time

from DB import sqlDB

class Extraction:
    jsonPatternFile = open('pattern.json', 'r')
    jsonData = json.loads(jsonPatternFile.read())
    obj = sqlDB()

    def extract(self, schemaName, tableName, idColumn, fieldName, outFiledName, nameOrAddress):
        name = self.jsonData[nameOrAddress]
        jsonIndex = -1
        for pattern in name:
            jsonIndex = jsonIndex + 1
            print("json ", nameOrAddress, jsonIndex)
            # print(pattern)
            self.obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName,
                             'where ' + fieldName + " " + pattern['where'] + " and " + outFiledName + " is null",
                             pattern['pattern'], pattern['group'])

    def partialExtract(self, schemaName, tableName, idColumn, fieldName, outFiledName, nameOrAddress):
        try:
            self.obj.defaultZillow()
            self.obj.connectSchema(schemaName)
            name = self.jsonData[nameOrAddress + 'Partial']
            if nameOrAddress == 'name':
                otherName = fieldName+'_address_extract'
            elif nameOrAddress == 'address':
                otherName = fieldName+'_name_extract'

            jsonIndex = -1
            for pattern in name:
                jsonIndex = jsonIndex + 1
                print("json ", nameOrAddress, jsonIndex)
                self.obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName,
                                 'where ' + fieldName + " " + pattern[
                                     'where'] + " and " + outFiledName + " is null and " + otherName + " is null ",
                                 pattern['pattern'], pattern['group'])
        except Exception as e:
            prin("partialExtract",e)

    def orderedExtractZillow(self, schemaName, tableName, idColumn, fieldName):
        try:
            self.obj.defaultZillow()
            schemaStatus = self.obj.connectSchema(schemaName)

            if schemaStatus == 1:
                tableStatus = self.obj.checkTable(tableName)
                if tableStatus == 1:
                    idFildCheck = self.obj.checkColumn(tableName, idColumn)
                    if idFildCheck == 1:
                        FildCheck = self.obj.checkColumn(tableName, fieldName)
                        if FildCheck == 1:
                            self.obj.createColumn(tableName, fieldName+'_address_extract')
                            self.obj.createColumn(tableName, fieldName+'_name_extract')
                            self.obj.createColumn(tableName, fieldName + '_no_match')

                            self.extract(schemaName, tableName, idColumn, fieldName, fieldName+'_address_extract', 'address')
                            self.extract(schemaName, tableName, idColumn, fieldName, fieldName+'_name_extract', 'name')
                            self.partialExtract(schemaName, tableName, idColumn, fieldName, fieldName+'_name_extract', 'name')
                            self.partialExtract(schemaName, tableName, idColumn, fieldName, fieldName+'_address_extract', 'address')

                            sqlWhere = ' where ' + fieldName+'_name_extract is null and ' + fieldName+'_address_extract is null and ' + fieldName + ' is not null'

                            self.obj.connectSchema(schemaName)
                            self.obj.insertField(tableName, idColumn, fieldName, fieldName + '_no_match', sqlWhere, ' *(.*)', 1)
                            return 1
                        else:
                            return FildCheck
                    else:
                        return idFildCheck
                else:
                    return tableStatus
                self.obj.mydb.close()
            else:
                return schemaStatus
        except Exception as e:
            print('exception', e)
            return e