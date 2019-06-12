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
            self.obj.connectSchema(schemaName)

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
        except Exception as e:
            print(e)

        finally:
            self.obj.mydb.close()


extrac = Extraction()
# schemaName = 'ga_statewide_renewal' #'testj'
# tableName = 'pe_owner'
# idColumnName = 'id'
# fieldName = 'test_ADDRESS1'

#schemaName = 'fl_flagler_rawdata' #'testj'
schemaName = 'testj'
tableName = 'testExtraction'
idColumnName = 'id'
fieldName = 'ADDRESS_2'

print(schemaName, tableName)
start = time.time()
extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
fieldName = 'ADDRESS_1'
extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
fieldName = 'ADDRESS_3'
extrac.orderedExtractZillow(schemaName, tableName, idColumnName, fieldName)
end = time.time()
print("Complete execution time: ", end-start)