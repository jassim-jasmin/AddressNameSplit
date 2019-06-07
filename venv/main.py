import DB
import json
import time

from DB import sqlDB

jsonPatternFile = open('pattern.json', 'r')
jsonData = json.loads(jsonPatternFile.read())
obj = sqlDB()

def extract(schemaName, tableName, idColumn, fieldName, outFiledName,nameOrAddress):
    name = jsonData[nameOrAddress]
    jsonIndex = -1
    for pattern in name:
        jsonIndex = jsonIndex + 1
        print("json index: ", jsonIndex)
        #print(pattern)
        obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName, 'where ' + fieldName + " "+ pattern['where'] + " and " + outFiledName + " is null",
                         pattern['pattern'], pattern['group'])

def partialExtract(schemaName, tableName, idColumn, fieldName, outFiledName,nameOrAddress):
    name = jsonData[nameOrAddress + 'Partial']

    if nameOrAddress == 'name':
        otherName = 'address_extract'
    elif nameOrAddress == 'address':
        otherName = 'name_extract'

    jsonIndex = -1
    for pattern in name:
        jsonIndex = jsonIndex + 1
        print("json index: ", jsonIndex)
        obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName,
                         'where ' + fieldName + " " + pattern['where'] + " and " + outFiledName + " is null and " + otherName + " is null ",
                         pattern['pattern'], pattern['group'])

start = time.time()
extract('testj', 'pe_owner', 'id', 'ADDRESS1', 'address_extract', 'address')
extract('testj', 'pe_owner', 'id', 'ADDRESS1', 'name_extract', 'name')
partialExtract('testj', 'pe_owner', 'id', 'ADDRESS1', 'name_extract', 'name')
partialExtract('testj', 'pe_owner', 'id', 'ADDRESS1', 'address_extract', 'address')
end = time.time()
print("Complete execution time: ", end-start)
