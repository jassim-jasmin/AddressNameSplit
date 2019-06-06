import DB
import json
import time

from DB import sqlDB

def extract(schemaName, tableName, idColumn, fieldName, outFiledName,nameOrAddress):
    jsonPatternFile = open('pattern.json', 'r')
    jsonData = json.loads(jsonPatternFile.read())
    obj = sqlDB()
    name = jsonData[nameOrAddress]
    jsonIndex = -1
    for pattern in name:
        jsonIndex = jsonIndex + 1
        print("json index: ", jsonIndex)
        print(pattern)
        obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName, 'where ' + fieldName + " "+ pattern['where'] + " and " + outFiledName + " is null",
                         pattern['pattern'], pattern['group'])

start = time.time()
extract('testj', 'pe_owner', 'id', 'ADDRESS1', 'address_extract', 'address')
extract('testj', 'pe_owner', 'id', 'ADDRESS1', 'name_extract', 'name')
end = time.time()
print("Complete execution time: ", end-start)
