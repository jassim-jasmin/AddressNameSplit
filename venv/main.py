import DB
import json
import time

from DB import sqlDB

def extract(schemaName, tableName, idColumn, fieldName, outFiledName,nameOrAddress):
    jsonPatternFile = open('pattern.json', 'r')
    jsonData = json.loads(jsonPatternFile.read())
    obj = sqlDB()
    name = jsonData[nameOrAddress]
    for pattern in name:
        obj.zillowUpdate(schemaName, tableName, idColumn, fieldName, outFiledName, 'where ' + pattern['where'],
                         pattern['pattern'], pattern['group'])

extract('testj', 'pe_owner', 'id', 'ADDRESS1', 'address_extract', 'address')