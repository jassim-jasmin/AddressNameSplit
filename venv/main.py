import DB
import json
import time

from DB import sqlDB

jsonPatternFile = open('pattern.json', 'r')
jsonData = json.loads(jsonPatternFile.read())
addressExtract = jsonData['address']
nameExtract = jsonData['name']
obj = sqlDB()

for addressPattern in addressExtract:
    #print(addressPattern['pattern'])
    obj.zillowUpdate('testj', 'pe_owner', 'id', 'ADDRESS1', 'address_extract', 'where ' + addressPattern['where'], addressPattern['pattern'], addressPattern['group'])