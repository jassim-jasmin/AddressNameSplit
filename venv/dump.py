import json
import time
import re

jsonPatternFile = open('pattern.json', 'r')
jsonData = json.loads(jsonPatternFile.read())

data = 'WILLETTE RD'
name = jsonData['name']
namePartial = jsonData['namePartial']
address = jsonData['address']
addressPartial = jsonData['addressPartial']

jsonIndex = -1
for pattern in name:
    jsonIndex = jsonIndex + 1
    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],re.search(pattern['pattern'],data).group(pattern['group']), 'name')
        break

for pattern in address:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],re.search(pattern['pattern'],data).group(pattern['group']), 'address')
        break

for pattern in namePartial:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],re.search(pattern['pattern'],data).group(pattern['group']), 'namePartial')
        break

for pattern in addressPartial:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],re.search(pattern['pattern'],data).group(pattern['group']), 'addressPartial')
        break