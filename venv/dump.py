import json
import time
import re

jsonPatternFile = open('pattern.json', 'r')
jsonData = json.loads(jsonPatternFile.read())

data = 'W 230 CARMEN ROAD N '
name = jsonData['name']
namePartial = jsonData['namePartial']
address = jsonData['address']
addressPartial = jsonData['addressPartial']

reg = '(( *^[sS][,.]?[uU][,.]?[iI][,.]?[tT][,.]?[eE]*[,.]?( | *$))|( *^[Uu][,.]?[nN][,.]?[iI][,.]?[tT][,.]?[eE]*[,.]?( | *$))|( *^[pP][,.]?[lL][,.]?[aA]?[,.]?[cC]?[,.]?[eE]?[,.]?( | *$))|( *^[Tt][,.]?[rR][,.]?[lL][,.]?[,.]?( | *$))|( *^[bB][,.]?[lL][,.]?[vV][,.]?[dD][,.]?( | *$))|( *^[pP][,.]?[lL][,.]?[aA][,.]?[zZ][,.]?[aA][,.]?( | *$))|( *^[rR][,.]?[oO0]?[,.]?[Uu]?[,.]?[tT][,.]?[eE]?[,.]?( | *$))|( *^[cC][,.]?[tT][,.]?( | *$))|( *^[pP][,.]?[iI][,.]?[kK][,.]?[eE][,.]?( | *$))|( *^[pP][,.]?[aA]?[,.]?[rR]?[,.]?[kK][,.]?[wW]?[,.]?[aA]?[,.]?[yY]?[,.]?( | *$))|( *^[hH][,.]?[iI]?[,.]?[gG]?[,.]?[hH]?[,.]?[wW][,.]?[aA]?[,.]?[yY][,.]?( | *$))|( *^[Ll][,.]?[aA]?[,.]?[nN][,.]?[eE]?[,.]?( | *$))|( *^[cC][,.]?[iI][,.]?[rR][,.]?[cC]?[,.]?[lL]?[,.]?[eE]?[,.]?( | *$))|( *^[Aa][,.]?[vV][,.]?[eE][,.]?[nN]?[,.]?[uU]?[,.]?[eE]*[,.]?( | *$))|( *^[dD][,.]?[rR][,.]?[iI]?[,.]?[vV]?[,.][eE]*[,.]?( | *$))|( *^[sS][,.]?[tT][.,]?[rR]?[,.]?[eE]?[,.][eE]?[,.]?[,.][tT][,.]( | *$))|( *^[pP] *[.,]? *[oO0] *[,.]? *[bB][,.]? *[oO0][.,]? *[xX][,.]?)|( *^[0-9])|( *^[sS][tT][rR]?[eE]?[eE]?[tT]?)( | *$)|([dD][rR][iI]?[vV]?[eE]*)( | *$)|( *^[rR][oO0]?[aA]?[Dd])) +n'

if re.search(reg,data):
    print("Found regex101 is wrong")
jsonIndex = -1
for pattern in name:
    jsonIndex = jsonIndex + 1
    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],jsonIndex,re.search(pattern['pattern'],data).group(pattern['group']), 'name')
        break

jsonIndex = -1
for pattern in address:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],jsonIndex,re.search(pattern['pattern'],data).group(pattern['group']), 'address')
        break

jsonIndex = -1
for pattern in namePartial:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],jsonIndex,re.search(pattern['pattern'],data).group(pattern['group']), 'namePartial')
        break

jsonIndex = -1
for pattern in addressPartial:
    jsonIndex = jsonIndex + 1

    if re.search(pattern['pattern'],data):
        print(pattern['pattern'],jsonIndex,re.search(pattern['pattern'],data).group(pattern['group']), 'addressPartial')
        break