import json

file = open('pattern.json', 'r')
data = json.loads(file.read())

#print(data)
address = data['address']

print(address)
for pattern in address:
 print(pattern['pattern'],r['where'])