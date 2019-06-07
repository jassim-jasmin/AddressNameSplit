from flask import Flask, request
from main import Extraction

app = Flask(__name__)

@app.route('/zillow', methods=['POST'])
def extract():
    try:
        content = request.json
        #print(content['hostAddress'])
        #print(content['userName'])
        #print(content['passWord'])
        print(content['schemaName'])
        print(content['tableName'])
        print(content['idColumn'])
        print(content['fieldName'])
        print(content['outFiledName'])
        data = {"local": "f:j"}
        Extraction.orderedExtractZillow(Extraction, content['schemaName'], content['tableName'], content['idColumn'], content['fieldName'])
        return "success"
    except Exception as e:
        print(e)
        return "failed"

if __name__ =='__main__':
    app.run(debug=True, host='192.168.15.63', port='3226')
