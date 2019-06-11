from flask import Flask, request
from main import Extraction
from flask_cors import CORS, cross_origin

app = Flask(__name__)

app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/": {"origins": "*"}})

@app.route('/zillow', methods=['POST'])
@cross_origin(origin='*',headers=['Content- Type','Authorization'])
def extract():
    try:
        content = request.get_json()
        # print(content)
        # print(request.is_json)
        # print(request)
        #print(content['hostAddress'])
        #print(content['userName'])
        #print(content['passWord'])

        print(content['schemaName'])
        print(content['tableName'])
        print(content['idColumn'])
        print(content['fieldName'])
        #print(content['outFiledName'])

        data = {"local": "f:j"}
        orderedExtractZillow()
        Extraction.orderedExtractZillow(Extraction, content['schemaName'], content['tableName'], content['idColumn'], content['fieldName'])
        return "success"
    except Exception as e:
        print(e)
        return "failed"

if __name__ =='__main__':
    app.run(debug=True, host='192.168.15.63', port='3226')
