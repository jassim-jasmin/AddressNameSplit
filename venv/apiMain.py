from flask import Flask, request, jsonify
from main import Extraction
from flask_cors import CORS, cross_origin

split = Extraction()

app = Flask(__name__)

app.config['CORS_HEADERS'] = 'Content-Type'

cors = CORS(app, resources={r"/": {"origins": "*"}})

@app.route('/zillow', methods=['POST'])
@cross_origin(origin='*',headers=['Content- Type','Authorization'])
def extract():
    try:
        content = request.get_json()

        print(content['schemaName'])
        print(content['tableName'])
        print(content['idColumn'])
        print(content['fieldName'])

        status = split.orderedExtractZillow(content['schemaName'], content['tableName'], content['idColumn'], content['fieldName'])
        if status == 1:
            return jsonify({"success":"Split completed"})
        else:
            print("string: ",str(status))
            return jsonify({"error": str(status)})
    except Exception as e:
        print(e)
        return jsonify({"error":'erroroccured'})

if __name__ =='__main__':
    app.run(debug=True, host='192.168.15.63', port='3226')
