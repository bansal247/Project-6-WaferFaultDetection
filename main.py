from wsgiref import simple_server
from flask import Flask, request, render_template, Response
from flask_cors import cross_origin, CORS
import flask_monitoringdashboard as dashboard #for requests timing and users monitoring
import os
from training_Validation_Insertion import train_validation
from trainingModel import trainModel

#These env may be used when users open interface from other regions having different environments.
os.putenv('LANG','en_US.UTF-8')#Set the environment variable named key to the string value.
os.putenv('LC_ALL', 'en_US.UTF-8')#LC_ALL is the environment variable that overrides all the other localisation settings.

app = Flask(__name__)

dashboard.config.init_from(file='/config.cfg') #make a file showing all configurations
dashboard.bind(app)# Make sure to call config.init_from() before bind()
CORS(app)


@app.route("/",methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict",methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        pass
    except:
        pass

@app.route("/train",methods=['POST'])
@cross_origin()
def trainRouteClient():
    try:
        if request.json['folderPath'] is not None:
            path = request.json['folderPath']

            train_valObj = train_validation(path) #train_valObj initialization
            train_valObj.train_validation()  # calling the training_validation function which validates the data

            trainModelObj = trainModel()  # object initialization
            trainModelObj.trainingModel()  # training the model for the files in the table

    except ValueError:
        return Response("Error Occurred! %s" % ValueError)

    except KeyError:
        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:
        return Response("Error Occurred! %s" % e)
    return Response("Training successfull!!")



port = int(os.getenv("PORT",5000))
if __name__ == '__main__':
    host = '0.0.0.0'
    #post is 5000
    httpd = simple_server.make_server(host,port,app)
    httpd.serve_forever()


