from flask import Flask, request, render_template
from flask import Response
from flask_cors import cross_origin
from prediction_Validation_Insertion import pred_validation
from trainingModel import trainModel
from training_Validation_Insertion import train_validation
from predictFromModel import prediction
from Connect_with_awsS3.connect_aws import aws
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt


app = Flask(__name__)

@app.route("/", methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route("/predict", methods=['POST'])
@cross_origin()
def predictRouteClient():
    try:
        if request.json is not None:
            path = request.json['filepath']

            db_obj = log_insertion_to_db('PredictionGeneralLog')
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction Started",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            print("inserting")
            db_obj.insert_data(data_db)
            print("inserted")

            pred_val = pred_validation(aws_obj.client, aws_obj.resource, 'cementstrengthproject') #object initialization

            pred_val.prediction_validation() #calling the prediction_validation function

            pred = prediction(path, aws_obj.client, aws_obj.resource, 'cementstrengthproject') #object initialization
            # predicting for dataset present in database
            path = pred.predictionFromModel()
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction End",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            db_obj.insert_data(data_db)
            return Response("Prediction File created at %s!!!" % path)
        elif request.form is not None:
            path = request.form['filepath']
            db_obj = log_insertion_to_db('PredictionGeneralLog')
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction Started",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            db_obj.insert_data(data_db)
            pred_val = pred_validation(aws_obj.client, aws_obj.resource,                                       'cementstrengthproject')  # object initialization
            pred_val.prediction_validation()  # calling the prediction_validation function
            pred = prediction(path, aws_obj.client, aws_obj.resource, 'cementstrengthproject')  # object initialization
            # predicting for dataset present in database
            path = pred.predictionFromModel()
            data_db = {'objective': 'PredictionSystem', 'message': "Prediction End",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            db_obj.insert_data(data_db)
            return Response("Prediction table is created at the database")

    except ValueError as v:
        data_db = {'objective': 'PredictionSystem', 'message': str(v),

                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % ValueError)

    except KeyError as k:
        data_db = {'objective': 'PredictionSystem', 'message': str(k),

                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % KeyError)

    except Exception as e:
        data_db = {'objective': 'PredictionSystem', 'message': str(e),

                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % e)



@app.route("/train", methods=['GET'])
@cross_origin()
def trainRouteClient():

    try:
        db_obj = log_insertion_to_db('TrainingGeneralLog')
        data_db = {'objective': 'TrainSystem', 'message': "Training Started",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        print("inserting")
        db_obj.insert_data(data_db)
        aws_obj.delete_modelfiles('cementstrengthproject')
        train_valObj = train_validation(aws_obj.client, aws_obj.resource, 'cementstrengthproject') #object initialization
        train_valObj.train_validation()#calling the training_validation function
        trainModelObj = trainModel(aws_obj.client, aws_obj.resource, 'cementstrengthproject') #object initialization
        trainModelObj.trainingModel() #training the model for the files in the table
        data_db = {'objective': 'TrainSystem', 'message': "Training Done",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)


    except ValueError as v:
        data_db = {'objective': 'TrainSystem', 'message': "Error Occurred! " + str(v),
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % ValueError)

    except KeyError as k:
        data_db = {'objective': 'TrainSystem', 'message': "Error Occurred! " + str(k),

                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % KeyError)


    except Exception as e:
        data_db = {'objective': 'TrainSystem', 'message': "Error Occurred! " + str(e),

                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        db_obj.insert_data(data_db)
        return Response("Error Occurred! %s" % e)

    return Response("Training successfull!!")

if __name__ == "__main__":
    aws_obj = aws()
    app.run(host='127.0.0.1',port=8000)
