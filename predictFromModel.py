import pandas
import json
from file_operations import file_methods
from data_preprocessing import preprocessing
from data_ingestion import data_loader_prediction
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt
from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation



class prediction:

    def __init__(self,path, client, resource, bucket):

        self.pred_data_val = Prediction_Data_validation(client, resource, bucket)
        self.client = client
        self.bucket = bucket
        self.resource = resource
        self.db_obj = log_insertion_to_db('PredictionLog')
        self.db_obj_table = log_insertion_to_db('PredictionOutput')
        if path is not None:
            self.pred_data_val = Prediction_Data_validation(self.client, self.resource, self.bucket)
        print("vejal created")

    def predictionFromModel(self):

        try:
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '', 'message': "Start of Training",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Getting Prediction Data from DataBase",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}

            self.db_obj.insert_data(data_db)
            print("getting the data")
            data_getter=data_loader_prediction.Data_Getter_Pred(self.client, self.resource)
            data=data_getter.get_data()
            print("got the data")

            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                      'message': "Got Prediction Data from DataBase",
                      'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            print("prediction preprocessing has started")
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Start Prepearing Data for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            preprocessor=preprocessing.Preprocessor(self.db_obj)

            is_null_present=preprocessor.is_null_present(data)
            print("got null data")
            if(is_null_present):
                data=preprocessor.impute_missing_values(data)

            data  = preprocessor.logTransformation(data)

            #scale the prediction data
            data_scaled = pandas.DataFrame(preprocessor.standardScalingData(data),columns=data.columns)
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '',
                       'message': "Data Prepeared For Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("data preprocessing is done")
            #data=data.to_numpy()
            print("loading kmeans file")
            file_loader=file_methods.File_Operation(self.db_obj, self.client, self.resource)
            print("created")
            kmeans=file_loader.load_model('modelfiles/KMeans')
            print("kmeans file loaded")
            ##Code changed
            #pred_data = data.drop(['Wafer'],axis=1)
            clusters=kmeans.predict(data_scaled)#drops the first column for cluster prediction
            data_scaled['clusters']=clusters
            clusters=data_scaled['clusters'].unique()
            result=[] # initialize blank list for storing predicitons
            # with open('EncoderPickle/enc.pickle', 'rb') as file: #let's load the encoder pickle file to decode the values
            #     encoder = pickle.load(file)
            print("number of clusters")
            print(clusters)
            for i in clusters:
                print(i)
                cluster_data= data_scaled[data_scaled['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                print("getting correct model")
                model_name = file_loader.find_correct_model_file(i,self.client, self.resource)
                model = file_loader.load_model(model_name)
                for val in (model.predict(cluster_data.values)):
                    result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            print("predicted")
            out = json.loads(result.to_json(orient='records'))
            self.db_obj_table.table.insert_many(out)
            data_db = {'objective': 'prediction', 'status': 'ok', 'error': '', 'message': "End of Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

        except Exception as ex:
            data_db = {'objective': 'prediction', 'status': 'error', 'error': 'ExceptionError', 'message': str(ex),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise ex
        return result.head().to_json(orient="records")

