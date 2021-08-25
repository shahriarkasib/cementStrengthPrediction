from Prediction_Raw_Data_Validation.predictionDataValidation import Prediction_Data_validation
from DataTypeValidation_Insertion_Prediction.DataTypeValidationPrediction import dBOperation
from DataTransformation_Prediction.DataTransformationPrediction import dataTransformPredict
from application_logging import logger
from datetime import datetime as dt
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db


class pred_validation:
    def __init__(self, client, resource, bucket):
        self.raw_data = Prediction_Data_validation(client, resource, bucket)
        self.dataTransform = dataTransformPredict(client, resource, bucket)
        self.dBOperation = dBOperation(client, resource, bucket)
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.log_writer = logger.App_Logger()
        self.db_obj = log_insertion_to_db("PredictionMainLog")

    def prediction_validation(self):

        try:

            self.log_writer.log(self.file_object,'Start of Validation on files for prediction!!')
            #extracting values from prediction schema
            print("starting validation of files")
            print("getting raw data values from schema")
            LengthOfDateStampInFile,LengthOfTimeStampInFile,column_names,noofcolumns = self.raw_data.valuesFromSchema()
            print("got the schema")
            #getting the regex defined to validate filename
            print("creating manual regex for filename validation")
            regex = self.raw_data.manualRegexCreation()
            print("amnual regex created")
            #validating filename of prediction files
            print("validating file name")
            self.raw_data.validationFileNameRaw(regex,LengthOfDateStampInFile,LengthOfTimeStampInFile)
            print("filename validated")
            #validating column length in the file
            print("validating column lenght")
            self.raw_data.validateColumnLength(noofcolumns)
            print("column length validated")
            #validating if any column has all values missing
            print("column  length validated")
            print("validating missing values")
            self.raw_data.validateMissingValuesInWholeColumn()
            print("missing values validated")
            self.log_writer.log(self.file_object,"Raw Data Validation Complete!!")
            print("raw data validation complete")

            self.log_writer.log(self.file_object,"Creating Prediction_Database and tables on the basis of given schema!!!")
            #create database with given name, if present open the connection! Create table with columns given in schema
            data_db = {'objective': 'rawdata', 'message': "Creating Prediction database Table",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("start creating prediction DataBase")

            table = self.dBOperation.createTableDb('PredictionData')

            print("prediction table created")
            data_db = {'objective': 'rawdata', 'message': "Table Creation Completed for prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            data_db = {'objective': 'rawdata', 'message': "Insertion of data into Table Started for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # insert csv files in the table
            print("Inserting good data in table")
            self.dBOperation.insertIntoTableGoodData(table)
            print("Good data is inserted into table")
            data_db = {'objective': 'rawdata', 'message': "Insertion of data into Table Completed for Prediction",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("done")

        except Exception as e:
            raise e









