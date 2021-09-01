from Training_Raw_data_validation.rawValidation import Raw_Data_validation
from DataTypeValidation_Insertion_Training.DataTypeValidation import dBOperation
from DataTransform_Training.DataTransformation import dataTransform
from datetime import datetime as dt
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db

class train_validation:
    def __init__(self, client, resource, bucket):
        self.raw_data = Raw_Data_validation(client, resource, bucket)
        self.dataTransform = dataTransform(client, resource, bucket)
        self.dBOperation = dBOperation(client, resource, bucket)
        self.db_obj = log_insertion_to_db('TrainMainLog')

    def train_validation(self):
        try:

            print("start train validation")
            # extracting values from prediction schema
            data_db = {'objective': 'rawdata', 'message': "Start of Validation on files",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of Getting values From Schema",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # extracting values from prediction schema
            LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, noofcolumns = self.raw_data.valuesFromSchema()

            data_db = {'objective': 'rawdata', 'message': "Got values From Schema",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("get values from schema")
            data_db = {'objective': 'rawdata', 'message': "Start of definining regex to validate filename",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()

            data_db = {'objective': 'rawdata', 'message': "Regex Defined to validate filename",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("create manual regex")
            # validating filename of prediction files
            data_db = {'objective': 'rawdata', 'message': "Start of validating Raw Data",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data_db = {'objective': 'rawdata', 'message': "Start of validating filename",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfDateStampInFile, LengthOfTimeStampInFile)

            print("print validation file name")
            data_db = {'objective': 'rawdata', 'message': "Filename Validated",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # validating column length in the file
            data_db = {'objective': 'rawdata', 'message': "Start of validating ColumnLength",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # validating column length in the file
            self.raw_data.validateColumnLength(noofcolumns)

            data_db = {'objective': 'rawdata', 'message': "ColumnLength Validated",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("validating column length ok")
            print("ok")
            # validating if any column has all values missing
            data_db = {'objective': 'rawdata', 'message': "Validating Missing Values In whole Column",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesInWholeColumn()
            data_db = {'objective': 'rawdata', 'message': "Missing Values Validated",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("validating missing values in column")

            data_db = {'objective': 'rawdata', 'message': "Raw Data Validation Completed",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


            # create database with given name, if present open the connection! Create table with columns given in schema

            data_db = {'objective': 'rawdata', 'message': "Creating Training database Table",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            table = self.dBOperation.createTableDb("InputData")
            print("table created")

            # insert csv files in the table
            data_db = {'objective': 'rawdata', 'message': "Insertion of Data into Table started",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


            self.dBOperation.insertIntoTableGoodData(table)

            data_db = {'objective': 'rawdata', 'message': "Insertion of Training Data in Table completed",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


            # Delete the good data folder after loading files in table
            data_db = {'objective': 'rawdata', 'message': "Deleting good data files from aws",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            self.raw_data.deleteExistingGoodDataTrainingFolder()

            data_db = {'objective': 'rawdata', 'message': "Good data files deleted from aws",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            # Move the bad files to archive folder
            print("existing good data folder deleted")
            # self.raw_data.moveBadFilesToArchiveBad()
            # self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            data_db = {'objective': 'rawdata', 'message': "Validation Operation completed",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


        except Exception as e:
            raise e









