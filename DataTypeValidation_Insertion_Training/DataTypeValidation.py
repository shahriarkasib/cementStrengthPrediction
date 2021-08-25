import shutil
import sqlite3
from datetime import datetime
from os import listdir
import os
import csv
import pandas as pd
from application_logging.logger import App_Logger
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt
import json
class dBOperation:
    """
      This class shall be used for handling all the SQL operations.

      Written By: iNeuron Intelligence
      Version: 1.0
      Revisions: None

      """
    def __init__(self, client, resource, bucket):
        self.client = client
        self.resource = resource
        self.bucket = bucket
        self.path = 'Training_Database/'
        self.badFilePath = "Training_Raw_files_validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_files_validated/Good_Raw"
        self.logger = App_Logger()
        self.db_obj = log_insertion_to_db('DBOperationLog')

    # def dataBaseConnection(self,DatabaseName):
    #
    #     """
    #             Method Name: dataBaseConnection
    #             Description: This method creates the database with the given name and if Database already exists then opens the connection to the DB.
    #             Output: Connection to the DB
    #             On Failure: Raise ConnectionError
    #
    #              Written By: iNeuron Intelligence
    #             Version: 1.0
    #             Revisions: None
    #
    #             """
    #     try:
    #         conn = sqlite3.connect(self.path+DatabaseName+'.db')
    #
    #         file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
    #         self.logger.log(file, "Opened %s database successfully" % DatabaseName)
    #         file.close()
    #     except ConnectionError:
    #         file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
    #         self.logger.log(file, "Error while connecting to database: %s" %ConnectionError)
    #         file.close()
    #         raise ConnectionError
    #     return conn

    def createTableDb(self,tablename):
        """
                        Method Name: createTableDb
                        Description: This method creates a table in the given database which will be used to insert the Good data after raw data validation.
                        Output: None
                        On Failure: Raise Exception

                         Written By: Shahriar Sourav
                        Version: 1.0
                        Revisions: None

                        """
        try:
            message = 'DataBase Input Table Created Succesfully'

            data_db = {'objective': 'CreateInputTable', 'status': 'ok', 'error': '',
                       'message': "Training data table Creation Started",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            db_obj_input = log_insertion_to_db(tablename)
            message = 'DataBase Input Table Created Succesfully'

            data_db = {'objective': 'CreateInputTable', 'status': 'ok', 'error': '',
                       'message': "Training data table Created", 'file': '',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)



        except Exception as e:
            file = open("Training_Logs/DbTableCreateLog.txt", 'a+')
            self.logger.log(file, "Error while creating table: %s " % e)
            file.close()
            file = open("Training_Logs/DataBaseConnectionLog.txt", 'a+')
            self.logger.log(file, "Closed database successfully" )
            file.close()
            raise e
        return db_obj_input.table

    def insertIntoTableGoodData(self,table):

        """
                               Method Name: insertIntoTableGoodData
                               Description: This method inserts the Good data files from the Good_Raw folder into the
                                            above created table.
                               Output: None
                               On Failure: Raise Exception

                                Written By: iNeuron Intelligence
                               Version: 1.0
                               Revisions: None

        """


        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')
        data_db = {'objective': 'rawdata', 'message': "Insertion of Data into Table started",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        bucket = self.resource.Bucket(self.bucket)
        files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdata/') if obj.size]
        try:
            for file in files:
                obj = self.client.get_object(
                    Bucket=self.bucket,
                    Key=file)
                data = pd.read_csv(obj['Body'])
                data.drop('Unnamed: 0', axis=1, inplace=True)
                try:
                    out = json.loads(data.to_json(orient='records'))
                    table.insert_many(out)
                    message = 'value inserted succesfully'
                    data_db = {'objective': 'insertIntoTableGoodData', 'status': 'ok', 'error': '',
                               'message': message, 'file': file, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)


                except:
                    pass
            data_db = {'objective': 'rawdata', 'message': "Insertion of Data into Table completed",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            print("data inserted")
        except Exception as e:

            self.logger.log(log_file,"Error while creating table: %s " % e)
            #shutil.move(goodFilePath+'/' + file, badFilePath)
            self.logger.log(log_file, "File Moved Successfully %s" % file)
            log_file.close()
        log_file.close()


    # def selectingDatafromtableintocsv(self,Database):
    #
    #     """
    #                            Method Name: selectingDatafromtableintocsv
    #                            Description: This method exports the data in GoodData table as a CSV file. in a given location.
    #                                         above created .
    #                            Output: None
    #                            On Failure: Raise Exception
    #
    #                             Written By: iNeuron Intelligence
    #                            Version: 1.0
    #                            Revisions: None
    #
    #     """
    #
    #     self.fileFromDb = 'Training_FileFromDB/'
    #     self.fileName = 'InputFile.csv'
    #     log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
    #     try:
    #         conn = self.dataBaseConnection(Database)
    #         sqlSelect = "SELECT *  FROM Good_Raw_Data"
    #         cursor = conn.cursor()
    #
    #         cursor.execute(sqlSelect)
    #
    #         results = cursor.fetchall()
    #         # Get the headers of the csv file
    #         headers = [i[0] for i in cursor.description]
    #
    #         #Make the CSV ouput directory
    #         if not os.path.isdir(self.fileFromDb):
    #             os.makedirs(self.fileFromDb)
    #
    #         # Open CSV file for writing.
    #         csvFile = csv.writer(open(self.fileFromDb + self.fileName, 'w', newline=''),delimiter=',', lineterminator='\r\n',quoting=csv.QUOTE_ALL, escapechar='\\')
    #
    #         # Add the headers and data to the CSV file.
    #         csvFile.writerow(headers)
    #         csvFile.writerows(results)
    #
    #         self.logger.log(log_file, "File exported successfully!!!")
    #         log_file.close()
    #
    #     except Exception as e:
    #         self.logger.log(log_file, "File exporting failed. Error : %s" %e)
    #         log_file.close()





