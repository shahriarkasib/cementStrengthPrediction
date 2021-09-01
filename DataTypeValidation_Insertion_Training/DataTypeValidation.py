import pandas as pd
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
        self.db_obj = log_insertion_to_db('DBOperationLog')

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

            raise e
