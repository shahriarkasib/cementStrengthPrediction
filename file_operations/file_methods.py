import pickle
from datetime import datetime as dt


class File_Operation:
    """
                This class shall be used to save the model after training
                and load the saved model for prediction.

                Written By: Shahriar Sourav
                Version: 1.0
                Revisions: None

                """
    def __init__(self,db_obj, client,resource):
        self.client = client
        self.resource = resource
        self.db_obj = db_obj
        print("object ccreated")

    def save_model(self,model,filename):
        """
            Method Name: save_model
            Description: Save the model file to directory
            Outcome: File gets saved
            On Failure: Raise Exception

            Written By: Shahriar Sourav
            Version: 1.0
            Revisions: None
        """

        data_db = {'objective': 'SaveModel', 'status': 'ok', 'error': '',
                   'message': 'Entered the save_model method of the File_Operation class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        try:

            bucket = 'cementstrengthproject'
            if self.resource.Bucket(bucket).creation_date is None:
                print("creating buket")
                print("creating bucket for models")
                self.client.create_bucket(
                    Bucket=bucket,
                    CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'}
                )
            else:
                pass
            key = 'modelfiles/' + filename + '.pkl'

            pickle_byte_obj = pickle.dumps(model)
            self.resource.Object(bucket, key).put(Body=pickle_byte_obj)
            print("model file saved")

            data_db = {'objective': 'SaveModel', 'status': 'ok', 'error': '',
                       'message': 'Model File  saved. Exited the save_model method of the Model_Finder class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            return 'success'
        except Exception as e:
            data_db = {'objective': 'SaveModel', 'status': 'error', 'error': 'ExceptionError',
                       'message': 'Model File could not be saved. Error: ' + str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise Exception()

    def load_model(self,filename):
        """
                    Method Name: load_model
                    Description: load the model file to memory
                    Output: The Model file loaded in memory
                    On Failure: Raise Exception

                    Written By: Shahriar Sourav
                    Version: 1.0
                    Revisions: None
        """
        data_db = {'objective': 'LoadModel', 'status': 'ok', 'error': '',
                   'message': 'Entered the load_model method of the File_Operation class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        try:
            bucket = 'cementstrengthproject'
            print("bucket name is strength project")
            print(filename + '.pkl')
            response = self.client.get_object(Bucket=bucket, Key= filename + '.pkl')

            print(response)
            body = response['Body'].read()
            return pickle.loads(body)
        except Exception as e:

            data_db = {'objective': 'LoadModel', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise Exception()


    def find_correct_model_file(self,cluster_number,client,resource):
        """
                            Method Name: find_correct_model_file
                            Description: Select the correct model based on cluster number
                            Output: The Model file
                            On Failure: Raise Exception

                            Written By: Shahriar Sourav
                            Version: 1.0
                            Revisions: None
                """
        data_db = {'objective': 'FindCorrectModel', 'status': 'ok', 'error': '',
                   'message': 'Entered the find_correct_model_file method of the File_Operation class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        try:
            self.cluster_number = cluster_number
            bucket = self.resource.Bucket('cementstrengthproject')
            try:
                files = [obj.key for obj in bucket.objects.filter(Prefix = 'modelfiles/')]
                print(files)
                for file in files:
                    if ((file.find(str(self.cluster_number))) != -1):
                        key = file
            except:
                pass

            data_db = {'objective': 'FindCorrectModel', 'status': 'ok', 'error': '',
                       'message': 'Exited the find_correct_model_file method of the Model_Finder class.',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            return key[:-4]
        except Exception as e:

            data_db = {'objective': 'FindCorrectModel', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise Exception()