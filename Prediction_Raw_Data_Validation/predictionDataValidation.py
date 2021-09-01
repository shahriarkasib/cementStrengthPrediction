import re
import json
import pandas as pd
from io import StringIO
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt




class Prediction_Data_validation:
    """
               This class shall be used for handling all the validation done on the Raw Prediction Data!!.

               Written By: Shahriar Sourav
               Version: 1.0
               Revisions: None

               """

    def __init__(self,client, resource, bucket):
        self.client = client
        self.resource = resource
        self.bucket = bucket
        self.db_obj = log_insertion_to_db('RawDataValidationLogPrediction')




    def valuesFromSchema(self):
        """
                                Method Name: valuesFromSchema
                                Description: This method extracts all the relevant information from the pre-defined "Schema" file.
                                Output: LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, Number of Columns
                                On Failure: Raise ValueError,KeyError,Exception

                                 Written By: Shahriar Sourav
                                Version: 1.0
                                Revisions: None

                                        """
        try:
            obj = self.client.get_object(
                Bucket=self.bucket,
                Key='dataSchema/schema_prediction.json')
            dic = json.load(obj['Body'])
            print("Got Schema")
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            message = "LengthOfDateStampInFile:: %s" % LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile + "\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            data_db = {'objective': 'schema_validation', 'status': 'ok', 'error_type': '',
                       'file_name': '', 'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

        except ValueError:
            data_db = {'objective': 'schema_validation', 'status': 'error', 'error_type':
                'ValueError', 'file_name': '', 'message': 'Value Error has occured',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise ValueError

        except KeyError:
            data_db = {'objective': 'schema_validation', 'status': 'error', 'error_type': 'KeyError', 'file_name': '',
                       'message': 'key error has occured', 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise KeyError

        except Exception as e:
            data_db = {'objective': 'schema_validation', 'status': 'error', 'error_type': 'ExceptionError',
                       'file_name': '',
                       'message': '', 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):

        """
                                      Method Name: manualRegexCreation
                                      Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                                  This Regex is used to validate the filename of the prediction data.
                                      Output: Regex pattern
                                      On Failure: None

                                       Written By: Shahriar Sourav
                                      Version: 1.0
                                      Revisions: None

                                              """
        regex = "['cement_strength']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                        Method Name: createDirectoryForGoodBadRawData
                                        Description: This method creates directories to store the Good Data and Bad Data
                                                      after validating the prediction data.

                                        Output: None
                                        On Failure: OSError

                                         Written By: Shahriar Sourav
                                        Version: 1.0
                                        Revisions: None

                                                """
        try:
            aws_object = self.resource.Object('cementstrengthproject', 'goodrawdataprediction/')
            aws_object.put()

            aws_object = self.resource.Object('cementstrengthproject', 'badrawdataprediction/')
            aws_object.put()
            message = "Directory for GoodBadrawData created Succesfully"
            data_db = {'objective': 'createDirectoryForGoodBadRawData', 'status': 'ok', 'error_type': '',
                       'file_name': '', 'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'createDirectoryForGoodBadRawData', 'status': 'error', 'error_type': 'OSError',
                       'file_name': '',
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise OSError

    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
            Method Name: validationFileNameRaw
            Description: This function validates the name of the prediction csv file as per given name in the schema!
                         Regex pattern is used to do the validation.If name format do not match the file is moved
                         to Bad Raw Data folder else in Good raw data.
            Output: None
            On Failure: Exception

             Written By: Shahriar Sourav
            Version: 1.0
            Revisions: None

        """
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.

        self.createDirectoryForGoodBadRawData()

        bucket = self.resource.Bucket('cementstrengthproject')
        files = [obj.key for obj in bucket.objects.filter(Prefix='predictiondata/') if obj.size]

        try:
            for filename in files:
                filename = filename.split('/')[1]
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[2]) == LengthOfDateStampInFile:
                        if len(splitAtDot[3]) == LengthOfTimeStampInFile:
                            copy_source = {
                                'Bucket': self.bucket,
                                'Key': 'predictiondata/' + filename
                            }
                            self.resource.meta.client.copy(copy_source, self.bucket,
                                                           'goodrawdataprediction/' + filename)
                            message = "Valid File name!! File moved to GoodRaw Folder "
                            data_db = {'objective': 'FileNameValidation', 'status': 'ok',
                                       'error_type': '', 'file_name': filename,
                                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                            self.db_obj.insert_data(data_db)

                        else:
                            copy_source = {
                                'Bucket': self.bucket,
                                'Key': 'predictiondata/' + filename
                            }
                            self.resource.meta.client.copy(copy_source, self.bucket,
                                                           'badrawdataprediction/' + filename)
                            message = "Invalid File name!! File moved to BadRaw Folder "
                            data_db = {'objective': 'FileNameValidation', 'status': 'error',
                                       'error_type': 'Lenght of Timestamp Mismatched', 'file_name': filename,
                                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                            self.db_obj.insert_data(data_db)

                    else:
                        copy_source = {
                            'Bucket': self.bucket,
                            'Key': 'predictiondata/' + filename
                        }
                        self.resource.meta.client.copy(copy_source, self.bucket,
                                                       'badrawdataprediction/' + filename)
                        message = "Invalid File name!! File moved to BadRaw Folder "
                        data_db = {'objective': 'FileNameValidation', 'status': 'error',
                                   'error_type': 'Lenght of Date Sample Mismatched', 'file_name': filename,
                                   'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                        self.db_obj.insert_data(data_db)
                else:
                    copy_source = {
                        'Bucket': self.bucket,
                        'Key': 'predictiondata/' + filename
                    }
                    self.resource.meta.client.copy(copy_source, self.bucket,
                                                   'badrawdataprediction/' + filename)
                    message = "Invalid File name!! File moved to BadRaw Folder "
                    data_db = {'objective': 'FileNameValidation', 'status': 'error',
                               'error_type': 'File Naming convention Mismatched', 'file_name': filename,
                               'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)
        except Exception as e:
            message = str(e)
            data_db = {'objective': 'FileNameValidation', 'status': 'error',
                       'error_type': 'ExceptionError', 'file_name': '',
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e




    def validateColumnLength(self,NumberofColumns):
        """
                    Method Name: validateColumnLength
                    Description: This function validates the number of columns in the csv files.
                                 It is should be same as given in the schema file.
                                 If not same file is not suitable for processing and thus is moved to Bad Raw Data folder.
                                 If the column number matches, file is kept in Good Raw Data for processing.
                                The csv file is missing the first column name, this function changes the missing name to "Wafer".
                    Output: None
                    On Failure: Exception

                     Written By: Shahriar Sourav
                    Version: 1.0
                    Revisions: None

             """
        try:
            bucket = self.resource.Bucket(self.bucket)
            files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdataprediction/') if obj.size]

            for file in files:
                obj = self.client.get_object(
                Bucket=self.bucket,
                Key=file)
                csv = pd.read_csv(obj['Body'])
                if csv.shape[1] == NumberofColumns:
                    pass
                else:
                    copy_source = {
                        'Bucket': self.bucket,
                        'Key': file
                    }
                    self.resource.meta.client.copy(copy_source, self.bucket,
                                                   'badrawdataprediction/' + file.split('/')[1])
                    self.resource.Object(self.bucket,file).delete()
                    print("baddata column length")
                    message = "Invalid Column Length for the file!! File moved to Bad Raw Folder"
                    data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                               'error_type': 'Error occured while validating Column Length', 'file_name': file,
                               'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                    self.db_obj.insert_data(data_db)


        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                       'error_type': 'OSError', 'file_name': "",
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise OSError

        except Exception as e:
            message = str(e)
            data_db = {'objective': 'ColumnLengthValidation', 'status': 'error',
                       'error_type': 'ExceptionError', 'file_name': '',
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: Shahriar Sourav
                                  Version: 1.0
                                  Revisions: None

                              """
        try:

            bucket = self.resource.Bucket(self.bucket)
            files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdataprediction/') if obj.size]

            for file in files:
                obj = self.client.get_object(
                    Bucket=self.bucket,
                    Key=file)
                csv = pd.read_csv(obj['Body'])
                count = 0
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        copy_source = {
                            'Bucket': self.bucket,
                            'Key': file
                        }
                        self.resource.meta.client.copy(copy_source, self.bucket,
                                                       'badrawdataprediction/' + file.split('/')[1])
                        self.resource.Object(self.bucket,file).delete()
                        print("baddata missing values ")
                        message = "Invalid Column Length for the file!! File moved to Bad Raw Folder"
                        data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                                   'error_type': 'Whole Column has Missing Value', 'file_name': file,
                                   'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
                        self.db_obj.insert_data(data_db)
                        break
                if count==0:
                    #csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv_buffer = StringIO()
                    csv.to_csv(csv_buffer)
                    self.resource.Object(self.bucket, file).put(Body=csv_buffer.getvalue())

        except OSError as ex:
            message = str(ex)
            data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                       'error_type': 'OSError', 'file_name': "",
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise OSError
        except Exception as e:
            message = str(e)
            data_db = {'objective': 'validateMissingValuesInWholeColumn', 'status': 'error',
                       'error_type': 'ExceptionError', 'file_name': '',
                       'message': message, 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e













