import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from io import StringIO
from application_logging.logger import App_Logger





class Raw_Data_validation:

    """
             This class shall be used for handling all the validation done on the Raw Training Data!!.

             Written By: Shahriar Sourav
             Version: 1.0
             Revisions: None

             """

    def __init__(self,client, resource, bucket):
        self.client = client
        self.resource = resource
        self.bucket = bucket
        self.Batch_Directory = 'Training_Batch_Files/'
        self.schema_path = 'schema_training.json'
        self.logger = App_Logger()


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
                Key='dataSchema/schema_training.json')
            dic = json.load(obj['Body'])
            print("Got Schema")
            pattern = dic['SampleFileName']
            LengthOfDateStampInFile = dic['LengthOfDateStampInFile']
            LengthOfTimeStampInFile = dic['LengthOfTimeStampInFile']
            column_names = dic['ColName']
            NumberofColumns = dic['NumberofColumns']

            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message ="LengthOfDateStampInFile:: %s" %LengthOfDateStampInFile + "\t" + "LengthOfTimeStampInFile:: %s" % LengthOfTimeStampInFile +"\t " + "NumberofColumns:: %s" % NumberofColumns + "\n"
            self.logger.log(file,message)

            file.close()



        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file,"ValueError:Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfDateStampInFile, LengthOfTimeStampInFile, column_names, NumberofColumns


    def manualRegexCreation(self):
        """
                                Method Name: manualRegexCreation
                                Description: This method contains a manually defined regex based on the "FileName" given in "Schema" file.
                                            This Regex is used to validate the filename of the training data.
                                Output: Regex pattern
                                On Failure: None

                                 Written By: iNeuron Intelligence
                                Version: 1.0
                                Revisions: None

                                        """
        regex = "['cement_strength']+['\_'']+[\d_]+[\d]+\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):

        """
                                      Method Name: createDirectoryForGoodBadRawData
                                      Description: This method creates directories to store the Good Data and Bad Data
                                                    after validating the training data.

                                      Output: None
                                      On Failure: OSError

                                       Written By: iNeuron Intelligence
                                      Version: 1.0
                                      Revisions: None

                                              """

        try:
            aws_object = self.resource.Object('cementstrengthproject', 'goodrawdata/')
            aws_object.put()

            aws_object = self.resource.Object('cementstrengthproject', 'badrawdata/')
            aws_object.put()

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while creating Directory %s:" % ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingGoodDataTrainingFolder
                                            Description: This method deletes the directory made  to store the Good Data
                                                          after loading the data in the table. Once the good files are
                                                          loaded in the DB,deleting the directory ensures space optimization.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            bucket = self.resource.Bucket('cementstrengthproject')
            files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdataprediction/') if obj.size]

            for file in files:
                self.resource.Object(self.bucket, file).delete()

        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):

        """
                                            Method Name: deleteExistingBadDataTrainingFolder
                                            Description: This method deletes the directory made to store the bad Data.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """

        try:
            bucket = self.resource.Bucket('cementstrengthproject')
            files = [obj.key for obj in bucket.objects.filter(Prefix='badrawdataprediction/') if obj.size]

            for file in files:
                self.resource.Object(self.bucket, file).delete()
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while Deleting Directory : %s" %s)
            file.close()
            raise OSError

    def moveBadFilesToArchiveBad(self):

        """
                                            Method Name: moveBadFilesToArchiveBad
                                            Description: This method deletes the directory made  to store the Bad Data
                                                          after moving the data in an archive folder. We archive the bad
                                                          files to send them back to the client for invalid data issue.
                                            Output: None
                                            On Failure: OSError

                                             Written By: iNeuron Intelligence
                                            Version: 1.0
                                            Revisions: None

                                                    """
        now = datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:

            source = 'Training_Raw_files_validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = 'TrainingArchiveBadData/BadData_' + str(date)+"_"+str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file,"Bad files moved to archive")
                path = 'Training_Raw_files_validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file,"Bad Raw Data Folder Deleted successfully!!")
                file.close()
        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e




    def validationFileNameRaw(self,regex,LengthOfDateStampInFile,LengthOfTimeStampInFile):
        """
                    Method Name: validationFileNameRaw
                    Description: This function validates the name of the training csv files as per given name in the schema!
                                 Regex pattern is used to do the validation.If name format do not match the file is moved
                                 to Bad Raw Data folder else in Good raw data.
                    Output: None
                    On Failure: Exception

                     Written By: iNeuron Intelligence
                    Version: 1.0
                    Revisions: None

                """

        #pattern = "['Wafer']+['\_'']+[\d_]+[\d]+\.csv"
        # delete the directories for good and bad data in case last run was unsuccessful and folders were not deleted.
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()
        self.createDirectoryForGoodBadRawData()

        bucket = self.resource.Bucket('cementstrengthproject')
        files = [obj.key for obj in bucket.objects.filter(Prefix='inputdata/') if obj.size]
        try:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            for filename in files:
                filename = filename.split('/')[1]
                if (re.match(regex, filename)):
                    splitAtDot = re.split('.csv', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))

                    if len(splitAtDot[2]) == LengthOfDateStampInFile:
                        if len(splitAtDot[3]) == LengthOfTimeStampInFile:

                            copy_source = {
                                'Bucket': self.bucket,
                                'Key': 'inputdata/' + filename
                            }
                            self.resource.meta.client.copy(copy_source, self.bucket,
                                                              'goodrawdata/' + filename)
                            self.logger.log(f,"Valid File name!! File moved to GoodRaw Folder :: %s" % filename)

                        else:
                            copy_source = {
                                'Bucket': self.bucket,
                                'Key': 'inputdata/' + filename
                            }
                            self.resource.meta.client.copy(copy_source, self.bucket,
                                                              'badrawdata/' + filename)
                            self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)

                    else:
                        copy_source = {
                            'Bucket': self.bucket,
                            'Key': 'inputdata/' + filename
                        }
                        self.resource.meta.client.copy(copy_source, self.bucket,
                                                          'badrawdata/' + filename)
                        self.logger.log(f,"Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
                else:
                    copy_source = {
                        'Bucket': 'cementstrengthproject',
                        'Key': 'inputdata/' + filename
                    }
                    self.resource.meta.client.copy(copy_source, self.bucket,
                                                      'badrawdata/' + filename)
                    self.logger.log(f, "Invalid File Name!! File moved to Bad Raw Folder :: %s" % filename)
            print("filename validated")
            f.close()

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occured while validating FileName %s" % e)
            f.close()
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

                           Written By: iNeuron Intelligence
                          Version: 1.0
                          Revisions: None

                      """
        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f,"Column Length Validation Started!!")
            bucket = self.resource.Bucket(self.bucket)
            files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdata/') if obj.size]

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
                                                   'badrawdata/' + file.split('/')[1])
                    self.resource.Object(self.bucket,file).delete()
                    self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
            self.logger.log(f, "Column Length Validation Completed!!")
            print("column length validated")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
        f.close()

    def validateMissingValuesInWholeColumn(self):
        """
                                  Method Name: validateMissingValuesInWholeColumn
                                  Description: This function validates if any column in the csv file has all values missing.
                                               If all the values are missing, the file is not suitable for processing.
                                               SUch files are moved to bad raw data.
                                  Output: None
                                  On Failure: Exception

                                   Written By: iNeuron Intelligence
                                  Version: 1.0
                                  Revisions: None

                              """
        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            bucket = self.resource.Bucket(self.bucket)
            files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdata/') if obj.size]

            for file in files:
                print(file)
                obj = self.client.get_object(
                Bucket=self.bucket,
                Key=file)
                csv = pd.read_csv(obj['Body'])
                count = 0
                print(csv.head())
                for columns in csv:
                    if (len(csv[columns]) - csv[columns].count()) == len(csv[columns]):
                        count+=1
                        copy_source = {
                            'Bucket': self.bucket,
                            'Key': file
                        }
                        self.resource.meta.client.copy(copy_source, self.bucket,
                                                       'badrawdata/' + file.split('/')[1])
                        self.resource.Object(self.bucket, file).delete()
                        self.logger.log(f,"Invalid Column Length for the file!! File moved to Bad Raw Folder :: %s" % file)
                        break
                if count==0:
                    print(csv.head())
                    #csv.rename(columns={"Unnamed: 0": "Wafer"}, inplace=True)
                    csv_buffer = StringIO()
                    csv.to_csv(csv_buffer)
                    self.resource.Object(self.bucket, file).put(Body=csv_buffer.getvalue())
            print("missing values handled")
        # except OSError:
        #     f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
        #     self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
        #     f.close()
        #     raise OSError
        # except Exception as e:
        #     f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
        #     self.logger.log(f, "Error Occured:: %s" % e)
        #     f.close()
        #     raise e
        # f.close()
        except:
            pass












