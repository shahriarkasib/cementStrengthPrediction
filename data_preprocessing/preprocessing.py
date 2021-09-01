import pandas as pd
import numpy as np
from sklearn.impute import KNNImputer
from sklearn.preprocessing import StandardScaler
from datetime import datetime as dt


class Preprocessor:
    """
        This class shall  be used to clean and transform the data before training.

        Written By: Shahriar Sourav
        Version: 1.0
        Revisions: None

        """

    def __init__(self, db_obj):
        self.db_obj = db_obj

    def remove_columns(self,data,columns):
        """
                Method Name: remove_columns
                Description: This method removes the given columns from a pandas dataframe.
                Output: A pandas DataFrame after removing the specified columns.
                On Failure: Raise Exception

                Written By: Shahriar Sourav
                Version: 1.0
                Revisions: None

        """
        data_db = {'objective': 'ColumnRemoval', 'status': 'ok', 'error': '',
                   'message': "Entered the remove_columns method of the Preprocessor class",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        self.data=data
        self.columns=columns
        try:
            self.useful_data=self.data.drop(labels=self.columns, axis=1) # drop the labels specified in the columns
            data_db = {'objective': 'ColumnRemoval', 'status': 'ok', 'error': '',
                       'message': "Column removal Successful.Exited the remove_columns method of the Preprocessor class",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            return self.useful_data
        except Exception as e:
            data_db = {'objective': 'ColumnRemoval', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()

    def separate_label_feature(self, data, label_column_name):
        """
                        Method Name: separate_label_feature
                        Description: This method separates the features and a Label Coulmns.
                        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
                        On Failure: Raise Exception

                        Written By: Shahriar Sourav
                        Version: 1.0
                        Revisions: None

                """
        data_db = {'objective': 'separate_label_feature', 'status': 'ok', 'error': '',
                'message': 'Entered the separate_label_feature method of the Preprocessor class',
                'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            data_db = {'objective': 'separate_label_feature', 'status': 'ok', 'error': '',
                       'message': 'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            return self.X,self.Y
        except Exception as e:
            data_db = {'objective': 'separate_label_feature', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e), 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()

    def dropUnnecessaryColumns(self,data,columnNameList):
        """
                        Method Name: dropUnnecessaryColumns
                        Description: This method drops the unwanted columns as discussed in EDA section.

                        Written By: Shahriar Sourav
                        Version: 1.0
                        Revisions: None

                                """
        data_db = {'objective': 'dropUnnecessaryColumns', 'status': 'ok', 'error': '',
                   'message': 'Dropping Unnecessary columns',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        data = data.drop(columnNameList,axis=1)
        data_db = {'objective': 'dropUnnecessaryColumns', 'status': 'ok', 'error': '',
                   'message': 'Unnecessary columns Dropped',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        return data


    def replaceInvalidValuesWithNull(self,data):

        """
                               Method Name: replaceInvalidValuesWithNull
                               Description: This method replaces invalid values i.e. '?' with null, as discussed in EDA.

                               Written By: Shahriar Sourav
                               Version: 1.0
                               Revisions: None

                                       """
        data_db = {'objective': 'replaceInvalidValuesWithNull', 'status': 'ok', 'error': '',
                   'message': 'Replacing invalid values with null',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        try:
            for column in data.columns:
                count = data[column][data[column] == '?'].count()
                if count != 0:
                    data[column] = data[column].replace('?', np.nan)
            return data
        except Exception as e:
            data_db = {'objective': 'replaceInvalidValuesWithNull', 'status': 'ok', 'error': '',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e

    def is_null_present(self, data):
        """
                                Method Name: is_null_present
                                Description: This method checks whether there are null values present in the pandas Dataframe or not.
                                Output: Returns a Boolean Value. True if null values are present in the DataFrame, False if they are not present.
                                On Failure: Raise Exception

                                Written By: Shahriar Sourav
                                Version: 1.0
                                Revisions: None

                        """
        data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                   'message': 'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        self.null_present = False
        print("start checking for null")
        try:
            self.null_counts = data.isna().sum()  # check for the count of null values per column
            for i in self.null_counts:
                if i > 0:
                    self.null_present = True
                    break
            if (self.null_present):  # write the logs to see which columns have null values
                dataframe_with_null = pd.DataFrame()
                dataframe_with_null['columns'] = data.columns
                dataframe_with_null['missing values count'] = np.asarray(data.isna().sum())
            data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                       'message': 'Finding missing values is a success.Data written to the null values file. Exited the is_null_present method of the Preprocessor class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("returning null data")
            return self.null_present
        except Exception as e:
            data_db = {'objective': 'is_null_present', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            raise Exception()

    def standardScalingData(self,X):
        data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                   'message': 'Entered the standardScalingData class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        scalar = StandardScaler()
        X_scaled = scalar.fit_transform(X)

        data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                   'message': 'Standard Scaling of data is done',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        return X_scaled

    def logTransformation(self,X):

        data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                   'message': 'Entered the logTransformation class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        for column in X.columns:
            X[column] += 1
            X[column] = np.log(X[column])

        data_db = {'objective': 'is_null_present', 'status': 'ok', 'error': '',
                   'message': 'Log Transformation of data is done',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        return X


    def impute_missing_values(self, data):
        """
                                        Method Name: impute_missing_values
                                        Description: This method replaces all the missing values in the Dataframe using KNN Imputer.
                                        Output: A Dataframe which has all the missing values imputed.
                                        On Failure: Raise Exception

                                        Written By: Shahriar Sourav
                                        Version: 1.0
                                        Revisions: None
                     """
        data_db = {'objective': 'impute_missing_values', 'status': 'ok', 'error': '',
                   'message': 'Entered the impute_missing_values method of the Preprocessor class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        self.data = data
        try:
            imputer = KNNImputer(n_neighbors=3, weights='uniform', missing_values=np.nan)
            self.new_array = imputer.fit_transform(self.data)  # impute the missing values
            # convert the nd-array returned in the step above to a Dataframe
            self.new_data = pd.DataFrame(data=self.new_array, columns=self.data.columns)
            data_db = {'objective': 'impute_missing_values', 'status': 'ok', 'error': '',
                       'message': 'Imputing missing values Successful. Exited the impute_missing_values method of the Preprocessor class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            return self.new_data
        except Exception as e:

            data_db = {'objective': 'impute_missing_values', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()

    def get_columns_with_zero_std_deviation(self,data):
        """
                                                Method Name: get_columns_with_zero_std_deviation
                                                Description: This method finds out the columns which have a standard deviation of zero.
                                                Output: List of the columns with standard deviation of zero
                                                On Failure: Raise Exception

                                                Written By: Shahriar Sourav
                                                Version: 1.0
                                                Revisions: None
                             """
        data_db = {'objective': 'get_columns_with_zero_std_deviation', 'status': 'ok', 'error': '',
                   'message': 'Entered the get_columns_with_zero_std_deviation method of the Preprocessor class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        self.columns=data.columns
        self.data_n = data.describe()
        self.col_to_drop=[]
        try:
            for x in self.columns:
                if (self.data_n[x]['std'] == 0): # check if standard deviation is zero
                    self.col_to_drop.append(x)  # prepare the list of columns with standard deviation zero
            data_db = {'objective': 'get_columns_with_zero_std_deviation', 'status': 'ok', 'error': '',
                       'message': 'Column search for Standard Deviation of Zero Successful. Exited the get_columns_with_zero_std_deviation method of the Preprocessor class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            return self.col_to_drop

        except Exception as e:
            data_db = {'objective': 'get_columns_with_zero_std_deviation', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise e