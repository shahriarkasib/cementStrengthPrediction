"""
This is the Entry point for Training the Machine Learning Model.

Written By: iNeuron Intelligence
Version: 1.0
Revisions: None

"""


# Doing the necessary imports
from sklearn.model_selection import train_test_split
from data_ingestion import data_loader
from data_preprocessing import preprocessing
from data_preprocessing import clustering
from best_model_finder import tuner
from file_operations import file_methods
from application_logging import logger
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt
#Creating the common Logging object


class trainModel:

    def __init__(self,client,resource,bucket):
        self.client = client
        self.resource = resource
        self.bucket = bucket
        self.db_obj = log_insertion_to_db('TrainModel')
        self.log_writer = logger.App_Logger()
        self.file_object = open("Training_Logs/ModelTrainingLog.txt", 'a+')
    def trainingModel(self):
        # Logging the start of Training
        self.log_writer.log(self.file_object, 'Start of Training')
        data_db = {'objective': 'training', 'status': 'ok', 'error': '', 'message': "Start of Training",
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)
        try:

            print("start training model")
            # Getting the data from the source
            data_db = {'objective': 'training', 'status': 'ok', 'error': '',
                       'message': "Getting Training Data from DataBase",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            # Getting the data from the source
            print("start getting data")
            data_getter=data_loader.Data_Getter(self.db_obj)
            print("data getter object created")
            data=data_getter.get_data()

            data_db = {'objective': 'training', 'status': 'ok', 'error': '',
                       'message': "Got Training Data from DataBase",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)


            """doing the data preprocessing"""

            data_db = {'objective': 'training', 'status': 'ok', 'error': '',
                       'message': "Start Prepearing Data for Training",
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            print("start data preprocessing")
            preprocessor=preprocessing.Preprocessor(self.db_obj)

            # check if missing values are present in the dataset
            print("check if null present")
            is_null_present=preprocessor.is_null_present(data)

            # if missing values are there, replace them appropriately.
            print('imputing missing values')
            if(is_null_present):
                data=preprocessor.impute_missing_values(data) # missing value imputation

            # get encoded values for categorical data

            #data = preprocessor.encodeCategoricalValues(data)

            # create separate features and labels
            print("start sperating label features")
            X, Y = preprocessor.separate_label_feature(data, label_column_name='Concrete_compressive _strength')
            # drop the columns obtained above
            #X=preprocessor.remove_columns(X,cols_to_drop)
            print("start logtransformation")
            X = preprocessor.logTransformation(X)
            """ Applying the clustering approach"""
            print("start clustering")
            kmeans=clustering.KMeansClustering(self.db_obj, self.client, self.resource) # object initialization.
            number_of_clusters=kmeans.elbow_plot(X)  #  using the elbow plot to find the number of optimum clusters

            # Divide the data into clusters
            print("start creating cluster")
            X=kmeans.create_clusters(X,number_of_clusters)

            #create a new column in the dataset consisting of the corresponding cluster assignments.
            X['Labels']=Y

            # getting the unique clusters from our dataset
            list_of_clusters=X['Cluster'].unique()

            """parsing all the clusters and looking for the best ML algorithm to fit on individual cluster"""

            for i in list_of_clusters:
                cluster_data=X[X['Cluster']==i] # filter the data for one cluster

                # Prepare the feature and Label columns
                cluster_features=cluster_data.drop(['Labels','Cluster'],axis=1)
                cluster_label= cluster_data['Labels']
                print("splitting into train and test")
                # splitting the data into training and test set for each cluster one by one
                x_train, x_test, y_train, y_test = train_test_split(cluster_features, cluster_label, test_size=1 / 3, random_state=36)

                print("Standard Scaling the data")
                x_train_scaled = preprocessor.standardScalingData(x_train)
                x_test_scaled = preprocessor.standardScalingData(x_test)
                print("start finding model")
                model_finder=tuner.Model_Finder(self.file_object,self.log_writer) # object initialization

                #getting the best model for each of the clusters
                print("getting the best model")
                best_model_name,best_model=model_finder.get_best_model(x_train_scaled,y_train,x_test_scaled,y_test)
                print("got the best model")
                #saving the best model to the directory.
                file_op = file_methods.File_Operation(self.db_obj, self.client,self.resource)
                save_model=file_op.save_model(best_model,best_model_name+str(i))

            # logging the successful Training
            self.log_writer.log(self.file_object, 'Successful End of Training')
            self.file_object.close()

        except Exception:
            # logging the unsuccessful Training
            self.log_writer.log(self.file_object, 'Unsuccessful End of Training')
            self.file_object.close()
            raise Exception