import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from file_operations import file_methods
from datetime import datetime as dt

class KMeansClustering:
    """
            This class shall  be used to divide the data into clusters before training.
            Written By: Shahriar Sourav
            Version: 1.0
            Revisions: None

            """
    def __init__(self, db_obj, client, resource):
        self.client = client
        self.resource = resource
        self.db_obj = db_obj

    def elbow_plot(self,data):
        """
                        Method Name: elbow_plot
                        Description: This method saves the plot to decide the optimum number of clusters to the file.
                        Output: A picture saved to the directory
                        On Failure: Raise Exception

                        Written By: Shahriar Sourav
                        Version: 1.0
                        Revisions: None

                """
        data_db = {'objective': 'ElbowPlot', 'status': 'ok', 'error': '',
                   'message': 'Entered the elbow_plot method of the KMeansClustering class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        wcss=[] # initializing an empty list
        try:
            for i in range (1,11):
                kmeans=KMeans(n_clusters=i,init='k-means++',random_state=42) # initializing the KMeans object
                kmeans.fit(data) # fitting the data to the KMeans Algorithm
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss) # creating the graph between WCSS and the number of clusters
            plt.title('The Elbow Method')
            plt.xlabel('Number of clusters')
            plt.ylabel('WCSS')
            # finding the value of the optimum cluster programmatically
            self.kn = KneeLocator(range(1, 11), wcss, curve='convex', direction='decreasing')

            data_db = {'objective': 'ElbowPlot', 'status': 'ok', 'error': '',
                       'message': 'The optimum number of clusters is: ' + str(
                           self.kn.knee) + ' . Exited the elbow_plot method of the KMeansClustering class',
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            return self.kn.knee

        except Exception as e:
            data_db = {'objective': 'ElbowPlot', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e), 'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()

    def create_clusters(self,data,number_of_clusters):
        """
                                Method Name: create_clusters
                                Description: Create a new dataframe consisting of the cluster information.
                                Output: A datframe with cluster column
                                On Failure: Raise Exception

                                Written By: Shahriar Sourav
                                Version: 1.0
                                Revisions: None

                        """
        data_db = {'objective': 'CreateCluster', 'status': 'ok', 'error': '',
                   'message': 'Entered the create_clusters method of the KMeansClustering class',
                   'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
        self.db_obj.insert_data(data_db)

        self.data=data
        try:
            self.kmeans = KMeans(n_clusters=number_of_clusters, init='k-means++', random_state=42)
            #self.data = self.data[~self.data.isin([np.nan, np.inf, -np.inf]).any(1)]
            self.y_kmeans=self.kmeans.fit_predict(data) #  divide data into clusters

            self.file_op = file_methods.File_Operation(self.db_obj, self.client, self.resource)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') # saving the KMeans model to directory
                                                                                    # passing 'Model' as the functions need three parameters

            self.data['Cluster']=self.y_kmeans  # create a new column in dataset for storing the cluster information
            data_db = {'objective': 'CreateCluster', 'status': 'ok', 'error': '',
                       'message': 'succesfully created ' + str(
                           self.kn.knee) + 'clusters. Exited the create_clusters method of the KMeansClustering class',
                       'time': 'very nice'}
            self.db_obj.insert_data(data_db)
            return self.data
        except Exception as e:
            data_db = {'objective': 'CreateCluster', 'status': 'error', 'error': 'ExceptionError',
                       'message': str(e),
                       'time': dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()