import pandas as pd

class dataTransformPredict:

     """
                  This class shall be used for transforming the Good Raw Training Data before loading it in Database!!.

                  Written By: Shahriar Sourav
                  Version: 1.0
                  Revisions: None

                  """

     def __init__(self, client, resource, bucket):
          self.client = client
          self.resource = resource
          self.bucket = bucket

     def addQuotesToStringValuesInColumn(self):

          """
                                  Method Name: addQuotesToStringValuesInColumn
                                  Description: This method replaces the missing values in columns with "NULL" to
                                               store in the table. We are using substring in the first column to
                                               keep only "Integer" data for ease up the loading.
                                               This column is anyways going to be removed during prediction.

                                   Written By: Shahriar Sourav
                                  Version: 1.0
                                  Revisions: None

                                          """

          try:
               bucket = self.resource.Bucket('cementstrenghtprediction')
               files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdata/') if obj.size]

               for file in files:
                    obj = self.client.get_object(
                         Bucket=self.bucket,
                         Key=file)
                    data = pd.read_csv(obj['Body'])
                    data['DATE'] = data["DATE"].apply(lambda x: "'" + str(x) + "'")

          except Exception as e:
               raise e
