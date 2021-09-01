import pandas as pd


class dataTransform:

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
                                           Description: This method converts all the columns with string datatype such that
                                                       each value for that column is enclosed in quotes. This is done
                                                       to avoid the error while inserting string values in table as varchar.

                                            Written By: Shahriar Sourav
                                           Version: 1.0
                                           Revisions: None

                                                   """

          try:
               bucket = self.resource.Bucket(self.bucket)
               files = [obj.key for obj in bucket.objects.filter(Prefix='goodrawdata/') if obj.size]

               for file in files:
                    print(file)
                    obj = self.client.get_object(
                         Bucket=self.bucket,
                         Key=file)
                    data = pd.read_csv(obj['Body'])
                    data['DATE'] = data["DATE"].apply(lambda x: "'" + str(x) + "'")

          except Exception as e:
               raise e

