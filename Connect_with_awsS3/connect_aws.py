import boto3
class aws:

    def __init__(self):

        self.region_name='us-east-1'
        self.aws_access_key_id = 'HZYEL7PLOVRNNQ3VAIKA'[::-1]
        self.aws_secret_access_key = 'ol71iQq36rg6LuU8A5qefHPsTEPHzcDBNErs3+LM'[::-1]
        try:
            self.client = boto3.client(
                                    's3',
                                    aws_access_key_id=self.aws_access_key_id,
                                    aws_secret_access_key= self.aws_secret_access_key,
                                    region_name= self.region_name
                                )

            self.resource =  boto3.resource(
                    's3',
                    aws_access_key_id=self.aws_access_key_id,
                    aws_secret_access_key=self.aws_secret_access_key,
                    region_name=self.region_name
                )
        except Exception as e:
            print(e)

    def delete_modelfiles(self, bucket_name):
        print("deleting modelfilea")
        bucket = self.resource.Bucket(bucket_name)
        bucket.objects.filter(Prefix="modelfiles/").delete()
        print("model files deleted")