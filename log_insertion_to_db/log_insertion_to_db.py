import pymongo
import ssl
class log_insertion_to_db:
    def __init__(self, tablename):
        self.tablename = tablename
        self.dbname = 'cementStrengthPredictionProject'
        # self.client_mongo = pymongo.MongoClient(
        #         "mongodb://localhost:27017/")
        self.client_mongo = pymongo.MongoClient\
            ("mongodb://localhost:27017/",ssl_cert_reqs=ssl.CERT_NONE)

        self.db = self.client_mongo[self.dbname]
        self.table = self.db[self.tablename]



    def insert_data(self,data):
        self.table.insert_one(data)