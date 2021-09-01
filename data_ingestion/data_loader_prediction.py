import pandas as pd
from log_insertion_to_db.log_insertion_to_db import log_insertion_to_db
from datetime import datetime as dt

class Data_Getter_Pred:
    """
                This class shall  be used for obtaining the data from the source for prediction.

                Written By: Shahriar Sourav
                Version: 1.0
                Revisions: None

    """
    def __init__(self, client, resource):
        self.client = client
        self.resource = resource
        self.db_obj = log_insertion_to_db("GetDataPrediction")
        self.db_obj_table = log_insertion_to_db("PredictionData")

    def get_data(self):
        """
                Method Name: get_data
                Description: This method reads the data from source.
                Output: A pandas DataFrame.
                On Failure: Raise Exception

                 Written By: Shahriar Sourav
                Version: 1.0
                Revisions: None

        """
        try:
            message = "Getting Data for Prediction"
            data_db = {"objective": "GetDataPrediction", "status": "ok", "error": "",
                       "message": message, "time": dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            data = pd.DataFrame(list(self.db_obj_table.table.find()))
            data.drop('_id', axis=1, inplace=True)

            message = "Got Data for Prediction"
            data_db = {"objective": "GetDataPrediction", "status": "ok", "error": "",
                       "message": message, "time": dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)

            return data

        except Exception as e:
            message = str(e)
            data_db = {"objective": "GetDataPrediction", "status": "error", "error": "ExceptionError",
                       "message": message, "time": dt.now().strftime("%d/%m/%Y %H:%M:%S")}
            self.db_obj.insert_data(data_db)
            raise Exception()


