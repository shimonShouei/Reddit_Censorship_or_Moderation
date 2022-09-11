import pymongo
import logging
from data_layer.data_layer import DataLayer 

logging.basicConfig(format='%(asctime)s %(message)s')


class MongoDataLayer(DataLayer):
    def __init__(self, connection_string):
        super().__init__()
        self.client = pymongo.MongoClient(connection_string)
        self.curr_collection = None

    def get_collection(self, year, collection_name, submission_kind):
        db = self.client[f"reddit_{year}"].with_options(write_concern=pymongo.WriteConcern(w=0))
        collection = db[f"{collection_name}_{submission_kind}"]
        self.curr_collection = collection
        return collection
    
    async def insert_many(self, data):
        self.curr_collection.insert_many(data)
