import yaml
from pymongo import MongoClient
import bson
from pprint import pprint

with open("configuration/configuration.yaml") as f:
    CONFIG = yaml.load(f, Loader=yaml.FullLoader)
    mongourl = CONFIG["mongourl"]
    MONGO_CLIENT = MongoClient(mongourl)

collection = MONGO_CLIENT["news"]["article"]
not_processed = collection.find(
            {
                "$or": [
                    {"processedEncoding": False},
                    {"processedEncoding": {"$exists": False}},
                ]
            }
        )
i = 0
for doc in not_processed:
    if i == 0:
        query = {"_id": doc["_id"]}
        newvalues = {
            "$set": {
                "testUpdate": True,
            }
        }
        pprint(doc)
        collection.update_one(query, newvalues)
        i += 1
    else:
        break
