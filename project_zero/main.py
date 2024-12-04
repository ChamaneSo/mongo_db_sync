import pymongo
from bson.json_util import dumps
from dotenv import dotenv_values
from watchers.payouts_watcher import PayoutsWatcher

config = dotenv_values(".env")

client = pymongo.MongoClient(config['MONGO_CONNECTION_STRING'])
db = client.get_database(name=config["MONGO_DB_NAME"])
payouts_watcher = PayoutsWatcher()


with db.watch() as stream:
    print('\nA change stream is open on the project zero database.  Currently watching ...\n\n')

    for change in stream:
        print(dumps(change, indent = 2))
        print(" --- ")

        if change["ns"]["coll"] == "payouts":
            payouts_watcher.watch(change=change)


