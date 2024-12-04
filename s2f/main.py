import pymongo
from bson.json_util import dumps
from dotenv import dotenv_values
from watchers.users_watcher import UsersWatcher
from watchers.accounts_watcher import AccountsWatcher

config = dotenv_values(".env")

client = pymongo.MongoClient(config['MONGO_CONNECTION_STRING'])
db = client.get_database(name=config["MONGO_DB_NAME"])
users_watcher = UsersWatcher()
accounts_watcher = AccountsWatcher()

with db.watch() as stream:
    print('\nA change stream is open on the s2f database.  Currently watching ...\n\n')

    for change in stream:
        print(dumps(change, indent = 2))
        print(" --- ")

        if change["ns"]["coll"] == "users":
            users_watcher.watch(change=change)

        if change["ns"]["coll"] == "accounts":
            accounts_watcher.watch(change=change)

client.close()
