from bson import ObjectId
from dotenv import dotenv_values
import psycopg2
from psycopg2.extras import DictCursor
from bson.json_util import dumps

config = dotenv_values(".env")

class AccountsTransformer:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=config["POSTGRES_DB"],
            user=config["POSTGRES_USER"],
            password=config["POSTGRES_PASSWORD"],
            host=config["POSTGRES_HOST"],
            port=config["POSTGRES_PORT"]
        )

        self.cursor = self.connection.cursor(cursor_factory=DictCursor)

    def __del__(self):
        self.cursor.close()
        self.connection.close()

    def __map_clean_fields_for_insert(self, data: dict):

        self.cursor.execute(f"SELECT * FROM users WHERE id = '{id}'")

        user = self.cursor.fetchone()

        insert_data = {}
        insert_data["id"] = str(ObjectId(data["_id"]))
        insert_data["userId"] = data["userId"] if "userId" in data.keys() else None
        insert_data["email"] = user["email"] if (user and "email" in user.keys() ) else None
        insert_data["accountId"] = data["accountId"] if "accountId" in data.keys() else insert_data["id"]
        insert_data["accountName"] = data["accountName"] if "accountName" in data.keys() else None
        insert_data["accountType"] = data["type"] if "type" in data.keys() else None
        insert_data["accountStatus"] = data["status"] if "status" in data.keys() else None
        insert_data["accountStatusUpdatedAt"] = str(data["updatedAt"]) if "updatedAt" in data.keys() else None
        insert_data["riskAttributes"] = {
            "initialBalance" : data["statrtingBalance"] if "statrtingBalance" in data.keys() else None,
            "balance" : data["balance"] if "balance" in data.keys() else None,
            "cashOnHand" : data["startOfDayBalance"] if "startOfDayBalance" in data.keys() else None,
            "profitAndLoss" : data["profitAndLoss"] if "profitAndLoss" in data.keys() else None,
            "highestProfitAndLoss" : data["highestDailyPnl"] if "highestDailyPnl" in data.keys() else None,
            "autoLiquidateThreshold" : None,
            "drawdown" : data["drawDownLimit"] if "drawDownLimit" in data.keys() else None,
            "tradingDays" : "0"
        }
        insert_data["productCode"] = None
        insert_data["targetReached"] = False
        insert_data["performanceAccountId"] = None
        insert_data["extraData"] = None
        self.cursor.execute("""SELECT * FROM data_provider_user WHERE "userId" = '%s'""", (data["userId"],))
        data_provider_user = self.cursor.fetchone()
        insert_data["dataProviderAccountId"] = data_provider_user["id"] if data_provider_user and "id" in data_provider_user.keys() else None
        insert_data["dataProviderAccountName"] = data_provider_user["username"] if data_provider_user and "username" in data_provider_user.keys() else None
        insert_data["dataProviderUsername"] = data_provider_user["username"] if data_provider_user and "username" in data_provider_user.keys() else None
        insert_data["dataProviderType"] = data_provider_user["type"] if data_provider_user and "type" in data_provider_user.keys() else None
        insert_data["createdAt"] = str(data["startDate"]) if "startDate" in data.keys() else None
        insert_data["updatedAt"] = str(data["updatedAt"]) if "updatedAt" in data.keys() else None
        print(dumps(insert_data, indent=2))
        return insert_data
    
    def transform_for_insert(self, data: dict):
        return self.__map_clean_fields_for_insert(data)
    
    def __map_clean_fields_for_update(self,id: str, data: dict):
        self.cursor.execute(f"SELECT * FROM users WHERE id = '{id}'")
        user = self.cursor.fetchone()
        self.cursor.execute(f"SELECT * FROM accounts WHERE id = '{id}'")
        account = self.cursor.fetchone()
        update_data = account
        
        update_data["userId"] = data["userId"] if "userId" in data.keys() else update_data["userId"]

        update_data["accountId"] = data["accountId"] if "accountId" in data.keys() else update_data["accountId"]
        update_data["accountName"] = data["accountName"] if "accountName" in data.keys() else update_data["accountName"]
        update_data["accountType"] = data["type"] if "type" in data.keys() else update_data["accountType"]
        update_data["accountStatus"] = data["status"] if "status" in data.keys() else update_data["accountStatus"]
        update_data["accountStatusUpdatedAt"] = str(data["updatedAt"]) if "updatedAt" in data.keys() else update_data["accountStatusUpdatedAt"]
        update_data["riskAttributes"] = {
            "initialBalance" : data["statrtingBalance"] if "statrtingBalance" in data.keys() else update_data["riskAttributes"]["initialBalance"],
            "balance" : data["balance"] if "balance" in data.keys() else update_data["riskAttributes"]["balance"],
            "cashOnHand" : data["startOfDayBalance"] if "startOfDayBalance" in data.keys() else update_data["riskAttributes"]["cashOnHand"],
            "profitAndLoss" : data["profitAndLoss"] if "profitAndLoss" in data.keys() else update_data["riskAttributes"]["profitAndLoss"],
            "highestProfitAndLoss" : data["highestDailyPnl"] if "highestDailyPnl" in data.keys() else update_data["riskAttributes"]["highestProfitAndLoss"],
            "autoLiquidateThreshold" : None,
            "drawdown" : data["drawDownLimit"] if "drawDownLimit" in data.keys() else update_data["riskAttributes"]["drawdown"],
            "tradingDays" : "0"
        }
        update_data["productCode"] = None
        update_data["targetReached"] = False
        update_data["performanceAccountId"] = None
        update_data["extraData"] = None
        print(dumps(update_data, indent=2))
        return update_data

    
    def transform_for_update(self, id: str, data: dict):
        return self.__map_clean_fields_for_update(id,data)