from bson import ObjectId
from bson.json_util import dumps

class PayoutsTransformer:
    def __init__(self):
        pass

    def __map_clean_fields_for_insert(self, data: dict):
        insert_data = {
            "id" : str(ObjectId(data["_id"])),
            "accountId" : data["accountId"] if "accountId" in data else None,
            "userId" : data["userId"] if "userId" in data else None,
            "amount" : data["amount"] if "amount" in data else None,
            "dataProviderAccountId" : data["dataProviderAccountId"] if "dataProviderAccountId" in data else None,
            "dataProviderType" : data["dataProviderType"] if "dataProviderType" in data else None,
            "status" : data["status"] if "status" in data else None,
            "reason" : data["reason"] if "reason" in data else None,
            "createdAt" : str(data["createdAt"]) if "createdAt" in data else None,
            "updatedAt" : str(data["createdAt"]) if "updatedAt" in data else None
        }
        dumps(insert_data, indent=2)
        return insert_data

    def transform_for_insert(self, data):
        return self.__map_clean_fields_for_insert(data)
    
    def __map_clean_fields_for_update(self, data: dict):
            if "accountId" in data:
                data["accountId"] = data["accountId"]
            if "userId" in data:
                data["userId"] = data["userId"]
            if "amount" in data:
                data["amount"] = data["amount"]
            if "dataProviderAccountId" in data:
                data["dataProviderAccountId"] = data["dataProviderAccountId"]
            if "dataProviderType" in data:
                data["dataProviderType"] = data["dataProviderType"]
            if "status" in data:
                data["status"] = data["status"]
            if "reason" in data:
                data["reason"] = data["reason"]
            if "createdAt" in data:
                data["createdAt"] = data["createdAt"]
            if "updatedAt" in data:
                data["updatedAt"] = data["updatedAt"]
            return data
    
    def transform_for_update(self, data):
        return self.__map_clean_fields_for_update(data)
