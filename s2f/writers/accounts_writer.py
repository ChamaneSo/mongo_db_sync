from writers.base_writer import BaseWriter
from bson import ObjectId

class AccountsWriter(BaseWriter):
    def __init__(self):
        super().__init__()
        print("Creating accounts table ...")

        schema = {
            "id": "VARCHAR(36)",                     
            "userId": "VARCHAR(36)",                 
            "email": "VARCHAR(255)",  
            "accountId": "VARCHAR(36)",
            "accountName": "VARCHAR(255)",              
            "accountType": "Integer DEFAULT NULL",      
            "accountStatus": "Integer DEFAULT NULL",  
            "accountStatusUpdatedAt": "TIMESTAMP DEFAULT NULL",   
            "riskAttributes": "JSONB DEFAULT NULL",               
            "productCode": "VARCHAR(255) DEFAULT NULL",      
            "targetReached": "BOOLEAN DEFAULT FALSE",              
            "performanceAccountId": "VARCHAR(36)",
            "extraData": "JSONB DEFAULT NULL",                    
            "dataProviderAccountId": "VARCHAR(36)",  
            "dataProviderAccountName": "VARCHAR(255)", 
            "dataProviderUsername": "VARCHAR(255)",    
            "dataProviderType": "data_provider_type", 
            "createdAt": "TIMESTAMP Default NULL",                
            "updatedAt": "TIMESTAMP DEFAULT NULL", 
        }

        #self._create_type("account_type_enum", ["'direct'", "'evaluation'", "'funded'", "'demo'"])
        #self._create_type("account_status_enum", ["'active'", "'inactive'", "'viewOnly'"])

        self._create_table("accounts", schema)

    def write_for_insert(self, data):
        self._insert("accounts", data)

    def write_for_update(self, id, data):
        id = str(ObjectId(id))
        self._update("accounts", id, data)

    def write_for_delete(self, id):
        id = str(ObjectId(id))
        self._delete("accounts", id)