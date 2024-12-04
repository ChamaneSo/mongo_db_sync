from writers.base_writer import BaseWriter
from bson import ObjectId
import uuid
from writers.accounts_writer import AccountsWriter

class UsersWriter(BaseWriter):
    def __init__(self):
        super().__init__()
        print("Creating users & data provider user tables ...")
        schema_users = {
            "id": "VARCHAR(255)",
            "userId" : "VARCHAR(255) NOT NULL",
            "email": "VARCHAR(255) NOT NULL",
            "emailVerified": "BOOLEAN DEFAULT TRUE",
            "previousEmails": "TEXT[]",
            "password": "VARCHAR(255) NOT NULL",
            "phone": "VARCHAR(255) DEFAULT NULL",
            "phoneVerified": "BOOLEAN DEFAULT TRUE",
            "info": "JSONB DEFAULT NULL",
            "previousInfo": "JSONB DEFAULT NULL",
            "compliance": "JSONB DEFAULT NULL",
            "status": "users_status NOT NULL",
            "createdAt": "TIMESTAMP DEFAULT NULL",
            "updatedAt": "TIMESTAMP DEFAULT NULL"
        }
        self._create_type("users_status", ["'active'", "'banned'", "'blackListed'"])
        self._create_table("users", schema=schema_users)

        schema_data_provider = {
            "id": "VARCHAR(255)",             # Assuming UUID type for id
            "userId": "VARCHAR(255) NOT NULL",         # Assuming UUID type for userId
            "type": "data_provider_type DEFAULT 'projectx'",           # Using ENUM for type
            "status": "data_provider_status DEFAULT 'active'",         # Using ENUM for status
            "username": "VARCHAR(255) NOT NULL",     # Username as string
            "password": "VARCHAR(255) NOT NULL",     # Password as string (should be encrypted)
            "email": "VARCHAR(255) NOT NULL"
        }
        self._create_type("data_provider_status", ["'active'", "'inactive'"])
        self._create_type("data_provider_type", ["'rithmic'" , "'projectx'" , "'tradeovate'" , "'CQG'"])
        self._create_table("data_provider_user", schema=schema_data_provider)
        self.accounts_writer = AccountsWriter()

    def write_for_insert(self, data: dict, data_provider_user: dict):
        self._insert("users", data)
        self._insert("data_provider_user", data_provider_user)

    def write_for_update(self,id: str, data: dict, data_provider_user: dict):
        id = str(ObjectId(id))
        if data != {}:
            self._update("users", id, data)
        if data_provider_user != {}:
            self._update("data_provider_user", id, data_provider_user)

    def write_for_delete(self, id : str):
        id = str(ObjectId(id))
        self._delete("users", id)
        self._delete("data_provider_user", id)