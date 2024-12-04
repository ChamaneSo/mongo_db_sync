from cryptography.fernet import Fernet
from dotenv import dotenv_values
from bson.json_util import dumps
from datetime import datetime
from bson import ObjectId
from psycopg2.extras import DictCursor
from writers.base_writer import BaseWriter
import uuid

import psycopg2

config = dotenv_values(".env")

class UsersTransformer:

    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=config["POSTGRES_DB"],
            user=config["POSTGRES_USER"],
            password=config["POSTGRES_PASSWORD"],
            host=config["POSTGRES_HOST"],
            port=config["POSTGRES_PORT"]
        )

    def __map_clean_fields_for_insert(self, data: dict):
        # clean fields
        del data["__v"]
        # map fields
        data["id"] = str(ObjectId(data["_id"]))
        del data["_id"]
        data["email"] = data['email']
        data["emailVerified"] = data["emailVerified"] if "emailVerified" in data.keys() else False
        data["previousEmails"] = data["previousEmails"] if "previousEmails" in data.keys() else []
        data["password"] = data["password"] if "password" in data.keys() else self.__generate_password_crypt("project_zero_pass", config["CRYPTO_SECRET_KEY"].encode())
        data["phone"] = data["phone"] if "phone" in data.keys() else None
        data["phoneVerified"] = data["phoneVerified"] if "phoneVerified" in data.keys() else False
        data["compliance"] = data["compliance"] if "compliance" in data.keys() else {  }
        data["status"] = data["status"] if "status" in data.keys() else "active"
        data["previousInfo"] = data["previousInfo"] if "previousInfo" in data.keys() else []
        data["userId"] = str(data["userId"])
        #info
        if "info" not in data.keys():
            data["info"] = {}
        data["info"]["firstName"] = data["firstName"] if "firstName" in data.keys() else None
        data["info"]["lastName"] = data["lastName"] if "lastName" in data.keys() else None
        data["info"]["phone"] = data['phone'] if "phone" in data.keys() else None 
        data["info"]["city"] = data['city'] if "city" in data.keys() else None
        data["info"]["country"] = data['country'] if "country" in data.keys() else None
        data["info"]["address"] = data['address'] if "address" in data.keys() else None
        data['info']['state'] = data['state'] if 'state' in data.keys() else None
        data['info']['postalCode'] = data['postalCode'] if 'postalCode' in data.keys() else None
        data['info']['ip'] = data['ip'] if 'ip' in data.keys() else None

        data["createdAt"] = str(data["createdAt"]) if "createdAt" in data.keys() else None
        data["updatedAt"] = str(data["updatedAt"]) if "updatedAt" in data.keys() else None

        del data["firstName"]
        del data["lastName"]

        data_provider_user = {
            "id": data["id"],
            "userId": data["userId"],
            "type": "projectx",
            "status": "active",
            "username": data["username"],
            "password": data["password"],
            "email": data["email"]
        }
        del data["username"]
        return data, data_provider_user
    
    def __generate_password_crypt(self, password: str, key: str):
        fernet = Fernet(key)
        encrypted = fernet.encrypt(password.encode()).decode()
        return encrypted

    def transorm_for_insert(self, data: dict):
        data, data_provider_user = self.__map_clean_fields_for_insert(data)
        return data, data_provider_user
    
    def transorm_for_update(self, id , data: dict):
        id = str(ObjectId(id))
        data_provider_user = {}
        if "username" in data.keys():
            data_provider_user["username"] = data["username"]
            del data["username"]
        if "email" in data.keys():
            data["email"] = data["email"]
            data_provider_user["email"] = data["email"]
        if "emailVerified" in data.keys():
            data["emailVerified"] = data["emailVerified"]
        if "previousEmails" in data.keys():
            data["previousEmails"] = data["previousEmails"]
        if "password" in data.keys():
            data["password"] = data["password"]
            data_provider_user["password"] = data["password"]
        if "phone" in data.keys():
            data["phone"] = data["phone"]
        if "phoneVerified" in data.keys():
            data["phoneVerified"] = data["phoneVerified"]
        if "info" in data.keys():
            data["info"] = data["info"]
        if "compliance" in data.keys():
            data["compliance"] = data["compliance"]
        if "status" in data.keys():
            data["status"] = data["status"]
        if "previousInfo" in data.keys():
            data["previousInfo"] = data["previousInfo"]
        if "createdAt" in data.keys():
            data["createdAt"] = data["createdAt"]
        if "updatedAt" in data.keys():
            data["updatedAt"] = data["updatedAt"]
        if "userId" in data.keys():
            data["userId"] = data["userId"]
            data_provider_user["userId"] = data["userId"]
        
        cursor = self.connection.cursor(cursor_factory=DictCursor)
        cursor.execute(f"SELECT info, compliance  FROM users WHERE id = '{id}'")

        result = cursor.fetchone()

        data["info"] = result["info"]

        data["compliance"] = result["compliance"]

        if "firstName" in data.keys():
            data["info"]["firstName"] = data["firstName"]
            del data["firstName"]
        if "lastName" in data.keys():
            data["info"]["lastName"] = data["lastName"]
            del data["lastName"]

        
        self.connection.commit()
        cursor.close()

        return data, data_provider_user