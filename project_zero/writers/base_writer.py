import psycopg2
from dotenv import dotenv_values
from psycopg2 import sql
import json
from datetime import datetime
from psycopg2.extras import Json

config = dotenv_values(".env")

class BaseWriter:
    def __init__(self):
        self.connection = psycopg2.connect(
            dbname=config["POSTGRES_DB"],
            user=config["POSTGRES_USER"],
            password=config["POSTGRES_PASSWORD"],
            host=config["POSTGRES_HOST"],
            port=config["POSTGRES_PORT"]
        )

    def _create_table(self, table_name, schema: dict):
        columns_with_types = ", ".join([f'"{col}" {dtype}' for col, dtype in schema.items()])
        
        query = f"CREATE TABLE IF NOT EXISTS {table_name} ({columns_with_types})"
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
            cursor.close()
        
    def _create_type(self, type_name, options: list):
        options_str = ", ".join(options)
        query = f"""
            DO $$ 
            BEGIN
                IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = '{type_name}') THEN
                    CREATE TYPE {type_name} AS ENUM ({options_str});
                END IF;
            END $$;
        """
        with self.connection.cursor() as cursor:
            cursor.execute(query)
            self.connection.commit()
            cursor.close()

    def _insert(self, table_name, data_dict: dict):
        columns = ', '.join(f'"{key}"' for key in data_dict.keys())
        values_placeholders = ', '.join(f'%({key})s' for key in data_dict.keys())

        # Formulate the insert query
        insert_query = f'INSERT INTO "{table_name}" ({columns}) VALUES ({values_placeholders})'

        if "compliance" in data_dict:
            data_dict["compliance"] = Json(eval(data_dict["compliance"])) if isinstance(data_dict["compliance"], str) else Json(data_dict["compliance"])
        if "info" in data_dict:
            data_dict["info"] = Json(eval(data_dict["info"])) if isinstance(data_dict["info"], str) else Json(data_dict["info"])

        if "previousEmails" in data_dict:
            data_dict["previousEmails"] = data_dict["previousEmails"] if isinstance(data_dict["previousEmails"], list) else eval(data_dict["previousEmails"])
        if "previousInfo" in data_dict:
            data_dict["previousInfo"] = data_dict["previousInfo"] if isinstance(data_dict["previousInfo"], list) else eval(data_dict["previousInfo"])

        if "riskAttributes" in data_dict:
            data_dict["riskAttributes"] = Json(eval(data_dict["riskAttributes"])) if isinstance(data_dict["riskAttributes"], str) else Json(data_dict["riskAttributes"])
        
        with self.connection.cursor() as cursor:
            cursor.execute(insert_query, data_dict)
            cursor.close()
        self.connection.commit()


    def _update(self, table_name, id: str, data_dict: dict, id_column="id"):

        set_clause = ', '.join([f'"{key}" = %s' for key in data_dict.keys()])
        query = f'UPDATE "{table_name}" SET {set_clause} WHERE id = %s'
        data = list(data_dict.values())
        data.append(id)
        with self.connection.cursor() as cursor:
            cursor.execute(query, data)
            cursor.close()
        self.connection.commit()
    

    def _delete(self, table_name, id: str):
        query = f'DELETE FROM "{table_name}" WHERE id = %s'
        with self.connection.cursor() as cursor:
            cursor.execute(query, (id,))
            cursor.close()
        self.connection.commit()
               
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.connection.close()

    def __del__(self):
        self.connection.close()