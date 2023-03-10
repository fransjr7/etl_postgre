import pandas as pd 
import psycopg2
import os 
from sqlalchemy import create_engine


class PostgreRepo():
    def __init__(self, config):
        # Init postgre connection
        self.config = config
        self.conn = psycopg2.connect(
            host = config["host"],
            database = config["database"],
            user = config["user"],
            password = config["password"])
        self.pg = self.conn.cursor()
        self.engine = create_engine(f'postgresql+psycopg2://{config["user"]}:{config["password"]}@{config["host"]}/{config["database"]}')
    
    def change_db(self, db:str):
        try:
            self.conn.close()
            self.engine.dispose()
            self.conn = psycopg2.connect(
                host = self.config["host"],
                database = db,
                user = self.config["user"],
                password = self.config["password"])
            self.pg = self.conn.cursor()
            self.engine = create_engine(f'postgresql+psycopg2://{self.config["user"]}:{self.config["password"]}@{self.config["host"]}/{db}')
        except Exception as error:
            print("Failed to change database ")
            print(error)

    def exec_query_pd(self, db:str, query:str):
        try:
            self.change_db(db)
            res = pd.read_sql(query, self.engine)
            return res
        except Exception as error:
            print(f"error in executing query : {error}")

    def exec_query(self, db:str, query:str):
        try:
            self.change_db(db)
            self.pg.execute(query)
            self.conn.commit()
        except Exception as error:
            print(f"error in executing query : {error}")

    def insert_to_db(self, data, job_cfg):
        try:
            self.change_db(job_cfg["target_dataset"])
            data.to_sql(job_cfg["table_name"], con=self.engine, if_exists=job_cfg["write_disposition"],index =False)
        except Exception as error:
            print(f"error in inserting to db : {error}")

    def close(self):
        self.conn.close()
        self.engine.dispose()