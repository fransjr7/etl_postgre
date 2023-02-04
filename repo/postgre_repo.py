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
            self.conn = psycopg2.connect(
                host = self.config.host,
                database = db,
                user = self.config.user,
                password = self.config.password)
            self.pg = self.conn.cursor()
            self.engine = create_engine(f'postgresql+psycopg2://{self.config["user"]}:{self.config["password"]}@{self.config["host"]}/{self.config["database"]}')
        except:
            print("Failed to change database ")

    def exec_query(self, db:str, query:str):
        print("b")

    def insert_to_db(self, data, db:str, table_name:str, write_disposition: str):
        print("A")