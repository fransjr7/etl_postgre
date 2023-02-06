import pandas as pd 
import psycopg2
import os 
from jinja2 import Template
from repo.postgre_repo import PostgreRepo
from lib.lib import read_file_json, read_file_string, scan_dir
from lib.query import index_create

class ProcessTable():
    def __init__(self,config):
        # Init postgre connection
        self.pg_client = PostgreRepo(config)

    def create_index(self, job_cfg):
        try:
            param ={
                "table_name": job_cfg["table_name"],
                "index_name": job_cfg["table_name"] + "_index",
                "column_list": ",".join(job_cfg["primary_key"])
            }
            template = Template(index_create)
            query = template.render(param = param)
            self.pg_client.exec_query(job_cfg["target_dataset"],query)
            print(f'Succesfullly create index {job_cfg["table_name"] + "_index"}')
        except Exception as error:
            print(f"Error in creating index : {error}")

    def process_job_repo(self, path: str):
        try:
            # Read all file in directory
            files = os.listdir(path)
            # Data pre-processing
            if "metadata_conf.json" not in files or "sql_query.sql" not in files:
                raise Exception("Error repository not contain required file")
            elif path[-1] != "\\":
                path += "\\"
            query = read_file_string(path+"sql_query.sql")
            job_cfg = read_file_json(path+"metadata_conf.json")

            # Read data from source dataset
            result = self.pg_client.exec_query_pd(job_cfg["source_dataset"],query)

            # Ingest data to target dataset
            self.pg_client.insert_to_db(result, job_cfg)

            # Create indexing of new table 
            if "primary_key" in job_cfg:
                self.create_index(job_cfg)
            print(f"Successfully processing file {path}")
        except Exception as error:
            print(f"Failed to process job in repository {path}")
            print(error)
        finally:
            del result

    def process_all_job(self, path: str):
        try:
            # list all folder in path
            list_dir = scan_dir(path)
            for dir in list_dir:
                self.process_job_repo(dir)
            
            print(f"Successfully processing all job")
        except Exception as error:
            print("Failed to process job repository")
            print(error)

    def finish(self):
        self.pg_client.close()