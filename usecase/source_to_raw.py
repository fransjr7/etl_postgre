import pandas as pd 
import psycopg2
import os 
from repo.postgre_repo import PostgreRepo

class SourceToRaw():
    def __init__(self, config):
        # Init postgre connection
        pg_client = PostgreRepo(config)
        self.conn = pg_client.conn
        self.pg = pg_client.pg
        self.engine= pg_client.engine

    def csv_to_raw(self, csv_path:str):
        try:
            # Read csv
            print(csv_path)
            temp_df = pd.read_csv(csv_path)
            temp_df = temp_df.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna('.'))
            print(temp_df.head(5))
            # Ingest to postgre
            print("public."+csv_path.split("\\")[-1].split(".")[0])
            temp_df.to_sql(csv_path.split("\\")[-1].split(".")[0], con=self.engine, if_exists='replace',index =False)
        except Exception as error:
            raise error
            print(f"CSV ingestion to Raw failed for path {csv_path}")
        finally:
            # Clear system memory
            del temp_df

    def load_to_raw(self, path:str):
        try:
            # check whether path is file or dir
            is_dir = False if "." in path.split("\\")[-1] else True 
            if is_dir and path[-1] != "\\":
                path += "\\"
            print(is_dir)
            print(path)
            if is_dir:
                # Read all file in directory
                files = os.listdir(path)
                files = list(filter(lambda x: ".csv" in x.lower(), files))
                for file in files:
                    print(path+file)
                    self.csv_to_raw(path+file)
            elif (path.split(".")[-1]).lower() == "csv":
                # Process for 1 file
                self.csv_to_raw(path)
            else:
                print("Directory path is not valid !")
        except Exception as error :
            raise error
            print("Load directory path to raw db failed ! ")

    def finish(self):
        self.conn.close()