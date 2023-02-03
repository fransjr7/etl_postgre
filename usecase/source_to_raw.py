import pandas as pd 
import pyscopg2
import os 

class SourceToRaw(config):
    def __init__:
        # Init postgre connection
        self.conn = pyscopg2.connect(
            host = config.host,
            database = config.db,
            user = config.user,
            password = config.password)
        self.pg = self.conn.cursor()

    def csv_to_raw(csv_path:str):
        try:
            # Read csv
            temp_df = pd.read_csv(csv_path).
            temp_df = temp_df.apply(lambda x: x.fillna(0) if x.dtype.kind in 'biufc' else x.fillna('.'))
            # Ingest to postgre
            temp_df.to_sql(csv_path.split("\\")[-1].split(".")[0], con=self.conn, if_exist='false',index =False)
        except:
            print(f"CSV ingestion to Raw failed for path {csv_path}")
        finally:
            # Clear system memory
            del temp_df

    def load_to_raw(path:str):
        try:
            # check whether path is file or dir
            is_dir = True if "." in path.split("\\")[-1] else False 
            if is_dir and path[-1] != "\\":
                path += "\\"
            
            if is_dir:
                # Read all file in directory
                files = os.listdir(path)
                files = list(filter(lambda x: ".csv" in x.lower(), files))
                for file in files:
                    csv_to_raw(path+file)
            elif (path.split(".")[-1]).lower() == "csv":
                # Process for 1 file
                csv_to_raw(path)
            else:
                print("Directory path is not valid !")
        except:
            print("Load directory path to raw db failed ! ")

    def finish():
        self.conn.close()