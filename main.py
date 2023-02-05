from usecase.source_to_raw import SourceToRaw
from usecase.process_table import ProcessTable
from config.config import Config
from lib.lib   import scan_dir

# Initial configuration
cfg = Config().conf["postgre"]
src = "C:\\Users\\frans\\OneDrive\\Documents\\GitHub\\etl_postgre\\source"

# init usecase
# raw_ingestion = SourceToRaw(cfg)
# raw_ingestion.load_to_raw(src)
# raw_ingestion.finish()

# process all job
job_path= "C:\\Users\\frans\\OneDrive\\Documents\\GitHub\\etl_postgre\\job"
postgre_job = ProcessTable(cfg)
postgre_job.process_all_job(job_path)
postgre_job.finish()


