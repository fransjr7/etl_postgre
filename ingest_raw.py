from usecase.source_to_raw import SourceToRaw
from config.config import Config

# Initial configuration
cfg = Config().conf["postgre"]
src = "C:\\Users\\frans\\OneDrive\\Documents\\GitHub\\etl_postgre\\source"

# init usecase
postgre_job = SourceToRaw(cfg)
postgre_job.load_to_raw(src)
postgre_job.finish()




