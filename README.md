# etl_postgre
How to use:
1. Install dependencies in requirement.txt
2. Adjust postgre server connection detail in config folder
3. Create required database in postgre server, default (raw & dwh)
4. Run main.py

How to add pipeline:
1. Add folder in job folder
2. 1 job folder must contain metadata_conf.json for configuration file and query in sql_query.sql file
3. Follow configurantion file from other existing job
4. Job will be run in main.py

How to add source csv:
1. Add csv file to source folder
2. Job will be run in main.py
