index_create = """
    CREATE INDEX {{param.index_name}} ON {{param.table_name}} ({{param.column_list}});
"""