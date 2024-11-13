from common import InsertLineBuilderBase

VALUES_COMMAND: str = "VALUES"

def insert_statement(table_name: str, table_parameters: str) -> str:
    return f"INSERT INTO {table_name} ({table_parameters})\n"

def write_sql_insert_statement(sql_stripped_data_file, table_name, table_parameters: str):
    insert_line = insert_statement(table_name, table_parameters)
    sql_stripped_data_file.write(insert_line)

def write_sql_values_keyword_statement(sql_stripped_data_file):
    values_line = "".join([VALUES_COMMAND, "\n"])
    sql_stripped_data_file.write(values_line)

def write_sql_values_data_statement(sql_stripped_data_file, record: InsertLineBuilderBase):
    values_data = record.build_insert_values_line()
    sql_stripped_data_file.write(values_data)