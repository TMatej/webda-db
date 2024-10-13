import os
from DataType import DataType
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import DATA_TYPES_FILE_NAME, DATA_TYPES_DATA_FOLDER_NAME, SQL_DATA_TYPES_FILE_NAME, DATA_TYPES_TABLE_NAME
from common.file_paths import ORIGIN_PUBLICATIONS_FOLDER_PATH, DESTINATION_DATA_FOLDER_PATH

def process_abbreviation_line(line: str) -> str:
    return line.split("=>", 1)[0].strip()

def parse_value(line: str) -> str:
    return line.split("=>", 1)[1].strip(" \",\n")

def main():
    source_file = os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, DATA_TYPES_FILE_NAME)
    destination_file = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_TYPES_DATA_FOLDER_NAME, SQL_DATA_TYPES_FILE_NAME)

    with open(source_file, "rt") as source:
        with open(destination_file, "wt") as sql_stripped_data_file:
            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(sql_stripped_data_file, DATA_TYPES_TABLE_NAME, DataType.get_table_parameters())

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(sql_stripped_data_file)

            first_line_was_written = False

            while True:
                first_line = source.readline()

                if not first_line:
                    break

                abbreviation = process_abbreviation_line(first_line)

                for _ in range(7):
                    next(source)    # file, ref, header, format, fttbl, cols, under

                long_description_line = source.readline()
                long_description = parse_value(long_description_line)

                short_description_line = source.readline()
                short_description = parse_value(short_description_line)

                data_type = DataType(abbreviation, short_description, long_description)

                if first_line_was_written:
                    # write insert command to file
                    sql_stripped_data_file.write(",\n")

                first_line_was_written = True

                write_sql_values_data_statement(sql_stripped_data_file, data_type)

                source.readline() # },

            # write end of insert command to file
            sql_stripped_data_file.write(";\n")

if __name__ == '__main__':
    main()