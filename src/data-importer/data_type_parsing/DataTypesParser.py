import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import DATA_TYPES_FILE_NAME, DATA_TYPES_DATA_FOLDER_NAME, SQL_DATA_TYPES_FILE_NAME, DATA_TYPES_TABLE_NAME
from common.file_paths import ORIGIN_PUBLICATIONS_FOLDER_PATH, DESTINATION_DATA_FOLDER_PATH
from data_type_parsing.DataType import DataType


def process_abbreviation_line(line: str) -> str:
    return line.split("=>", 1)[0].strip()

def parse_value(line: str) -> str:
    return line.split("=>", 1)[1].strip(" \",\n")

def extract_data_type(source_file) -> DataType | None:
    first_line = source_file.readline()

    if not first_line:
        return None

    while first_line == "\n":
        first_line = source_file.readline()

        if not first_line:
            return None

    file_line = source_file.readline()
    ref_line = source_file.readline()
    header_line = source_file.readline()
    format_line = source_file.readline()
    fttbl_line = source_file.readline()
    cols_line = source_file.readline()
    under_line = source_file.readline()
    long_description_line = source_file.readline()
    short_description_line = source_file.readline()

    abbreviation = process_abbreviation_line(first_line)
    data_file = parse_value(file_line)
    ref_file = parse_value(ref_line)
    header = parse_value(header_line)
    column_format = parse_value(format_line)
    fttbl = parse_value(fttbl_line)
    cols = parse_value(cols_line)
    under = parse_value(under_line)
    long_description = parse_value(long_description_line)
    short_description = parse_value(short_description_line)

    line = source_file.readline().strip()

    if line != "},":
        raise ValueError(f'Incorrect format. Expected: "}}," - received: "{line}"')

    return DataType(abbreviation, data_file, ref_file, header, column_format, fttbl, cols, under, long_description,
                         short_description)

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
                # extract each data type from file
                data_type = extract_data_type(source)

                if data_type is None:
                    break

                if first_line_was_written:
                    # write insert command to file
                    sql_stripped_data_file.write(",\n")

                first_line_was_written = True

                write_sql_values_data_statement(sql_stripped_data_file, data_type)

            # write end of insert command to file
            sql_stripped_data_file.write(";\n")

if __name__ == '__main__':
    main()