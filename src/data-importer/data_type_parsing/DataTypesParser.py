import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import DATA_TYPES_FILE_NAME, DATA_TYPES_DATA_FOLDER_NAME, SQL_DATA_TYPES_FILE_NAME, \
    DATA_TYPES_TABLE_NAME, ERROR_OUTPUT_FILE_NAME
from common.file_paths import DESTINATION_DATA_FOLDER_PATH
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

def process_single_file(
        original_data_types_file_path,
        sql_destination_data_types_file,
        error_output_file_path):

    # if dias.dat does not exist for the cluster create only cluster record
    if not os.path.exists(original_data_types_file_path):
        print("FILE NOT FOUND: ", original_data_types_file_path)
        return

    with open(original_data_types_file_path, "rt") as original_data_types_file:
        first_line_was_written = False

        while True:
            # check the format
            try:
                # extract each data type from file
                data_type = extract_data_type(original_data_types_file)
            except ValueError as e:
                # store info about not processed files due to format inconsistency
                with open(error_output_file_path, "at") as not_processed_files_file:
                    message = "\t".join(["File not processed", original_data_types_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)
                return

            if data_type is None:
                break

            if first_line_was_written:
                # write insert command to file
                sql_destination_data_types_file.write(",\n")

            first_line_was_written = True

            write_sql_values_data_statement(sql_destination_data_types_file, data_type)


def main():
    original_data_types_file_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_TYPES_FILE_NAME)
    destination_data_types_folder_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_TYPES_DATA_FOLDER_NAME)

    # create folder on path
    if not os.path.exists(destination_data_types_folder_path):
        os.mkdir(destination_data_types_folder_path)

    sql_destination_data_types_file_path = os.path.join(destination_data_types_folder_path, SQL_DATA_TYPES_FILE_NAME)
    error_output_file_path = os.path.join(destination_data_types_folder_path, ERROR_OUTPUT_FILE_NAME)

    with open(sql_destination_data_types_file_path, "wt") as sql_destination_data_types_file:
        # create and write INSERT part of SQL insert command
        write_sql_insert_statement(sql_destination_data_types_file, DATA_TYPES_TABLE_NAME, DataType.get_table_parameters())

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(sql_destination_data_types_file)

        process_single_file(
            original_data_types_file_path,
            sql_destination_data_types_file,
            error_output_file_path)

        # write end of insert command to file
        sql_destination_data_types_file.write(";\n")

if __name__ == '__main__':
    main()