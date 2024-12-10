import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import DATA_TYPES_FILE_NAME, DATA_TYPES_DATA_FOLDER_NAME, SQL_DATA_TYPES_FILE_NAME, \
    DATA_TYPES_TABLE_NAME, ERROR_OUTPUT_FILE_NAME, NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE, \
    DATA_DESTINATION_FOLDER_NAME
from common.folder_paths import DESTINATION_FOLDER_PATH
from data_types_parsing.DataType import DataType


def process_abbreviation_line(line: str) -> str:
    return line.split("=>", 1)[0].strip()

def parse_value(line: str) -> str:
    return line.split("=>", 1)[1].strip(" \",\n")

def extract_data_type(data_types_origin_file) -> DataType | None:
    first_line = data_types_origin_file.readline()

    if not first_line:
        return None

    while first_line == "\n":
        first_line = data_types_origin_file.readline()

        if not first_line:
            return None

    file_line = data_types_origin_file.readline()
    ref_line = data_types_origin_file.readline()
    header_line = data_types_origin_file.readline()
    format_line = data_types_origin_file.readline()
    fttbl_line = data_types_origin_file.readline()
    cols_line = data_types_origin_file.readline()
    under_line = data_types_origin_file.readline()
    description_line = data_types_origin_file.readline()
    name_line = data_types_origin_file.readline() # title

    abbreviation = process_abbreviation_line(first_line)
    data_file = parse_value(file_line)
    ref_file = parse_value(ref_line)
    header = parse_value(header_line)
    column_format = parse_value(format_line)
    fttbl = parse_value(fttbl_line)
    cols = parse_value(cols_line)
    under = parse_value(under_line)
    long_description = parse_value(description_line)
    short_description = parse_value(name_line)

    line = data_types_origin_file.readline().strip()

    if line != "},":
        raise ValueError(f'Incorrect format. Expected: "}}," - received: "{line}"')

    return DataType(abbreviation, data_file, ref_file, header, column_format, fttbl, cols, under, long_description,
                         short_description)

def process_data_types_file(
        data_types_origin_file_path: str,
        data_types_destination_sql_file,
        error_output_destination_file_path: str,
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(data_types_origin_file_path):
        print(f"FILE NOT FOUND: '{data_types_origin_file_path}'.")
        return

    with open(data_types_origin_file_path, "rt") as data_types_origin_file:
        print(f"Processing file content: {data_types_origin_file_path}")

        while True:
            # check the format
            try:
                # extract each data type from file
                data_type = extract_data_type(data_types_origin_file)
            except ValueError as e:
                # store info about not processed files due to format inconsistency
                with open(error_output_destination_file_path, "at") as not_processed_files_file:
                    message = "\t".join(["File not processed", data_types_origin_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)
                return

            if data_type is None:
                break

            if comma_shall_be_writen[0]:
                # write insert command to file
                data_types_destination_sql_file.write(",\n")

            comma_shall_be_writen[0] = True

            write_sql_values_data_statement(data_types_destination_sql_file, data_type)


def main():
    # check folder path existence and create folder on path if it does not exist yet
    destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME)
    data_types_destination_folder_path: str = os.path.join(destination_folder_path, DATA_TYPES_DATA_FOLDER_NAME)
    if not os.path.exists(data_types_destination_folder_path):
        os.makedirs(data_types_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(data_types_destination_folder_path, ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    # origin file path
    data_types_origin_file_path: str = os.path.join(destination_folder_path, DATA_TYPES_FILE_NAME)

    # destination sql file path
    data_types_destination_sql_file_path: str = os.path.join(data_types_destination_folder_path, SQL_DATA_TYPES_FILE_NAME)

    comma_shall_be_writen: [bool] = [False]

    with open(data_types_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as data_types_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = DataType.get_table_parameters()
        write_sql_insert_statement(data_types_destination_sql_file, DATA_TYPES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(data_types_destination_sql_file)

        process_data_types_file(
            data_types_origin_file_path,
            data_types_destination_sql_file,
            error_output_destination_file_path,
            comma_shall_be_writen)

        # write end of insert command to file
        data_types_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(data_types_destination_sql_file_path, "wt") as data_types_destination_sql_file:
            data_types_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()