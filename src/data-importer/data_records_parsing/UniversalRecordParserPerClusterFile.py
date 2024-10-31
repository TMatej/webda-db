import os

from common.constants import CLUSTERS_DATA_FOLDER_NAME, SQL_FILE_SUFFIX, DATA_TYPES_FILE_NAME
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH, \
    ORIGIN_PUBLICATIONS_FOLDER_PATH, PROCESSED_DATA_FILES_PATH, NOT_PROCESSED_FILES_PATH
from data_type_parsing.DataType import DataType
from data_type_parsing.DataTypesParser import extract_data_type

# TODO - checking need refactor as the columns sometimes hold more information (more columns) than stated in the format
def check_record_file_structure(open_file, data_type: DataType):
    first_line = open_file.readline().strip()

    stripped_first_line = first_line.lower()
    cols = data_type.cols.replace("\\t", "\t").lower()

    first_line_tuples = stripped_first_line.split("\t")
    cols_tuples = cols.split("\t")

    if len(first_line_tuples) > len(cols_tuples):
        sanitized_cols = data_type.cols.replace("\t", "\\t")
        sanitized_stripped_first_line = first_line.replace("\t", "\\t")
        print(f'Header columns does not match the number of predefined columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{len(cols_tuples)}" - "{len(first_line_tuples)}".')
        raise ValueError(f'Header columns does not match the number of predefined columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{len(cols_tuples)}" - "{len(first_line_tuples)}".')


    for i in range(len(first_line_tuples)):
            first_line_part = first_line_tuples[i].strip()
            cols_part = cols_tuples[i].strip()

            if first_line_part != cols_part:
                sanitized_cols = data_type.cols.replace("\t", "\\t")
                sanitized_stripped_first_line = first_line.replace("\t", "\\t")
                print(f'Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{cols_part}" - "{first_line_part}".')
                raise ValueError(f'Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}". "{cols_part}" - "{first_line_part}".')

    # if stripped_first_line != cols:
    #     sanitized_cols = cols.replace("\t", "\\t")
    #     sanitized_stripped_first_line = stripped_first_line.replace("\t", "\\t")
    #     print(f'First line does not match the columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}".')
    #     raise ValueError(f'First line does not match the columns. Expected: "{sanitized_cols}" - received: "{sanitized_stripped_first_line}".')

    # this can be optional
    # second_line = open_file.readline().lower()
    # stripped_second_line = second_line.rstrip().lower()
    # under = data_type.under.replace("\\t", "\t")
    # if stripped_second_line != under:
    #     sanitized_under = under.replace("\t", "\\t")
    #     sanitized_stripped_second_line = stripped_second_line.replace("\t", "\\t")
    #     print(f'Second line does not match the format. Expected: "{sanitized_under}" - received: "{sanitized_stripped_second_line}".')
    #     raise ValueError(f'Second line does not match the format. Expected: "{sanitized_under}" - received: "{sanitized_stripped_second_line}".')

    # reset pointer to beginning
    open_file.seek(0)

def process_record(folder_name, line: str, column_formats: [str]) -> str | None:
    line_tuple = line.strip("\n").split("\t")

    if len(line_tuple) > len(column_formats):
        column_formats_sanitized_string = "\\t".join(column_formats)
        line_tuple_sanitized_string = "\\t".join(line_tuple)
        print(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "{len(column_formats)}" - "{len(line_tuple)}".')
        raise ValueError(f'Number of values does not match the number of predefined columns. Expected: "{column_formats_sanitized_string}" - received: "{line_tuple_sanitized_string}". "{len(column_formats)}" - "{len(line_tuple)}".')

    line = f"'{folder_name}'"

    for i in range(len(column_formats)):
        line = "".join([line, ", "])

        if i >= len(line_tuple):
            line = "".join([line, "NULL"])
            continue

        column_format = column_formats[i]
        column_value = line_tuple[i].strip()

        # empty value -> NULL
        if len(column_value) == 0 :
            line = "".join([line, "NULL"])
            continue

        # number or string value
        if "d" in column_format or "f" in column_format:
            line = "".join([line, column_value])
        else:
            line = "".join([line, "'", column_value, "'"])

    return line

def process_data_types() -> [DataType]:
    source_file = os.path.join(ORIGIN_PUBLICATIONS_FOLDER_PATH, DATA_TYPES_FILE_NAME)

    data_types: [DataType] = []

    with open(source_file, "rt") as source:
        while True:
            data_type = extract_data_type(source)

            if data_type is None:
                break

            data_types.append(data_type)

    return data_types


def process_records_of_data_type(
        data_type_file,
        data_type_sql_file,
        data_type_file_path,
        data_type: DataType,
        folder_name: str,
        column_formats: [str]):
    #  move to first record
    _ = data_type_file.readline() # header
    _ = data_type_file.readline() # header splitter
    line_number = 2  # first two lines were already read

    first_line_was_written = False

    # process each record in the file
    while True:
        line = data_type_file.readline()
        line_number += 1

        if not line:
            break

        # skip empty line
        if line == "\n":
            continue

        if first_line_was_written:
            # write insert command to file
            data_type_sql_file.write(",\n")

        first_line_was_written = True

        # process record
        try:
            insert_line = process_record(folder_name, line, column_formats)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(NOT_PROCESSED_FILES_PATH, "at") as not_processed_files_file:
                message = "\t".join(
                    [data_type.file_name, data_type_file_path, line_number.__str__(), e.__str__(), "\n"])
                not_processed_files_file.write(message)
            # empty already processed lines
            data_type_sql_file.truncate(0)
            return
        # write VALUES part of SQL insert command
        data_type_sql_file.write("".join(["(", insert_line, ")"]))

    # write end of insert command to file
    data_type_sql_file.write(";\n")

def process_data_file(
        data_type: DataType,
        folder_name: str,
        cluster_sql_folder_path: str,
        file_name: str,
        data_type_table_name: str,
        data_type_table_parameters: str,
        column_formats: [str]):
    # source file path
    data_type_file_path = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, folder_name, file_name)

    # destination sql file path
    data_type_sql_file_path = os.path.join(cluster_sql_folder_path, file_name + SQL_FILE_SUFFIX)

    # if data type file does not exist for the cluster skip to next
    if not os.path.exists(data_type_file_path):
        return

    with open(data_type_file_path, "rt") as data_type_file:
        # check the format
        try:
            check_record_file_structure(data_type_file, data_type)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(NOT_PROCESSED_FILES_PATH, "at") as not_processed_files_file:
                message = "\t".join([data_type.file_name, data_type_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        with open(data_type_sql_file_path, "wt") as data_type_sql_file:
            print(f"Printing file content: {data_type_file_path}")

            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(data_type_sql_file, data_type_table_name, data_type_table_parameters)

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(data_type_sql_file)

            # process the records
            process_records_of_data_type(
                data_type_file,
                data_type_sql_file,
                data_type_file_path,
                data_type,
                folder_name,
                column_formats)

def main():
    destination_folder = os.path.join(DESTINATION_DATA_FOLDER_PATH, CLUSTERS_DATA_FOLDER_NAME)
    folder_content = os.listdir(ORIGIN_CLUSTERS_FOLDER_PATH)

    folder_names = [f for f in folder_content if os.path.isdir(os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, f))]
    folder_names.sort()

    # process data types
    data_types = process_data_types()

    # make processed data file map

    processed_data_types_names = []
    with open(PROCESSED_DATA_FILES_PATH, "rt") as processed_data_types_file:
        processed_data_types_names = list(map(str.strip, processed_data_types_file.readlines()))

    for data_type in data_types:
        # DATA TYPE METADATA PROCESSING
        file_name = data_type.file_name.strip().lower()

        if file_name in processed_data_types_names:
            continue

        # compute the values for each data type once
        data_type_table_name = data_type.file_name.strip().lower().replace(".", "_").capitalize() # TODO - make mapper for types
        columns: [str] = data_type.cols.strip().split("\\t")
        columns.insert(0, "foldername")
        data_type_table_parameters: [str] = ", ".join(map(lambda x: x.strip().lower().capitalize(), columns))
        columns_formats = data_type.format.strip().split()

        # process the data type for each file
        for folder_name in folder_names:
            # create folder on path if it does not exist yet
            cluster_sql_folder_path = os.path.join(destination_folder, folder_name)
            if not os.path.exists(cluster_sql_folder_path):
                os.makedirs(cluster_sql_folder_path, exist_ok=True)

            # process the data file
            process_data_file(
                data_type,
                folder_name,
                cluster_sql_folder_path,
                file_name,
                data_type_table_name,
                data_type_table_parameters,
                columns_formats)

        processed_data_types_names.append(file_name)
        with open(PROCESSED_DATA_FILES_PATH, "at") as processed_data_types_file:
            processed_data_types_file.write(file_name + "\n")

if __name__ == '__main__':
    main()