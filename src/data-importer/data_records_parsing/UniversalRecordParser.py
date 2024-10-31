import os

from common.constants import SQL_FILE_SUFFIX, DATA_TYPES_FILE_NAME, DATA_RECORDS_DATA_FOLDER_NAME
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH, \
    ORIGIN_PUBLICATIONS_FOLDER_PATH, PROCESSED_DATA_FILES_PATH, NOT_PROCESSED_FILES_PATH
from data_type_parsing.DataType import DataType
from data_type_parsing.DataTypesParser import extract_data_type

BUFFER_SIZE = 104857600 # 100MB

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
        column_value = line_tuple[i].strip().replace("'", "''")

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
        data_type_file_path: str,
        folder_name: str,
        column_formats: [str],
        first_line_was_written: [bool]):
    #  move to first record
    _ = data_type_file.readline() # header
    _ = data_type_file.readline() # header splitter
    line_number = 2  # first two lines were already read

    # process each record in the file
    while True:
        line = data_type_file.readline()
        line_number += 1

        if not line:
            break

        # skip empty line
        if line == "\n":
            continue

        if first_line_was_written[0]:
            # write insert command to file
            data_type_sql_file.write(",\n")

        first_line_was_written[0] = True

        # process record
        try:
            insert_line = process_record(folder_name, line, column_formats)

            # write VALUES part of SQL insert command
            data_type_sql_file.write("".join(["(", insert_line, ")"]))
        except ValueError as e:
            # store info about not processed lines due to format inconsistency
            with open(NOT_PROCESSED_FILES_PATH, "at") as not_processed_files_file:
                message = "\t".join(
                    ["Line not processed", line_number.__str__(), data_type_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)

def process_data_file(
        data_type: DataType,
        data_type_sql_file,
        cluster_folder_name: str,
        data_type_file_name: str,
        column_formats: [str],
        first_line_was_written: [bool]):
    # source file path
    data_type_file_path = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, cluster_folder_name, data_type_file_name)

    # if data type file does not exist for the cluster skip to next
    if not os.path.exists(data_type_file_path):
        return

    with open(data_type_file_path, "rt") as data_type_file:
        print(f"Processing content of file: {data_type_file_path}")

        # check the format
        try:
            check_record_file_structure(data_type_file, data_type)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(NOT_PROCESSED_FILES_PATH, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", data_type_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        # process the records
        process_records_of_data_type(
            data_type_file,
            data_type_sql_file,
            data_type_file_path,
            cluster_folder_name,
            column_formats,
            first_line_was_written)


def main():
    folder_content = os.listdir(ORIGIN_CLUSTERS_FOLDER_PATH)
    cluster_folder_names = [f for f in folder_content if os.path.isdir(os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, f))]
    cluster_folder_names.sort()

    # process data types
    data_types = process_data_types()
    data_types.sort(key=lambda dt: dt.file_name)

    processed_data_types_names = []
    with open(PROCESSED_DATA_FILES_PATH, "rt") as processed_data_types_file:
        processed_data_types_names = list(map(str.strip, processed_data_types_file.readlines()))

    # create folder on path if it does not exist yet
    cluster_sql_folder_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_RECORDS_DATA_FOLDER_NAME)
    if not os.path.exists(cluster_sql_folder_path):
        os.makedirs(cluster_sql_folder_path, exist_ok=True)

    for data_type in data_types:
        # DATA TYPE METADATA PROCESSING
        data_type_file_name = data_type.file_name.strip().lower()

        if data_type_file_name in processed_data_types_names:
            continue

        data_type_table_name = data_type.file_name.strip().lower().replace(".", "_").capitalize() # TODO - make mapper for types

        columns: [str] = data_type.cols.strip().split("\\t")
        columns.insert(0, "foldername")
        data_type_table_parameters: [str] = ", ".join(map(lambda x: x.strip().lower().capitalize(), columns))

        columns_formats = data_type.format.strip().split()

        # destination sql file path
        data_record_sql_file_path = os.path.join(cluster_sql_folder_path, data_type_file_name + SQL_FILE_SUFFIX)

        with open(data_record_sql_file_path, "wt", buffering=BUFFER_SIZE) as data_record_sql_file:
            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(data_record_sql_file, data_type_table_name, data_type_table_parameters)

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(data_record_sql_file)

            first_line_was_written = [False]

            # process the data records of type for each cluster
            for cluster_folder_name in cluster_folder_names:
                # process the data file
                process_data_file(
                    data_type,
                    data_record_sql_file,
                    cluster_folder_name,
                    data_type_file_name,
                    columns_formats,
                    first_line_was_written)

            if first_line_was_written[0]:
                # write end of INSERT command to file
                data_record_sql_file.write(";\n")

        if not first_line_was_written[0]:
            # remove file contents as no data were found
            with open(data_record_sql_file_path, "wt", buffering=BUFFER_SIZE) as data_record_sql_file:
                data_record_sql_file.write("-- NO DATA WERE FOUND")

        processed_data_types_names.append(data_type_file_name)
        with open(PROCESSED_DATA_FILES_PATH, "at") as processed_data_types_file:
            processed_data_types_file.write(data_type_file_name + "\n")

if __name__ == '__main__':
    main()