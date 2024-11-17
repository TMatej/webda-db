import os

from common.constants import CLUSTERS_DATA_FOLDER_NAME, SQL_FILE_SUFFIX
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH, \
    PROCESSED_DATA_FILES_PATH, NOT_PROCESSED_FILES_PATH
from data_records_parsing.UniversalRecordParserBase import process_record, check_record_file_structure, \
    process_data_types
from data_type_parsing.DataType import DataType


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

    processed_data_types_names = []
    with open(PROCESSED_DATA_FILES_PATH, "rt") as processed_data_types_file:
        processed_data_types_names = list(map(str.strip, processed_data_types_file.readlines()))

    for data_type in data_types:
        # DATA TYPE METADATA PROCESSING
        data_type_file_name = data_type.ref_file_name.strip().lower()

        if data_type_file_name in processed_data_types_names:
            continue

        data_type_table_name = data_type.ref_file_name.strip().lower().replace(".", "_").capitalize() # TODO - make mapper for types
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
                data_type_file_name,
                data_type_table_name,
                data_type_table_parameters,
                columns_formats)

        processed_data_types_names.append(data_type_file_name)
        with open(PROCESSED_DATA_FILES_PATH, "at") as processed_data_types_file:
            processed_data_types_file.write(data_type_file_name + "\n")

if __name__ == '__main__':
    main()