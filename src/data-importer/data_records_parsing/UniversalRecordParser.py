import os

from common.constants import SQL_FILE_SUFFIX, DATA_RECORDS_DATA_FOLDER_NAME, ERROR_OUTPUT_FILE_NAME
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_CLUSTERS_FOLDER_PATH, \
     PROCESSED_DATA_FILES_PATH
from data_records_parsing.UniversalRecordParserBase import process_record, check_record_file_structure, \
    process_data_types
from data_type_parsing.DataType import DataType

BUFFER_SIZE = 104857600 # 100MB

def process_records_of_data_type(
        data_type_file,
        data_type_sql_file,
        data_type_file_path: str,
        error_output_file_path: str,
        folder_name: str,
        file_name: str,
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
            insert_line = process_record(folder_name, file_name, line, column_formats)

            # write VALUES part of SQL insert command - manual insert due to unknown column structure
            data_type_sql_file.write("".join(["(", insert_line, ")"]))
        except ValueError as e:
            # store info about not processed lines due to format inconsistency
            with open(error_output_file_path, "at") as not_processed_files_file:
                message = "\t".join(
                    ["Line not processed", line_number.__str__(), data_type_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)

def process_data_file(
        original_data_type_file_path: str,
        sql_destination_data_type_file,
        error_output_file_path: str,
        data_type: DataType,
        cluster_folder_name: str,
        data_type_file_name: str,
        column_formats: [str],
        first_line_was_written: [bool]):
    # if data type file does not exist for the cluster skip to next
    if not os.path.exists(original_data_type_file_path):
        return

    with open(original_data_type_file_path, "rt") as original_data_type_file:
        print(f"Processing content of file: {original_data_type_file_path}")

        # check the format
        try:
            check_record_file_structure(original_data_type_file, data_type)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", original_data_type_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        # process the records
        process_records_of_data_type(
            original_data_type_file,
            sql_destination_data_type_file,
            original_data_type_file_path,
            error_output_file_path,
            cluster_folder_name,
            data_type_file_name,
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
    destination_data_records_folder_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_RECORDS_DATA_FOLDER_NAME)

    if not os.path.exists(destination_data_records_folder_path):
        os.makedirs(destination_data_records_folder_path, exist_ok=True)

    # error destination file
    error_output_file_path = os.path.join(destination_data_records_folder_path, ERROR_OUTPUT_FILE_NAME)

    for data_type in data_types:
        # DATA TYPE METADATA PROCESSING
        data_type_file_name: str = data_type.file_name.strip().lower()

        if data_type_file_name in processed_data_types_names:
            continue



        data_type_table_name = data_type.file_name.strip().lower().replace(".", "_").capitalize() # TODO - make mapper for types



        columns: [str] = data_type.cols.strip().split("\\t")
        columns.insert(0, "filename")
        columns.insert(0, "foldername")
        # insert ref_file to columns
        data_type_table_parameters: [str] = ", ".join(map(lambda x: x.strip().lower().capitalize(), columns))
        columns_formats = data_type.format.strip().split()

        # destination sql file path
        data_record_sql_file_path = os.path.join(destination_data_records_folder_path, data_type_file_name + SQL_FILE_SUFFIX)

        with open(data_record_sql_file_path, "wt", buffering=BUFFER_SIZE) as data_record_sql_file:
            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(data_record_sql_file, data_type_table_name, data_type_table_parameters)

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(data_record_sql_file)

            first_line_was_written: [bool] = [False]

            # process the data records of type for each cluster
            for cluster_folder_name in cluster_folder_names:
                # source file path
                original_data_type_file_path = os.path.join(ORIGIN_CLUSTERS_FOLDER_PATH, cluster_folder_name,
                                                            data_type_file_name)

                # process the data file
                process_data_file(
                    original_data_type_file_path,
                    data_record_sql_file,
                    error_output_file_path,
                    data_type,
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