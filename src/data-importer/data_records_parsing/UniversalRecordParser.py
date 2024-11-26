import os

from common.constants import SQL_FILE_SUFFIX, DATA_RECORDS_DATA_FOLDER_NAME, ERROR_OUTPUT_FILE_NAME, \
    NO_DATA_WERE_FOUND_SQL_COMMENT, DATA_TYPES_PROCESSED_NAMES_FILE_NAME, BUFFER_SIZE, SQL_FILE_NAME_COLUMN_NAME, \
    SQL_FOLDER_NAME_COLUMN_NAME
from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, CLUSTERS_ORIGIN_FOLDER_PATH
from UniversalRecordParserBase import process_record, check_record_file_structure, \
    process_data_types
from data_types_parsing.DataType import DataType

def process_records_of_data_type(
        data_type_origin_file,
        data_type_destination_sql_file,
        data_type_origin_file_path: str,
        error_output_destination_file_path: str,
        data_type: DataType,
        folder_name: str,
        file_name: str,
        column_formats: [str],
        comma_shall_be_writen: [bool]):
    #  move to first record
    _ = data_type_origin_file.readline() # header
    _ = data_type_origin_file.readline() # header splitter
    line_number = 2  # first two lines were already read

    columns = data_type.cols.strip().split("\\t")

    # process each record in the file
    while True:
        line = data_type_origin_file.readline()
        line_number += 1

        if not line:
            break

        # skip empty line
        if line == "\n":
            continue

        # process record
        try:
            insert_line = process_record(folder_name, file_name, line, columns, column_formats)

            if comma_shall_be_writen[0]:
                # write insert command to file
                data_type_destination_sql_file.write(",\n")

            comma_shall_be_writen[0] = True

            # write VALUES part of SQL insert command - manual insert due to unknown column structure
            data_type_destination_sql_file.write("".join(["(", insert_line, ")"]))
        except ValueError as e:
            # store info about not processed lines due to format inconsistency
            with open(error_output_destination_file_path, "at") as error_output_destination_file:
                message = "\t".join(
                    ["Line not processed", line_number.__str__(), data_type_origin_file_path, e.__str__(), "\n"])
                error_output_destination_file.write(message)

def process_data_file(
        data_type_origin_file_path: str,
        data_type_destination_sql_file,
        error_output_destination_file_path: str,
        data_type: DataType,
        cluster_folder_name: str,
        data_type_file_name: str,
        column_formats: [str],
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(data_type_origin_file_path):
        # print(f"FILE NOT FOUND: '{data_type_origin_file_path}'.") # commented out due to effectivity of processing
        return

    # check that data type values are valid columns and columns formats

    with open(data_type_origin_file_path, "rt") as data_type_origin_file:
        print(f"Processing content of file: {data_type_origin_file_path}")

        # check the format
        try:
            check_record_file_structure(data_type_origin_file, data_type)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_destination_file_path, "at") as error_output_destination_file:
                message = "\t".join(["File not processed", data_type_origin_file_path, e.__str__(), "\n"])
                error_output_destination_file.write(message)
            return

        # process the records
        process_records_of_data_type(
            data_type_origin_file,
            data_type_destination_sql_file,
            data_type_origin_file_path,
            error_output_destination_file_path,
            data_type,
            cluster_folder_name,
            data_type_file_name,
            column_formats,
            comma_shall_be_writen)


def main():
    # check folder path existence and create folder on path if it does not exist yet
    data_records_destination_folder_path: str = os.path.join(DESTINATION_DATA_FOLDER_PATH, DATA_RECORDS_DATA_FOLDER_NAME)
    if not os.path.exists(data_records_destination_folder_path):
        os.makedirs(data_records_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(data_records_destination_folder_path, ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    clusters_origin_folder_content = os.listdir(CLUSTERS_ORIGIN_FOLDER_PATH)
    cluster_folder_names = [f for f in clusters_origin_folder_content if os.path.isdir(os.path.join(CLUSTERS_ORIGIN_FOLDER_PATH, f))]

    # process data types
    data_types = process_data_types()
    data_types.sort(key=lambda dt: dt.file_name)

    cluster_folder_names.sort()

    processed_data_files_path = os.path.join(data_records_destination_folder_path, DATA_TYPES_PROCESSED_NAMES_FILE_NAME)

    processed_data_types_names = []
    if os.path.exists(processed_data_files_path):
        with open(processed_data_files_path, "rt") as processed_data_types_file:
            processed_data_types_names = list(map(str.strip, processed_data_types_file.readlines()))

    for data_type in data_types:
        # DATA TYPE METADATA PROCESSING
        data_type_file_name: str = data_type.file_name.strip().lower()

        new_data_type_file_name: str = data_type_file_name.replace(".", "-")

        if data_type_file_name in processed_data_types_names:
            continue

        # destination sql file path
        data_record_destination_sql_file_path: str = os.path.join(data_records_destination_folder_path, new_data_type_file_name + SQL_FILE_SUFFIX)

        # CHECK
        if len(cluster_folder_names) == 0:
            with open(data_record_destination_sql_file_path, "wt") as data_record_destination_sql_file:
                data_record_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
            return

        file_name, file_extension =  os.path.splitext(data_type.file_name)
        data_type_table_name = file_name.lower() + file_extension.lower().replace(".", "_")

        columns: [str] = data_type.cols.strip().split("\\t")
        columns_formats: [str] = data_type.format.strip().split()

        # check that number of columns matches the number of column formats
        if len(columns) != len(columns_formats):
            with open(error_output_destination_file_path, "at") as error_output_destination_file:
                message = "\t".join([f"Data type: '{data_type.file_name}' not processed", f"Number of columns: '{len(columns)}' differs from number of columns formats: '{len(columns_formats)}'.", "\n"])
                error_output_destination_file.write(message)
            continue

        processed_columns: list = list(map(lambda x: x.strip().lower(), columns))
        processed_columns.insert(0, SQL_FILE_NAME_COLUMN_NAME)       # add origin filename to db columns
        processed_columns.insert(0, SQL_FOLDER_NAME_COLUMN_NAME)     # add origin foldername to db columns

        data_type_table_parameters: str = ", ".join(processed_columns)

        with open(data_record_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as data_record_destination_sql_file:
            # create and write INSERT part of SQL insert command
            write_sql_insert_statement(data_record_destination_sql_file, data_type_table_name, data_type_table_parameters)

            # create and write VALUES keyword of SQL insert command
            write_sql_values_keyword_statement(data_record_destination_sql_file)

            comma_shall_be_writen: [bool] = [False]

            # process the data records of type for each cluster
            for cluster_folder_name in cluster_folder_names:
                # source file path
                data_type_origin_file_path = os.path.join(CLUSTERS_ORIGIN_FOLDER_PATH, cluster_folder_name,
                                                            data_type_file_name)

                # process the data file
                process_data_file(
                    data_type_origin_file_path,
                    data_record_destination_sql_file,
                    error_output_destination_file_path,
                    data_type,
                    cluster_folder_name,
                    data_type_file_name,
                    columns_formats,
                    comma_shall_be_writen)

            if comma_shall_be_writen[0]:
                # write end of INSERT command to file
                data_record_destination_sql_file.write(";\n")

        if not comma_shall_be_writen[0]:
            # remove file contents as no data were found
            with open(data_record_destination_sql_file_path, "wt") as data_record_destination_sql_file:
                data_record_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)

        processed_data_types_names.append(data_type_file_name)
        with open(processed_data_files_path, "at") as processed_data_types_file:
            processed_data_types_file.write(data_type_file_name + "\n")

if __name__ == '__main__':
    main()