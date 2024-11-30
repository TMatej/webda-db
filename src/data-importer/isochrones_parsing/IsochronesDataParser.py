import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, \
    write_sql_values_data_statement
from common.constants import  ERROR_OUTPUT_FILE_NAME, NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE, \
    DATA_DESTINATION_FOLDER_NAME, DATA_ISOCHRONES_FOLDER_NAME, \
    ISOCHRONES_ORIGIN_FOLDER_NAME, GENEVA_FOLDER_NAME, PADOVA_FOLDER_NAME, SQL_ISOCHRONES_FILE_NAME, \
    ISOCHRONES_TABLE_NAME
from common.file_paths import DESTINATION_FOLDER_PATH, ORIGIN_FOLDER_PATH
from isochrones_parsing.Isochrone import Isochrone


def process_isochrone_line(
        line: str,
        file_name: str,
        isochrone_type: str) -> Isochrone | None:
    if not line:
        return None

    parts: [str] = line.split("\t")

    logt = parts[0].strip()
    mv = parts[1].strip()
    mbv = parts[2].strip()
    mub = parts[3].strip()
    mvr = parts[4].strip()
    mvi = parts[5].strip()
    mri = parts[6].strip()

    return Isochrone(file_name, isochrone_type, logt, mv, mbv, mub, mvr, mvi, mri)


def check_standard(
        isochrones_origin_file_path:str,
        error_output_destination_file_path: str) -> bool:
    with open(isochrones_origin_file_path, "rt") as isochrones_origin_file:
        with open(error_output_destination_file_path, "at") as not_processed_files_file:
            first_data_line = isochrones_origin_file.readline().strip()
            if "logt\tMv\tMbv\tMub\tMvr\tMvi\tMri" not in first_data_line:
                message = "First data line does not contain 'logt\tMv\tMbv\tMub\tMvr\tMv\tMri'.\n"
                not_processed_files_file.write(message)
                return False

            second_data_line = isochrones_origin_file.readline().strip()
            if "----\t--\t---\t---\t---\t---\t---" not in second_data_line:
                message = "Second data line does not contain '----\t--\t---\t---\t---\t---\t---'.\n"
                not_processed_files_file.write(message)
                return False

            # check individual lines
            line_number = 2
            result = True
            while True:
                data_line = isochrones_origin_file.readline()

                if not data_line:
                    break

                line_number += 1
                parts = data_line.split("\t")

                if len(parts) != 7:
                    message = f"Line {line_number} is not correct. '{data_line}'."
                    not_processed_files_file.write(message)
                    result = False
    return result


def process_isochrones_file(
        isochrones_origin_file_path: str,
        isochrones_destination_sql_file,
        error_output_destination_file_path: str,
        isochrones_file_name: str,
        isochrone_type: str,
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(isochrones_origin_file_path):
        print(f"FILE NOT FOUND: '{isochrones_origin_file_path}'.")
        return

    # check the format
    result = check_standard(isochrones_origin_file_path, error_output_destination_file_path)
    if not result:
        return

    with open(isochrones_origin_file_path, "rt") as isochrones_origin_file:
        print(f"Processing file content: {isochrones_origin_file_path}")

        while True:
            _ = isochrones_origin_file.readline()  # first line
            _ = isochrones_origin_file.readline()  # second line

            # process record
            isochrone_line = isochrones_origin_file.readline()
            isochrone_record = process_isochrone_line(isochrone_line, isochrones_file_name, isochrone_type)

            if not isochrone_record:
                break

            if comma_shall_be_writen[0]:
                # write insert command to file
                isochrones_destination_sql_file.write(",\n")

            comma_shall_be_writen[0] = True

            # create and write VALUES part of SQL insert command for CLUSTERS file
            write_sql_values_data_statement(isochrones_destination_sql_file, isochrone_record)


def main():
    # check folder path existence and create folder on path if it does not exist yet
    isochrones_destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, DATA_ISOCHRONES_FOLDER_NAME)
    if not os.path.exists(isochrones_destination_folder_path):
        os.makedirs(isochrones_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(isochrones_destination_folder_path, ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    # destination sql file path
    isochrones_destination_sql_file_path: str = os.path.join(isochrones_destination_folder_path, SQL_ISOCHRONES_FILE_NAME)

    comma_shall_be_writen: [bool] = [False]

    with open(isochrones_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as isochrones_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = Isochrone.get_table_parameters()
        write_sql_insert_statement(isochrones_destination_sql_file, ISOCHRONES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(isochrones_destination_sql_file)

        geneva_isochrones_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, ISOCHRONES_ORIGIN_FOLDER_NAME, GENEVA_FOLDER_NAME)
        geneva_isochrones_origin_folder_content = os.listdir(geneva_isochrones_origin_folder_path)
        geneva_isochrones_file_names = [f for f in geneva_isochrones_origin_folder_content if
                                os.path.isfile(os.path.join(geneva_isochrones_origin_folder_path, f))]

        for geneva_file_name in geneva_isochrones_file_names:

            # origin file path
            geneva_isochrones_origin_file_path: str = os.path.join(geneva_isochrones_origin_folder_path, geneva_file_name)

            process_isochrones_file(
                geneva_isochrones_origin_file_path,
                isochrones_destination_sql_file,
                error_output_destination_file_path,
                geneva_file_name,
                GENEVA_FOLDER_NAME,
                comma_shall_be_writen)

        padova_isochrones_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, ISOCHRONES_ORIGIN_FOLDER_NAME, PADOVA_FOLDER_NAME)
        padova_isochrones_origin_folder_content = os.listdir(padova_isochrones_origin_folder_path)
        padova_isochrones_file_names = [f for f in padova_isochrones_origin_folder_content if
                                        os.path.isfile(os.path.join(padova_isochrones_origin_folder_path, f))]
        for padova_file_name in padova_isochrones_file_names:
            # origin file path
            padova_isochrones_origin_file_path: str = os.path.join(padova_isochrones_origin_folder_path, padova_file_name)

            process_isochrones_file(
                padova_isochrones_origin_file_path,
                isochrones_destination_sql_file,
                error_output_destination_file_path,
                padova_file_name,
                PADOVA_FOLDER_NAME,
                comma_shall_be_writen)

        # write end of insert command to file
        isochrones_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(isochrones_destination_sql_file_path, "wt") as isochrones_destination_sql_file:
            isochrones_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()