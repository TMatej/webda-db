import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import NO_DATA_WERE_FOUND_SQL_COMMENT, ERROR_OUTPUT_FILE_NAME, BUFFER_SIZE, \
    ADOPTED_NUMBER_REFERENCES_FOLDER_NAME, SQL_ADOPTED_NUMBER_REFERENCES_FILE_NAME, \
    NUMBERING_SYSTEM_FILE_NAME, ADOPTED_NUMBER_REFERENCES_TABLE_NAME, DATA_DESTINATION_FOLDER_NAME, \
    REFERENCES_ORIGIN_FOLDER_NAME
from common.folder_paths import DESTINATION_FOLDER_PATH, ORIGIN_FOLDER_PATH
from AdoptedNumberReference import AdoptedNumberReference

def check_standard(numbering_system_origin_file):
    first_data_line = numbering_system_origin_file.readline().strip()

    if "OCl\tReference" not in first_data_line:
        raise ValueError("First data line does not contain 'OCl\tReference'.")

    second_data_line = numbering_system_origin_file.readline().strip()

    if "---\t---------" not in second_data_line:
        raise ValueError("Second data line does not contain '---\t---------'.")

    line_number = 2

    while True:
        line_number += 1

        # collect record properties
        first_record_line = numbering_system_origin_file.readline()

        if not first_record_line or first_record_line == "\n":
            break

        line_number = check_line_recursively(numbering_system_origin_file, first_record_line, line_number)

    numbering_system_origin_file.seek(0)

def check_line_recursively(numbering_system_origin_file, line: str, line_number: int) -> int:
    # collect record properties
    first_record_line = line

    if not first_record_line or first_record_line == "\n":
        return line_number

    first_parts = first_record_line.split("\t")
    if len(first_parts) > 2:
        raise ValueError(
            f"(first) The line number: '{line_number}' was split more than once: Line: '{first_record_line}'.")

    if len(first_parts) < 2:
        print(f"(first) Line number: '{line_number}' has no reference part.")
        return line_number

    line_number += 1
    second_record_line = numbering_system_origin_file.readline()
    second_parts = second_record_line.split("\t")
    if len(second_parts) > 2:
        raise ValueError(
            f"(second) The line number: '{line_number}' was split more than once: Line: '{second_record_line}'.")

    if len(second_parts) < 2:
        print(f"(second) Line number: '{line_number}' has no bibcode part.")
        return line_number

    if second_parts[0].strip() != first_parts[0].strip():
        print(f"First '{line_number - 1}' and second '{line_number}' lines does not match in first parameter.")
        return check_line_recursively(numbering_system_origin_file, second_record_line, line_number)

    return line_number

def process_numbering_system_data_file(
        numbering_system_origin_file_path: str,
        adopted_numbers_reference_destination_sql_file,
        error_output_destination_file_path: str,
        comma_shall_be_writen: [bool]):
    # if dias.dat does not exist for the cluster create only cluster record
    if not os.path.exists(numbering_system_origin_file_path):
        print(f"FILE NOT FOUND: '{numbering_system_origin_file_path}'.", )
        return

    with open(numbering_system_origin_file_path, 'rt') as numbering_system_origin_file:
        print(f"Processing file content: {numbering_system_origin_file_path}")

        # check the format
        try:
            check_standard(numbering_system_origin_file)
        except ValueError as e:
            # store info about not processed files due to format inconsistency
            with open(error_output_destination_file_path, "at") as not_processed_files_file:
                message = "\t".join(["File not processed", numbering_system_origin_file_path, e.__str__(), "\n"])
                not_processed_files_file.write(message)
            return

        # process record in the file

        while True:
            # collect record properties
            first_record_line = numbering_system_origin_file.readline()

            if not first_record_line or first_record_line == "\n":
                break

            first_parts = first_record_line.split("\t")

            if len(first_parts) < 2:
                continue

            second_record_line = numbering_system_origin_file.readline()
            second_parts = second_record_line.split("\t")

            if len(second_parts) < 2:
                continue

            if second_parts[0].strip() != first_parts[0].strip():
                continue

            adopted_number_reference = AdoptedNumberReference(
                first_parts[0],
                second_parts[1],
                first_parts[1])

            if comma_shall_be_writen[0]:
                # write insert command to file
                adopted_numbers_reference_destination_sql_file.write(",\n")

            comma_shall_be_writen[0] = True

            # create and write VALUES part of SQL insert command for CLUSTERS file
            write_sql_values_data_statement(adopted_numbers_reference_destination_sql_file, adopted_number_reference)


def main():
    # check folder path existence and create folder on path if it does not exist yet
    adopted_numbers_reference_destination_sql_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, ADOPTED_NUMBER_REFERENCES_FOLDER_NAME)
    if not os.path.exists(adopted_numbers_reference_destination_sql_folder_path):
        os.makedirs(adopted_numbers_reference_destination_sql_folder_path, exist_ok=True)

    # error destination file path
    error_output_destination_file_path: str = os.path.join(adopted_numbers_reference_destination_sql_folder_path,
                                                           ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    # origin data file path
    numbering_system_origin_file_path = os.path.join(ORIGIN_FOLDER_PATH, REFERENCES_ORIGIN_FOLDER_NAME, NUMBERING_SYSTEM_FILE_NAME)

    # destination sql file path
    adopted_numbers_reference_destination_sql_file_path: str = os.path.join(adopted_numbers_reference_destination_sql_folder_path, SQL_ADOPTED_NUMBER_REFERENCES_FILE_NAME)

    # CHECK
    if not os.path.exists(numbering_system_origin_file_path):
        with open(adopted_numbers_reference_destination_sql_file_path, "wt") as adopted_numbers_reference_destination_sql_file:
            adopted_numbers_reference_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    comma_shall_be_writen: [bool] = [False]

    with open(adopted_numbers_reference_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as adopted_numbers_reference_destination_sql_file:
        # create and write INSERT part of SQL insert command
        adopted_numbers_references_table_parameters = AdoptedNumberReference.get_table_parameters()
        write_sql_insert_statement(adopted_numbers_reference_destination_sql_file, ADOPTED_NUMBER_REFERENCES_TABLE_NAME,
                                   adopted_numbers_references_table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(adopted_numbers_reference_destination_sql_file)

        # process document
        process_numbering_system_data_file(
            numbering_system_origin_file_path,
            adopted_numbers_reference_destination_sql_file,
            error_output_destination_file_path,
            comma_shall_be_writen)

        # write finish of the insert command to file
        adopted_numbers_reference_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(adopted_numbers_reference_destination_sql_file_path, "wt") as adopted_numbers_reference_destination_sql_file:
            adopted_numbers_reference_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()