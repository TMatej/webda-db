import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import PUBLICATION_REFERENCES_TABLE_NAME, PUBLICATION_FILE_SUFFIX, \
    PUBLICATION_REFERENCES_DATA_FOLDER_NAME, SQL_PUBLICATION_REFERENCES_FILE_NAME, ERROR_OUTPUT_FILE_NAME, \
    NO_DATA_WERE_FOUND_SQL_COMMENT, BUFFER_SIZE, NUMBERING_SYSTEM_FILE_NAME, DATA_DESTINATION_FOLDER_NAME, \
    REFERENCES_ORIGIN_FOLDER_NAME
from common.file_paths import DESTINATION_FOLDER_PATH, ORIGIN_FOLDER_PATH
from PublicationReference import PublicationReference

def move_to_first_record(publication_references_origin_file) -> int:
    line_number = 1

    data_line = publication_references_origin_file.readline().strip()
    line_number += 1

    # without header
    if data_line == "":
        return line_number

    # with header
    if data_line.startswith("Dummy"):
        _ = publication_references_origin_file.readline()
        line_number += 1
        return line_number

    # unknown format -> raise error
    raise ValueError("Unknown file format.")


def parse_key(line: str) -> str:
    return line.split("\t", 1)[0].strip(" \",\n")


def parse_value(line: str) -> str:
    return line.split("\t", 1)[1].strip(" \",\n")


def check_key(line: str, expected_key: str, line_number: int):
    key = parse_key(line)
    if key != expected_key:
        raise ValueError(f'Line: {line_number} - Incorrect format. Expected: "{expected_key}" - received: "{key}"')


def extract_publication_reference(publication_references_origin_file, file_name: str, line_number: int) -> PublicationReference | None:
    ref_line = publication_references_origin_file.readline()

    if not ref_line:
        return None

    check_key(ref_line, "Ref", line_number)
    line_number += 1

    author_line = publication_references_origin_file.readline()
    check_key(author_line, "Author", line_number)
    line_number += 1

    journal_line = publication_references_origin_file.readline()
    check_key(journal_line, "Journal", line_number)
    line_number += 1

    title_line = publication_references_origin_file.readline()
    check_key(title_line, "Title", line_number)
    line_number += 1

    bibcode_line = publication_references_origin_file.readline()
    check_key(bibcode_line, "Bibcode", line_number)
    line_number += 1

    year_line = publication_references_origin_file.readline()
    check_key(year_line, "Year", line_number)
    line_number += 1

    data_line = publication_references_origin_file.readline()
    if parse_key(data_line) != "Keyword":
        check_key(data_line, "Data", line_number)
        line_number += 1

    ref_value = parse_value(ref_line)
    author_value = parse_value(author_line)
    journal_value = parse_value(journal_line)
    title_value = parse_value(title_line)
    bibcode_value = parse_value(bibcode_line)
    year_value = parse_value(year_line)

    if parse_key(data_line) != "Keyword":
        data_value = parse_value(data_line)
    else:
        data_value = None

    line = publication_references_origin_file.readline().strip()
    line_number += 1

    if line != "":
        raise ValueError(f'Line: {line_number} - Incorrect format. Expected: "" - received: "{line}"')

    return PublicationReference(file_name, ref_value, author_value, journal_value, title_value, bibcode_value, year_value, data_value)

def process_publication_references_file(
        publication_references_origin_file_path: str,
        publication_references_destination_sql_file,
        error_output_file_path: str,
        file_name: str,
        comma_shall_be_writen: [bool]):
    # return if file does not exist
    if not os.path.exists(publication_references_origin_file_path):
        print(f"FILE NOT FOUND: '{publication_references_origin_file_path}'.")
        return

    with open(publication_references_origin_file_path, 'rt') as publication_references_origin_file:
        print(f"Processing file content: {publication_references_origin_file_path}")

        line_number = move_to_first_record(publication_references_origin_file)

        while True:
            # check the format
            try:
                # extract each publication reference from file
                publication_reference = extract_publication_reference(publication_references_origin_file, file_name, line_number)
                line_number += 8
            except ValueError as e:
                # store info about not processed files due to format inconsistency
                with open(error_output_file_path, "at") as not_processed_files_file:
                    message = "\t".join(["File not processed", publication_references_origin_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)
                return  # TODO - maybe continue?

            if publication_reference is None:
                break

            if comma_shall_be_writen[0]:
                # write insert command to file
                publication_references_destination_sql_file.write(",\n")

            comma_shall_be_writen[0] = True

            # create and write VALUES part of SQL insert command
            write_sql_values_data_statement(publication_references_destination_sql_file, publication_reference)

# these are references to publications not publications itself CHECK !!!
def main():
    # check folder path existence and create folder on path if it does not exist yet
    publication_references_destination_folder_path: str = os.path.join(DESTINATION_FOLDER_PATH, DATA_DESTINATION_FOLDER_NAME, PUBLICATION_REFERENCES_DATA_FOLDER_NAME)
    if not os.path.exists(publication_references_destination_folder_path):
        os.makedirs(publication_references_destination_folder_path, exist_ok=True)

    # error destination file
    error_output_destination_file_path: str = os.path.join(publication_references_destination_folder_path, ERROR_OUTPUT_FILE_NAME)
    with open(error_output_destination_file_path, "wt") as _:
        print(f"Cleaning error file '{error_output_destination_file_path}'.")

    publication_references_origin_folder_path: str = os.path.join(ORIGIN_FOLDER_PATH, REFERENCES_ORIGIN_FOLDER_NAME)
    publication_references_origin_folder_content = os.listdir(publication_references_origin_folder_path)
    file_names = [f for f in publication_references_origin_folder_content
                  if os.path.isfile(os.path.join(publication_references_origin_folder_path, f)) and
                  f.endswith(PUBLICATION_FILE_SUFFIX) and not f.__eq__(NUMBERING_SYSTEM_FILE_NAME)]

    # destination sql file path
    publication_references_destination_sql_file_path: str = os.path.join(publication_references_destination_folder_path,
                                                                         SQL_PUBLICATION_REFERENCES_FILE_NAME)
    # CHECK
    if len(file_names) == 0:
        with open(publication_references_destination_sql_file_path, "wt") as publication_references_destination_sql_file:
            publication_references_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)
        return

    file_names.sort()

    comma_shall_be_writen: [bool] = [False]

    with open(publication_references_destination_sql_file_path, "wt", buffering=BUFFER_SIZE) as publication_references_destination_sql_file:
        # create and write INSERT part of SQL insert command
        table_parameters = PublicationReference.get_table_parameters()
        write_sql_insert_statement(publication_references_destination_sql_file, PUBLICATION_REFERENCES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(publication_references_destination_sql_file)

        for file_name in file_names:
            publication_references_origin_file_path = os.path.join(publication_references_origin_folder_path, file_name)

            process_publication_references_file(
                publication_references_origin_file_path,
                publication_references_destination_sql_file,
                error_output_destination_file_path,
                file_name,
                comma_shall_be_writen)

        # write end of insert command to file
        publication_references_destination_sql_file.write(";\n")

    if not comma_shall_be_writen[0]:
        # remove file contents as no data were found
        with open(publication_references_destination_sql_file_path, "wt") as publication_references_destination_sql_file:
            publication_references_destination_sql_file.write(NO_DATA_WERE_FOUND_SQL_COMMENT)


if __name__ == '__main__':
    main()