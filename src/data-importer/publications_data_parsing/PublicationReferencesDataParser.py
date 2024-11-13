import os

from common.create_sql_insert_methods import write_sql_insert_statement, write_sql_values_keyword_statement, write_sql_values_data_statement
from common.constants import PUBLICATION_REFERENCES_TABLE_NAME, PUBLICATION_FILE_SUFFIX, \
    PUBLICATION_REFERENCES_DATA_FOLDER_NAME, SQL_PUBLICATION_REFERENCES_FILE_NAME, ERROR_OUTPUT_FILE_NAME
from common.file_paths import DESTINATION_DATA_FOLDER_PATH, ORIGIN_PUBLICATION_REFERENCES_FOLDER_PATH
from PublicationReference import PublicationReference

def move_to_first_record(open_file) -> int:
    line_number = 1

    data_line = open_file.readline().strip()
    line_number += 1

    # without header
    if data_line == "":
        return line_number

    # with header
    if data_line.startswith("Dummy"):
        _ = open_file.readline()
        line_number += 1
        return line_number

    # unknown format -> raise error
    raise ValueError("Unknown file format.")

def process_publication_record(file_name, lines: [str]) -> PublicationReference:
    ref = None
    author = None
    journal = None
    title = None
    bibcode = None
    year = None
    data = None

    for line in lines:
        pair = line.split("\t", 1)

        if len(pair) == 2:
            key, value = pair
        else:
            continue

        stripped_value = value.strip()

        match key.lower():
            case "ref":
                ref = stripped_value
            case "author":
                author = stripped_value
            case "journal":
                journal = stripped_value
            case "title":
                title = stripped_value
            case "bibcode":
                bibcode = stripped_value
            case "year":
                year = stripped_value
            case "data":
                data = stripped_value

    return PublicationReference(file_name, ref, author, journal, title, bibcode, year, data)

def parse_key(line: str) -> str:
    return line.split("\t", 1)[0].strip(" \",\n")

def parse_value(line: str) -> str:
    return line.split("\t", 1)[1].strip(" \",\n")

def check_key(line: str, expected_key: str, line_number: int):
    key = parse_key(line)
    if key != expected_key:
        raise ValueError(f'Line: {line_number} - Incorrect format. Expected: "{expected_key}" - received: "{key}"')

def extract_publication_reference(source_file, file_name: str, line_number: int) -> PublicationReference | None:
    ref_line = source_file.readline()

    if not ref_line:
        return None

    check_key(ref_line, "Ref", line_number)
    line_number += 1

    author_line = source_file.readline()
    check_key(author_line, "Author", line_number)
    line_number += 1

    journal_line = source_file.readline()
    check_key(journal_line, "Journal", line_number)
    line_number += 1

    title_line = source_file.readline()
    check_key(title_line, "Title", line_number)
    line_number += 1

    bibcode_line = source_file.readline()
    check_key(bibcode_line, "Bibcode", line_number)
    line_number += 1

    year_line = source_file.readline()
    check_key(year_line, "Year", line_number)
    line_number += 1

    data_line = source_file.readline()
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

    line = source_file.readline().strip()
    line_number += 1

    if line != "":
        raise ValueError(f'Line: {line_number} - Incorrect format. Expected: "" - received: "{line}"')

    return PublicationReference(file_name, ref_value, author_value, journal_value, title_value, bibcode_value, year_value, data_value)

def process_single_file(
        original_publication_references_file_path: str,
        sql_destination_publication_references_file,
        error_output_file_path: str,
        file_name: str,
        first_line_was_written: [bool]):
    with open(original_publication_references_file_path, 'rt') as original_publication_references_file:
        print(f"Processing file content: {original_publication_references_file_path}")

        line_number = move_to_first_record(original_publication_references_file)

        while True:
            # check the format
            try:
                # extract each publication reference from file
                publication_reference = extract_publication_reference(original_publication_references_file, file_name, line_number)
                line_number += 8
            except ValueError as e:
                # store info about not processed files due to format inconsistency
                with open(error_output_file_path, "at") as not_processed_files_file:
                    message = "\t".join(["File not processed", original_publication_references_file_path, e.__str__(), "\n"])
                    not_processed_files_file.write(message)
                return  # TODO - maybe continue?

            if publication_reference is None:
                break

            if first_line_was_written[0]:
                # write insert command to file
                sql_destination_publication_references_file.write(",\n")

            first_line_was_written[0] = True

            # create and write VALUES part of SQL insert command
            write_sql_values_data_statement(sql_destination_publication_references_file, publication_reference)

def main():
    destination_publication_references_folder_path = os.path.join(DESTINATION_DATA_FOLDER_PATH, PUBLICATION_REFERENCES_DATA_FOLDER_NAME)
    original_publication_references_folder_content = os.listdir(ORIGIN_PUBLICATION_REFERENCES_FOLDER_PATH)

    # create folder on path
    if not os.path.exists(destination_publication_references_folder_path):
        os.mkdir(destination_publication_references_folder_path)

    file_names = [f for f in original_publication_references_folder_content
                  if os.path.isfile(os.path.join(ORIGIN_PUBLICATION_REFERENCES_FOLDER_PATH, f)) and
                  f.endswith(PUBLICATION_FILE_SUFFIX) and not f.__eq__("sysno.ref")]

    if len(file_names) == 0:
        return

    file_names.sort()

    # TODO - these are references to publications not publications itself CHECK !!!

    sql_destination_publication_references_file_path = os.path.join(destination_publication_references_folder_path, SQL_PUBLICATION_REFERENCES_FILE_NAME)
    error_output_file_path = os.path.join(destination_publication_references_folder_path, ERROR_OUTPUT_FILE_NAME)

    with open(sql_destination_publication_references_file_path, "wt") as sql_destination_publications_file:
        # create and write INSERT part of SQL insert command
        table_parameters = PublicationReference.get_table_parameters()
        write_sql_insert_statement(sql_destination_publications_file, PUBLICATION_REFERENCES_TABLE_NAME, table_parameters)

        # create and write VALUES keyword of SQL insert command
        write_sql_values_keyword_statement(sql_destination_publications_file)

        first_line_was_written: [bool] = [False]

        for file_name in file_names:
            publication_references_file_path = os.path.join(ORIGIN_PUBLICATION_REFERENCES_FOLDER_PATH, file_name)
            process_single_file(
                publication_references_file_path,
                sql_destination_publications_file,
                error_output_file_path,
                file_name,
                first_line_was_written)

        # write end of insert command to file
        sql_destination_publications_file.write(";\n")

if __name__ == '__main__':
    main()